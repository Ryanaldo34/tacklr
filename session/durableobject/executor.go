package durableobject

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/loop"
)

// executor is the durable-object loop.Executor for one run of a session.
//
// Every call that loop.Run makes is one journal entry. A run first replays:
// each call returns its entry instead of acting, until the journal ends;
// from then on each call acts and journals its result. Each call writes
// once: its entry, the mailbox wakes it took, the durable events it made,
// what it stored and the letters it sends to other objects go in one
// write, so they land together or not at all. The journal holds one turn:
// when the session would wait with a journal, it suspends instead, and
// goes on from its carry with an empty one.
type executor struct {
	o    *Object
	init Init
	turn *loop.Turn
	ctx  context.Context

	journal []entry
	pos     int
	// seq is the number the next journal entry gets, and entries how many
	// the journal holds.
	seq     uint64
	entries int
	// pending are the mailbox wakes taken since the last entry.
	pending []uint64
	// events are the durable events of the step that runs.
	events buffer
	// armed is when the step that runs set the watchdog.
	armed time.Time
	// newTurn says the next Commit is a new turn's first.
	newTurn bool
}

// entry is one journaled executor call: its result value, its error, and
// the mailbox wakes it took.
type entry struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value,omitempty"`
	Err   *stepError      `json:"err,omitempty"`
	Taken []uint64        `json:"taken,omitempty"`
}

// stepError is a step's error as the journal keeps it: its text, and the
// kind of failure a client acts on.
type stepError struct {
	Text string           `json:"text"`
	Kind session.FailKind `json:"kind,omitempty"`
}

func encodeErr(err error) *stepError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return &stepError{Text: context.Canceled.Error(), Kind: session.FailCancelled}
	case errors.Is(err, session.ErrModelRefused):
		return &stepError{Text: err.Error(), Kind: session.FailRefused}
	case errors.Is(err, session.ErrMaxTokens):
		return &stepError{Text: err.Error(), Kind: session.FailMaxTokens}
	}
	return &stepError{Text: err.Error()}
}

// err is the error the turn sees: the sentinel of its kind, with its text.
func (e *stepError) err() error {
	switch {
	case e == nil:
		return nil
	case e.Kind == session.FailCancelled:
		return context.Canceled
	}
	return e.Kind.WithText(e.Text)
}

// toolResult is one loop.ToolResult as the journal keeps it.
type toolResult struct {
	Outcome tacklr.CallOutcome `json:"outcome"`
	Err     *stepError         `json:"err,omitempty"`
}

// buffer collects the durable events of a step, for its write.
type buffer struct {
	mu  sync.Mutex
	evs []storedEvent
}

func (b *buffer) add(ev session.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evs = append(b.evs, storedEvent{Topic: session.TopicEvents, Event: ev})
}

// take is the events collected, which it empties.
func (b *buffer) take() []storedEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	evs := b.evs
	b.evs = nil
	return evs
}

// newExecutor reads the session's journal; the mailbox wakes its entries
// took are not the session's to take again. A storage failure stops the
// run.
func newExecutor(o *Object, init Init, turn *loop.Turn) *executor {
	e := &executor{o: o, init: init, turn: turn, ctx: o.ctx}
	taken := map[uint64]bool{}
	err := scanAll(o.host, prefix(kindJournal, init.SessionID), func(raw Entry) error {
		var en entry
		if err := json.Unmarshal(raw.Value, &en); err != nil {
			return fmt.Errorf("journal entry %s: %w", raw.Key, err)
		}
		e.journal = append(e.journal, en)
		e.seq, e.entries = seqOf(raw.Key)+1, e.entries+1
		for _, n := range en.Taken {
			taken[n] = true
		}
		return nil
	})
	if err != nil {
		fail(err)
	}
	o.mu.Lock()
	o.mail = slices.DeleteFunc(o.mail, func(q queued) bool { return taken[q.seq] })
	o.mu.Unlock()
	return e
}

// replaying reports whether the journal has entries the run has not
// returned yet.
func (e *executor) replaying() bool { return e.pos < len(e.journal) }

// replay returns the next journal entry for a call of kind, decoded into
// out. ok is false once the journal ended: the call acts. An entry of
// another kind means the session did not run as it did before, and stops
// the run.
func (e *executor) replay(kind string, out any) (ok bool, err error) {
	if !e.replaying() {
		return false, nil
	}
	en := e.journal[e.pos]
	e.pos++
	if en.Kind != kind {
		fail(fmt.Errorf("journal entry %d is %s, the session asked for %s", e.pos-1, en.Kind, kind))
	}
	if out != nil && len(en.Value) > 0 {
		if err := json.Unmarshal(en.Value, out); err != nil {
			fail(fmt.Errorf("journal entry %d: %w", e.pos-1, err))
		}
	}
	return true, en.Err.err()
}

// write journals a call's result in the call's one write: with ops, the
// step's durable events, the wakes it took, the session's status, and
// letters for other objects. It returns the error as the journal keeps
// it, so a live call and its replay see the same error. A failed write
// stops the run.
func (e *executor) write(kind string, value any, err error, ops []Op, letters ...letter) error {
	if cerr := e.o.ctx.Err(); cerr != nil {
		fail(cerr)
	}
	en := entry{Kind: kind, Err: encodeErr(err), Taken: e.pending}
	if value != nil {
		raw, merr := json.Marshal(value)
		if merr != nil {
			fail(fmt.Errorf("journal %s: %w", kind, merr))
		}
		en.Value = raw
	}
	raw, _ := json.Marshal(en) // a kind, JSON and numbers always encode
	id := e.init.SessionID
	st := e.status()
	ops = append(ops, put(seqKey(kindJournal, id, e.seq), raw), writeStatus(st))
	e.o.mu.Lock()
	sent := e.o.out.post(letters...)
	e.o.mu.Unlock()
	if werr := e.o.events.commit(id, e.events.take(), append(ops, sent...)...); werr != nil {
		fail(werr)
	}
	e.o.mu.Lock()
	e.o.mail = slices.DeleteFunc(e.o.mail, func(q queued) bool { return slices.Contains(e.pending, q.seq) })
	e.o.st = st
	e.o.out.sent(sent)
	e.o.sendLocked()
	e.o.mu.Unlock()
	e.seq++
	e.entries++
	e.pending = nil
	return en.Err.err()
}

// status is what Status and Jobs answer now.
func (e *executor) status() statusRecord {
	ids := make([]session.ID, len(e.turn.Jobs))
	for i, j := range e.turn.Jobs {
		ids[i] = j.ID
	}
	return statusRecord{Status: e.turn.Status(), Jobs: ids}
}

// Begin implements loop.Executor. The object holds no turn-scoped resources
// besides its harness, which it keeps across turns. A new turn drops the
// events of the turns before it, and its first Commit their outcome
// records, once it committed what they left open. A replay already did,
// and a run again after a stop only deletes again.
func (e *executor) Begin(kind string, _, _ int) {
	if kind != session.WakePrompt || e.replaying() {
		return
	}
	e.newTurn = true
	id := e.init.SessionID
	head, err := e.o.events.Head(e.ctx, id)
	if err == nil {
		err = e.o.events.Trim(e.ctx, id, head)
	}
	if err != nil {
		fail(err)
	}
}

// End implements loop.Executor.
func (e *executor) End() {}

// take is the oldest mailbox wake the run has not taken.
func (e *executor) take() (session.Wake, bool) {
	e.o.mu.Lock()
	defer e.o.mu.Unlock()
	for _, q := range e.o.mail {
		if !slices.Contains(e.pending, q.seq) {
			e.pending = append(e.pending, q.seq)
			return q.wake, true
		}
	}
	return session.Wake{}, false
}

// takeCancel reports whether a cancel is queued and takes the first one,
// with the prompts queued before it (loop.TakeCancel).
func (e *executor) takeCancel() bool {
	e.o.mu.Lock()
	defer e.o.mu.Unlock()
	var open []queued
	for _, q := range e.o.mail {
		if !slices.Contains(e.pending, q.seq) {
			open = append(open, q)
		}
	}
	wakes := make([]session.Wake, len(open))
	for i, q := range open {
		wakes[i] = q.wake
	}
	_, taken, ok := loop.TakeCancel(wakes)
	for _, i := range taken {
		e.pending = append(e.pending, open[i].seq)
	}
	return ok
}

// Next implements loop.Executor: the oldest wake in the mailbox. With block
// and none queued, the session suspends instead of waiting: its run ends,
// its state is its carry, and the next wake starts a run from there with an
// empty journal. So no goroutine waits, and the journal holds one turn.
// Taking a job's end closes the job's object, which the session no longer
// needs.
func (e *executor) Next(block bool) session.Wake {
	var w session.Wake
	if ok, _ := e.replay("next", &w); ok {
		return w
	}
	w, ok := e.take()
	if !ok && block {
		e.idle()
		w = session.Wake{Kind: session.WakeSuspend}
	}
	var ops []Op
	var letters []letter
	if w.Kind == session.WakeJob {
		child := w.Job.ID
		ops = []Op{del(childKey(e.init.SessionID, child))}
		letters = []letter{{To: child, Wake: &session.Wake{Kind: session.WakeClose}}}
	}
	_ = e.write("next", w, nil, ops, letters...)
	return w
}

// busy sets the watchdog alarm while a step runs, so a platform that stops
// the object mid-step restarts it. It sets it again once half of it passed.
func (e *executor) busy() {
	now := time.Now()
	if !e.armed.IsZero() && now.Sub(e.armed) < e.o.cfg.Watchdog/2 {
		return
	}
	e.o.mu.Lock()
	e.o.watchdog = now.Add(e.o.cfg.Watchdog)
	e.o.armLocked()
	e.o.mu.Unlock()
	e.armed = now
}

// idle clears the watchdog: the session waits for a wake, which restarts
// it.
func (e *executor) idle() {
	if e.armed.IsZero() {
		return
	}
	e.o.mu.Lock()
	e.o.watchdog = time.Time{}
	e.o.armLocked()
	e.o.mu.Unlock()
	e.armed = time.Time{}
}

// wait runs step and waits for it, or for a cancel in the mailbox, which
// stops it. cancelled says a cancel stopped it.
func (e *executor) wait(step func(ctx context.Context) error) (cancelled bool, err error) {
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- step(ctx) }()
	ticker := time.NewTicker(e.o.cfg.Watchdog / 2)
	defer ticker.Stop()
	for {
		if e.takeCancel() {
			cancel()
			<-done
			return true, context.Canceled
		}
		select {
		case err := <-done:
			return false, err
		case <-e.o.notify:
		case <-ticker.C:
			e.busy()
		}
	}
}

// attempt runs step until it succeeds, fails for good, or ran
// StepAttempts times. A network failure, a stale snapshot and a refusal
// (a content filter often clears) run again after a backoff.
func (e *executor) attempt(ctx context.Context, step func(ctx context.Context) error) error {
	delay := e.o.cfg.RetryBackoff
	for n := 1; ; n++ {
		err := step(ctx)
		retry := errors.Is(err, tacklr.ErrNetwork) || errors.Is(err, session.ErrStaleCheckpoint) || errors.Is(err, session.ErrModelRefused)
		if !retry || n >= e.o.cfg.StepAttempts || ctx.Err() != nil {
			return err
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(delay*2, retryBackoffMax)
	}
}

// emitter sends the harness's events: a live one to the subscribers that
// listen now, a durable one to b, for its step's write. The turn's end
// events are Emit's.
func (e *executor) emitter(b *buffer) func(session.Event) {
	return func(ev session.Event) {
		switch {
		case ev.Type == session.EventComplete, ev.Type == session.EventInterrupt, ev.Type == session.EventError:
		case ev.Live():
			e.o.deltas.Publish(e.init.SessionID, ev)
		default:
			b.add(ev)
		}
	}
}

// Infer implements loop.Executor.
func (e *executor) Infer() (tacklr.Reply, error) {
	var reply tacklr.Reply
	if ok, err := e.replay("infer", &reply); ok {
		return reply, err
	}
	e.busy()
	cancelled, err := e.wait(func(ctx context.Context) error {
		return e.attempt(ctx, func(ctx context.Context) error {
			e.events.take() // an attempt before this one made them
			h, err := e.o.harness.read(ctx, e)
			if err != nil {
				return err
			}
			out, stop := tacklr.PipeEvents(e.emitter(&e.events))
			defer stop()
			reply, err = h.Infer(ctx, out)
			return err
		})
	})
	if cancelled {
		reply = tacklr.Reply{}
		e.events.take()
	}
	return reply, e.write("infer", reply, err, nil)
}

// Tools implements loop.Executor: one goroutine per call, at most
// MaxToolCalls at once. A call writes its outcome, its result event and its
// result in one write when it ends, so a run again after a stop runs only
// the calls that did not end. A cancel stops the calls still running, and
// each of them reports the cancel.
func (e *executor) Tools(runs []tacklr.ToolRun) []loop.ToolResult {
	var stored []toolResult
	if ok, _ := e.replay("tools", &stored); ok {
		return toolResults(stored)
	}
	e.busy()
	stored = make([]toolResult, len(runs))
	finished := make([]bool, len(runs))
	var mu sync.Mutex
	var stop error
	cancelled, _ := e.wait(func(ctx context.Context) error {
		h, err := e.o.harness.read(ctx, e)
		sem := make(chan struct{}, e.o.cfg.MaxToolCalls)
		var wg sync.WaitGroup
		for i := range runs {
			wg.Go(func() {
				if err != nil {
					stored[i] = toolResult{Outcome: tacklr.CallOutcome{Call: runs[i].Call}, Err: encodeErr(err)}
					return
				}
				r, ended, rerr := e.ended(runs[i])
				if !ended && rerr == nil {
					sem <- struct{}{}
					r, ended, rerr = e.runCall(ctx, h, runs[i])
					<-sem
				}
				mu.Lock()
				defer mu.Unlock()
				stored[i], finished[i] = r, ended
				stop = cmp.Or(stop, rerr)
			})
		}
		wg.Wait()
		return nil
	})
	if stop != nil {
		fail(stop)
	}
	if cancelled {
		for i := range runs {
			if !finished[i] {
				stored[i] = toolResult{Outcome: tacklr.CallOutcome{Call: runs[i].Call}, Err: encodeErr(context.Canceled)}
			}
		}
	}
	// The entry holds the results now; the calls' own records go.
	ops := make([]Op, len(runs))
	for i, run := range runs {
		ops[i] = del(e.callKey(run))
	}
	_ = e.write("tools", stored, nil, ops)
	return toolResults(stored)
}

// callKey is where a call of this turn keeps its result once it ended.
func (e *executor) callKey(run tacklr.ToolRun) string {
	return prefix(kindCall, e.init.SessionID) + url.PathEscape(run.Call.Key())
}

// ended is the result of run's call when a run of this step before a stop
// ended it.
func (e *executor) ended(run tacklr.ToolRun) (toolResult, bool, error) {
	raw, ok, err := e.o.host.Get(e.callKey(run))
	if err != nil || !ok {
		return toolResult{}, false, err
	}
	var r toolResult
	err = json.Unmarshal(raw, &r)
	return r, true, err
}

// runCall is one call's step: it runs the tool and, unless the turn
// cancelled it meanwhile, writes its outcome by call key, its result event
// and its result in one write. ended is false for a call the turn
// cancelled, which reports the cancel, not what its tool returned.
func (e *executor) runCall(ctx context.Context, h *tacklr.Harness, run tacklr.ToolRun) (r toolResult, ended bool, err error) {
	turnCtx := ctx
	run.MaxChildren = e.o.cfg.MaxChildren
	if run.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, run.Timeout+loop.ToolTimeoutMargin)
		defer cancel()
	}
	st := &staged{snapshots: e.o.snapshots}
	var b buffer
	var o tacklr.CallOutcome
	callErr := e.attempt(ctx, func(ctx context.Context) error {
		st.ops = nil
		b.take()
		out, stop := tacklr.PipeEvents(e.emitter(&b))
		defer stop()
		var err error
		o, err = adapter.RunTool(ctx, st, e.init.SessionID, h, run, out)
		return err
	})
	r = toolResult{Outcome: o, Err: encodeErr(cmp.Or(callErr, ctx.Err()))}
	if turnCtx.Err() != nil {
		return r, false, nil
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return r, false, err
	}
	return r, true, e.o.events.commit(e.init.SessionID, b.take(), append(st.ops, put(e.callKey(run), raw))...)
}

func toolResults(stored []toolResult) []loop.ToolResult {
	out := make([]loop.ToolResult, len(stored))
	for i, r := range stored {
		out[i] = loop.ToolResult{Outcome: r.Outcome, Err: r.Err.err()}
	}
	return out
}

// Commit implements loop.Executor. It runs to completion: a cancel queued
// meanwhile stays queued for the turn. The checkpoint's changed rows, its
// events and the entry are one write; a new turn's first Commit also drops
// the outcome records of the turns before it, which it committed.
func (e *executor) Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error) {
	var rev session.Revision
	if ok, err := e.replay("commit", &rev); ok {
		return rev, err
	}
	e.busy()
	st := &staged{snapshots: e.o.snapshots}
	err := e.attempt(e.ctx, func(ctx context.Context) error {
		st.ops = nil
		e.events.take()
		var err error
		rev, err = e.o.harness.commit(ctx, e, st, base, in)
		return err
	})
	var ops []Op
	if err == nil {
		ops = st.ops
		if e.newTurn {
			outcomes, oerr := deleteAll(e.o.host, prefix(kindOutcome, e.init.SessionID))
			if oerr != nil {
				fail(oerr)
			}
			ops = append(ops, outcomes...)
		}
	}
	werr := e.write("commit", rev, err, ops)
	st.applied()
	if err == nil {
		e.newTurn = false
	}
	return rev, werr
}

// Emit implements loop.Executor: the turn's end event and the entry, in one
// write.
func (e *executor) Emit(ev session.Event) {
	if ok, _ := e.replay("emit", nil); ok {
		return
	}
	e.events.add(ev)
	_ = e.write("emit", nil, nil, nil)
}

// Job implements loop.Executor: the named job's handler, in this object.
func (e *executor) Job(name, task string) (string, error) {
	var out string
	if ok, err := e.replay("job", &out); ok {
		return out, err
	}
	fn, found := e.o.cfg.Jobs[name]
	if !found {
		return "", e.write("job", "", fmt.Errorf("%w: job %s", tacklr.ErrNotFound, name), nil)
	}
	e.busy()
	cancelled, err := e.wait(func(ctx context.Context) error {
		return e.attempt(ctx, func(ctx context.Context) error {
			var err error
			out, err = fn(ctx, task)
			return err
		})
	})
	if cancelled {
		out = ""
	}
	return out, e.write("job", out, err, nil)
}

// Start implements loop.Executor: a letter that starts the job's own
// object with this session's credentials, sealed for the job, and a record
// of the job, so the session's close reaches it.
func (e *executor) Start(s tacklr.JobStart) error {
	if ok, err := e.replay("start", nil); ok {
		return err
	}
	id, child := e.init.SessionID, session.ID(s.ID)
	init := Init{SessionID: child, Parent: id, Task: s.Task, Mounts: e.turn.Mounts}
	if s.Specialist {
		init.Specialist = s.Name
	} else {
		init.Worker = s.Name
	}
	sec, err := e.o.secrets.Get(e.ctx, id)
	var sealed []byte
	if err == nil && len(sec.Auth.Bindings) > 0 {
		plain, _ := json.Marshal(sec) // strings, times and maps of strings only
		sealed, err = e.o.cfg.Sealer.Seal(e.ctx, plain, []byte(child))
	}
	if err != nil {
		return e.write("start", nil, err, nil)
	}
	return e.write("start", nil, nil, []Op{put(childKey(id, child), nil)}, letter{To: child, Create: &init, Auth: sealed})
}

// Cancel implements loop.Executor: a letter that asks the job's object to
// stop.
func (e *executor) Cancel(id session.ID) {
	if ok, _ := e.replay("cancel", nil); ok {
		return
	}
	_ = e.write("cancel", nil, nil, nil, letter{To: id, Wake: &session.Wake{Kind: session.WakeCancel}})
}

// suspend keeps the session's state as its carry and drops the journal and
// the wakes it took, in one write: the next run starts from the carry. A
// run that journaled nothing changed nothing: its carry is current.
func (e *executor) suspend() {
	if e.entries == 0 {
		return
	}
	id := e.init.SessionID
	var ops []Op
	err := scanAll(e.o.host, prefix(kindJournal, id), func(raw Entry) error {
		ops = append(ops, del(raw.Key))
		var en entry
		_ = json.Unmarshal(raw.Value, &en) // newExecutor or write wrote it
		for _, n := range en.Taken {
			ops = append(ops, del(seqKey(kindMailbox, id, n)))
		}
		return nil
	})
	if err != nil {
		fail(err)
	}
	raw, err := json.Marshal(e.turn.Carry())
	if err != nil {
		fail(err)
	}
	st := e.status()
	if err := e.o.events.commit(id, nil, append(ops, put(key(kindCarry, id), raw), writeStatus(st))...); err != nil {
		fail(err)
	}
	e.o.mu.Lock()
	e.o.st = st
	e.o.mu.Unlock()
}

// finish ends the session after loop.Run returned (which stopped its
// running jobs): it tells its parent how it ended, and keeps only its final
// status. A closed session also closes its jobs, forgets its messages in
// the brain and deletes what it saved. It is one write, and its letters go
// out after.
func (e *executor) finish(result string, runErr error) {
	id, t := e.init.SessionID, e.turn
	e.o.harness.close()
	var letters []letter
	if t.Closed {
		children := prefix(kindChild, id)
		err := scanAll(e.o.host, children, func(c Entry) error {
			child, _ := url.PathUnescape(c.Key[len(children):]) // childKey escaped it
			letters = append(letters, letter{To: session.ID(child), Wake: &session.Wake{Kind: session.WakeClose}})
			return nil
		})
		if err != nil {
			fail(err)
		}
	}
	if parent := e.init.Parent; parent != "" {
		end := loop.JobEnd(id, result, runErr)
		letters = append(letters, letter{To: parent, Wake: &session.Wake{Kind: session.WakeJob, Job: end}})
	}
	o := e.o
	o.mu.Lock()
	defer o.mu.Unlock()
	// A close may have been queued after the session's last wake: it closes
	// the session too.
	m := o.m
	closed := t.Closed || m.Closing
	if closed && o.cfg.Agent.Brain != nil {
		letters = append(letters, letter{Forget: id})
	}
	m.Ended, m.Closed, m.Closing, m.Final = true, closed, m.Closing || closed, t.Status()
	// An ended session keeps its snapshot and events for a late reader,
	// until its close; a closed one deletes everything.
	ops, err := o.leftovers(id, closed)
	if err != nil {
		fail(err)
	}
	st := statusRecord{Status: m.Final}
	sent := o.out.post(letters...)
	if err := o.events.commit(id, nil, append(ops, append(sent, writeMeta(m), writeStatus(st))...)...); err != nil {
		fail(err)
	}
	o.m, o.st, o.mail = m, st, nil
	o.out.sent(sent)
	if closed {
		o.closed(e.ctx, id)
	}
	o.sendLocked()
}

var _ loop.Executor = (*executor)(nil)
