package durableobject

import (
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

// retryBackoff is the first wait before a step runs again; it doubles up to
// retryBackoffMax.
var (
	retryBackoff    = time.Second
	retryBackoffMax = 30 * time.Second
)

// toolTimeoutMargin is what a tool call's step adds to the tool's own
// timeout.
const toolTimeoutMargin = time.Minute

// executor is the durable-object loop.Executor for one run of a session.
//
// Every call that loop.Run makes is one journal entry. A run first replays:
// each call returns its entry instead of acting, until the journal ends;
// from then on each call acts and journals its result. Wakes come from the
// mailbox in storage. An entry records the mailbox wakes its call took, in
// the same write, so a wake is taken exactly once across restarts.
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
	// taken are the mailbox wakes taken since the last entry; queue is the
	// mailbox wakes not taken, in arrival order, read up to seen.
	taken []uint64
	queue []queued
	seen  uint64
	// armed says the watchdog alarm is set, and when.
	armed time.Time
	// newTurn says the next Commit is a new turn's first.
	newTurn bool
}

type queued struct {
	seq  uint64
	wake session.Wake
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
	case e.Kind != "":
		return kindError{text: e.Text, kind: e.Kind.Err(e.Text)}
	}
	return errors.New(e.Text)
}

// kindError is a failure's text with the sentinel its kind names, so
// errors.Is finds it.
type kindError struct {
	text string
	kind error
}

func (e kindError) Error() string { return e.text }

func (e kindError) Unwrap() error { return e.kind }

// toolResult is one loop.ToolResult as the journal keeps it.
type toolResult struct {
	Outcome tacklr.CallOutcome `json:"outcome"`
	Err     *stepError         `json:"err,omitempty"`
}

// newExecutor reads the session's journal and the mailbox wakes it has not
// taken. A storage failure stops the run.
func newExecutor(o *Object, init Init, turn *loop.Turn) *executor {
	e := &executor{o: o, init: init, turn: turn, ctx: context.Background()}
	id := init.SessionID
	entries, err := o.host.Scan(prefix(kindJournal, id), "", 0)
	if err != nil {
		panic(stopped{err})
	}
	taken := map[uint64]bool{}
	for _, raw := range entries {
		var en entry
		if err := json.Unmarshal(raw.Value, &en); err != nil {
			panic(stopped{fmt.Errorf("journal entry %s: %w", raw.Key, err)})
		}
		e.journal = append(e.journal, en)
		e.seq, e.entries = seqOf(raw.Key)+1, e.entries+1
		for _, n := range en.Taken {
			taken[n] = true
		}
	}
	e.fill()
	e.queue = slices.DeleteFunc(e.queue, func(q queued) bool { return taken[q.seq] })
	if len(e.journal) == 0 {
		e.goLive()
	}
	return e
}

// replay returns the next journal entry for a call of kind, decoded into
// out. ok is false once the journal ended: the call acts. An entry of
// another kind means the session did not run as it did before, and stops
// the run.
func (e *executor) replay(kind string, out any) (ok bool, err error) {
	e.o.publish(e.turn)
	if e.pos >= len(e.journal) {
		return false, nil
	}
	en := e.journal[e.pos]
	e.pos++
	if en.Kind != kind {
		panic(stopped{fmt.Errorf("journal entry %d is %s, the session asked for %s", e.pos-1, en.Kind, kind)})
	}
	if out != nil && len(en.Value) > 0 {
		if err := json.Unmarshal(en.Value, out); err != nil {
			panic(stopped{fmt.Errorf("journal entry %d: %w", e.pos-1, err)})
		}
	}
	if e.pos == len(e.journal) {
		e.goLive()
	}
	return true, en.Err.err()
}

// goLive marks the end of replay: what Status reports is current.
func (e *executor) goLive() {
	e.o.publish(e.turn)
	e.o.mu.Lock()
	e.o.closeReady()
	e.o.mu.Unlock()
}

// record journals a call's result and the wakes it took, in one write. It
// returns the error as the journal keeps it, so a live call and its replay
// see the same error.
func (e *executor) record(kind string, value any, err error) error {
	en := entry{Kind: kind, Err: encodeErr(err), Taken: e.taken}
	if value != nil {
		raw, merr := json.Marshal(value)
		if merr != nil {
			panic(stopped{fmt.Errorf("journal %s: %w", kind, merr)})
		}
		en.Value = raw
	}
	raw, _ := json.Marshal(en) // a kind, JSON and numbers always encode
	if werr := e.o.host.Write(put(seqKey(kindJournal, e.init.SessionID, e.seq), raw)); werr != nil {
		panic(stopped{werr})
	}
	e.seq++
	e.entries++
	e.taken = nil
	return en.Err.err()
}

// Begin implements loop.Executor. The object holds no turn-scoped resources
// besides its harness, which it keeps across turns. A new turn drops the
// events of the turns before it, and its first Commit their outcome
// records, once it committed what they left open. A replay already did,
// and a run again after a stop only deletes again.
func (e *executor) Begin(kind string, _, _ int) {
	if kind != session.WakePrompt || e.pos < len(e.journal) {
		return
	}
	e.newTurn = true
	id := e.init.SessionID
	head, err := e.o.events.Head(e.ctx, id)
	if err != nil {
		panic(stopped{err})
	}
	ops, err := e.o.events.trimOps(id, head)
	if err != nil {
		panic(stopped{err})
	}
	if err := e.o.host.Write(ops...); err != nil {
		panic(stopped{err})
	}
}

// End implements loop.Executor.
func (e *executor) End() {}

// Next implements loop.Executor: the oldest wake in the mailbox. With block
// it waits for one, or suspends a parent session whose journal is long.
func (e *executor) Next(block bool) session.Wake {
	var w session.Wake
	if ok, _ := e.replay("next", &w); ok {
		return w
	}
	for {
		e.fill()
		if len(e.queue) > 0 {
			q := e.queue[0]
			e.queue = e.queue[1:]
			e.taken = append(e.taken, q.seq)
			w = q.wake
			break
		}
		if !block {
			break
		}
		if e.init.Task == "" && e.init.Worker == "" && e.entries >= e.o.cfg.JournalLimit {
			w = session.Wake{Kind: session.WakeSuspend}
			break
		}
		e.idle()
		<-e.o.notify
	}
	_ = e.record("next", w, nil)
	return w
}

// fill queues the mailbox wakes stored since the last read, in arrival
// order.
func (e *executor) fill() {
	id := e.init.SessionID
	after := ""
	if e.seen > 0 {
		after = seqKey(kindMailbox, id, e.seen)
	}
	entries, err := e.o.host.Scan(prefix(kindMailbox, id), after, 0)
	if err != nil {
		panic(stopped{err})
	}
	for _, raw := range entries {
		var w session.Wake
		if err := json.Unmarshal(raw.Value, &w); err != nil {
			panic(stopped{fmt.Errorf("mailbox entry %s: %w", raw.Key, err)})
		}
		e.seen = seqOf(raw.Key)
		e.queue = append(e.queue, queued{seq: e.seen, wake: w})
	}
}

// takeCancel reports whether a cancel is queued and takes the first one,
// with the prompts queued before it: they steered the turn the cancel
// ends. Wakes queued after the cancel stay, in order.
func (e *executor) takeCancel() bool {
	i := slices.IndexFunc(e.queue, func(q queued) bool { return q.wake.Kind == session.WakeCancel })
	if i < 0 {
		return false
	}
	kept := make([]queued, 0, len(e.queue)-1)
	for j, q := range e.queue {
		if j > i || (j < i && q.wake.Kind != session.WakePrompt) {
			kept = append(kept, q)
			continue
		}
		e.taken = append(e.taken, q.seq)
	}
	e.queue = kept
	return true
}

// busy sets the watchdog alarm while a step runs, so a platform that stops
// the object mid-step restarts it. It sets it again once half of it passed.
func (e *executor) busy() {
	now := time.Now()
	if !e.armed.IsZero() && now.Sub(e.armed) < e.o.cfg.Watchdog/2 {
		return
	}
	if err := e.o.host.SetAlarm(now.Add(e.o.cfg.Watchdog)); err == nil {
		e.armed = now
	}
}

// idle clears the watchdog alarm: the session waits for a wake, which
// restarts it.
func (e *executor) idle() {
	if e.armed.IsZero() {
		return
	}
	if err := e.o.host.SetAlarm(time.Time{}); err == nil {
		e.armed = time.Time{}
	}
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
		e.fill()
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
// (a content filter often clears) run again after a backoff, and each run
// again is a retry event.
func (e *executor) attempt(ctx context.Context, step func(ctx context.Context) error) error {
	delay := retryBackoff
	for n := 1; ; n++ {
		err := step(ctx)
		retry := errors.Is(err, tacklr.ErrNetwork) || errors.Is(err, session.ErrStaleCheckpoint) || errors.Is(err, session.ErrModelRefused)
		if !retry || n >= e.o.cfg.StepAttempts || ctx.Err() != nil {
			return err
		}
		_ = e.o.events.Append(context.WithoutCancel(ctx), e.init.SessionID, session.TopicRetry, session.Event{Type: session.EventError, Content: "retry"})
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(delay*2, retryBackoffMax)
	}
}

// emitter appends the harness's events to the event log. The turn's end
// events are Emit's.
func (e *executor) emitter(ctx context.Context) func(session.Event) {
	ctx = context.WithoutCancel(ctx)
	return func(ev session.Event) {
		switch ev.Type {
		case session.EventComplete, session.EventInterrupt, session.EventError:
			return
		}
		if ev.Delta {
			// Live only: the message's complete event carries the text.
			e.o.deltas.Publish(e.init.SessionID, ev)
			return
		}
		_ = e.o.events.Append(ctx, e.init.SessionID, session.TopicEvents, ev)
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
			h, err := e.o.harness.read(ctx, e)
			if err != nil {
				return err
			}
			out, stop := tacklr.PipeEvents(e.emitter(ctx))
			defer stop()
			reply, err = h.Infer(ctx, out)
			return err
		})
	})
	if cancelled {
		reply = tacklr.Reply{}
	}
	return reply, e.record("infer", reply, err)
}

// Tools implements loop.Executor: one goroutine per call, at most
// MaxToolCalls at once. A cancel stops the calls still running, and each of
// them reports the cancel.
func (e *executor) Tools(runs []tacklr.ToolRun) []loop.ToolResult {
	var stored []toolResult
	if ok, _ := e.replay("tools", &stored); ok {
		return toolResults(stored)
	}
	e.busy()
	stored = make([]toolResult, len(runs))
	finished := make([]bool, len(runs))
	var mu sync.Mutex
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
				sem <- struct{}{}
				defer func() { <-sem }()
				o, rerr := e.runTool(ctx, h, runs[i])
				mu.Lock()
				defer mu.Unlock()
				stored[i] = toolResult{Outcome: o, Err: encodeErr(rerr)}
				finished[i] = ctx.Err() == nil
			})
		}
		wg.Wait()
		return nil
	})
	if cancelled {
		for i := range runs {
			if !finished[i] {
				stored[i] = toolResult{Outcome: tacklr.CallOutcome{Call: runs[i].Call}, Err: encodeErr(context.Canceled)}
			}
		}
	}
	_ = e.record("tools", stored, nil)
	return toolResults(stored)
}

// runTool is one call's step: it runs the tool and keeps its outcome in the
// store by call key, so the journal holds only that it is stored. A call
// the turn cancelled reports the cancel, not what its tool returned.
func (e *executor) runTool(ctx context.Context, h *tacklr.Harness, run tacklr.ToolRun) (tacklr.CallOutcome, error) {
	run.MaxChildren = e.o.cfg.MaxChildren
	if run.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, run.Timeout+toolTimeoutMargin)
		defer cancel()
	}
	var o tacklr.CallOutcome
	err := e.attempt(ctx, func(ctx context.Context) error {
		out, stop := tacklr.PipeEvents(e.emitter(ctx))
		defer stop()
		var err error
		o, err = adapter.RunTool(ctx, e.o.snapshots, e.init.SessionID, h, run, out)
		return err
	})
	if err == nil {
		err = ctx.Err()
	}
	return o, err
}

func toolResults(stored []toolResult) []loop.ToolResult {
	out := make([]loop.ToolResult, len(stored))
	for i, r := range stored {
		out[i] = loop.ToolResult{Outcome: r.Outcome, Err: r.Err.err()}
	}
	return out
}

// Commit implements loop.Executor. It runs to completion: a cancel queued
// meanwhile stays queued for the turn.
func (e *executor) Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error) {
	var rev session.Revision
	if ok, err := e.replay("commit", &rev); ok {
		return rev, err
	}
	e.busy()
	err := e.attempt(e.ctx, func(ctx context.Context) error {
		var err error
		rev, err = e.o.harness.commit(ctx, e, base, in)
		return err
	})
	if err == nil && e.newTurn {
		e.newTurn = false
		if cerr := e.o.snapshots.ClearOutcomes(e.ctx, e.init.SessionID); cerr != nil {
			panic(stopped{cerr})
		}
	}
	return rev, e.record("commit", rev, err)
}

// Emit implements loop.Executor: the turn's end event, in the event log.
func (e *executor) Emit(ev session.Event) {
	if ok, _ := e.replay("emit", nil); ok {
		return
	}
	_ = e.o.events.Append(e.ctx, e.init.SessionID, session.TopicEvents, ev)
	_ = e.record("emit", nil, nil)
}

// Job implements loop.Executor: the named job's handler, in this object.
func (e *executor) Job(name, task string) (string, error) {
	var out string
	if ok, err := e.replay("job", &out); ok {
		return out, err
	}
	fn, found := e.o.cfg.Jobs[name]
	if !found {
		return "", e.record("job", "", fmt.Errorf("%w: job %s", tacklr.ErrNotFound, name))
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
	return out, e.record("job", out, err)
}

// Start implements loop.Executor: the job's own object, created with this
// session's credentials. A job already started under its id runs on.
func (e *executor) Start(s tacklr.JobStart) error {
	if ok, err := e.replay("start", nil); ok {
		return err
	}
	sec, err := e.o.secrets.Get(e.ctx, e.init.SessionID)
	if err == nil {
		init := Init{SessionID: session.ID(s.ID), Parent: e.init.SessionID, Task: s.Task, Mounts: e.turn.Mounts, Auth: sec.Auth}
		if s.Specialist {
			init.Specialist = s.Name
		} else {
			init.Worker = s.Name
		}
		// The session records each child first, so its close reaches the
		// ones that already ended too.
		err = e.o.host.Write(put(childKey(e.init.SessionID, init.SessionID), nil))
		if err == nil {
			err = e.o.host.Object(init.SessionID).Create(e.ctx, init)
		}
		if errors.Is(err, session.ErrExists) {
			err = nil
		}
	}
	return e.record("start", nil, err)
}

// Cancel implements loop.Executor: it asks the job's object to stop.
func (e *executor) Cancel(id session.ID) {
	if ok, _ := e.replay("cancel", nil); ok {
		return
	}
	_ = e.o.host.Object(id).Wake(e.ctx, session.Wake{Kind: session.WakeCancel})
	_ = e.record("cancel", nil, nil)
}

// suspend keeps the session's state as its carry and drops the journal and
// the wakes it took, in one write: the next run starts from the carry.
func (e *executor) suspend() {
	id := e.init.SessionID
	entries, err := e.o.host.Scan(prefix(kindJournal, id), "", 0)
	if err != nil {
		panic(stopped{err})
	}
	ops := make([]Op, 0, len(entries)+1)
	for _, raw := range entries {
		ops = append(ops, del(raw.Key))
		var en entry
		_ = json.Unmarshal(raw.Value, &en) // newExecutor or record wrote it
		for _, n := range en.Taken {
			ops = append(ops, del(seqKey(kindMailbox, id, n)))
		}
	}
	raw, err := json.Marshal(carryOf(e.turn))
	if err != nil {
		panic(stopped{err})
	}
	if err := e.o.host.Write(append(ops, put(key(kindCarry, id), raw))...); err != nil {
		panic(stopped{err})
	}
}

// finish ends the session after loop.Run returned: it stops its running
// jobs, tells its parent how it ended, and keeps only its final status. A
// closed session also closes its jobs and deletes what it saved. Each step
// is safe to repeat: a run that stopped part way does them again.
func (e *executor) finish(m meta, result string, runErr error) {
	ctx, id, t := e.ctx, e.init.SessionID, e.turn
	for _, j := range t.Jobs {
		if !j.Ended {
			_ = e.o.host.Object(j.ID).Wake(ctx, session.Wake{Kind: session.WakeCancel})
		}
	}
	if t.Closed {
		ids := []session.ID{id}
		children, err := e.o.host.Scan(prefix(kindChild, id), "", 0)
		if err != nil {
			panic(stopped{err})
		}
		for _, c := range children {
			child, _ := url.PathUnescape(c.Key[len(prefix(kindChild, id)):]) // childKey escaped it
			ids = append(ids, session.ID(child))
			_ = e.o.host.Object(session.ID(child)).Wake(ctx, session.Wake{Kind: session.WakeClose})
		}
		if brain := e.o.cfg.Agent.Brain; brain != nil {
			for _, sid := range ids {
				_ = brain.DeleteSessionMessages(ctx, e.o.cfg.Agent.SearchNamespace, string(sid))
			}
		}
	}
	if parent := e.init.Parent; parent != "" {
		res := session.JobResult{Output: result}
		if runErr != nil {
			res = session.JobResult{Output: runErr.Error(), Failed: true}
		}
		_ = e.o.host.Object(parent).Wake(ctx, session.Wake{Kind: session.WakeJob, Job: session.JobEnd{ID: id, Result: res}})
	}
	e.o.harness.close()
	e.o.mu.Lock()
	defer e.o.mu.Unlock()
	// A close may have been queued after the session's last wake: it closes
	// the session too.
	if cur, ok, err := e.o.meta(); err == nil && ok {
		m.Closing = cur.Closing
	}
	closed := t.Closed || m.Closing
	m.Ended, m.Closed, m.Final = true, closed, t.Status()
	e.o.status, e.o.jobs = m.Final, nil
	// A closed session deletes everything; an ended one keeps its snapshot
	// and events for a late reader, until its parent's close.
	if err := e.o.clear(ctx, m, closed); err != nil {
		panic(stopped{err})
	}
}

var _ loop.Executor = (*executor)(nil)
