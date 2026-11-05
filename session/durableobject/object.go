package durableobject

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/loop"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Config is what every session object of a deployment runs with.
type Config struct {
	// Agent is what every session runs; a specialist session runs its
	// specialist on it.
	Agent tacklr.AgentOptions
	// Sealer seals each session's credentials before they are stored.
	Sealer session.Sealer
	// Jobs are the named jobs a session can start.
	Jobs map[string]session.JobHandler
	// MaxToolCalls bounds how many tool calls of one reply run at once
	// (default 32).
	MaxToolCalls int
	// MaxChildren bounds a session's running children (default 1000).
	MaxChildren int
	// StepAttempts is how many times a step that failed on the network
	// runs (default 3).
	StepAttempts int
	// RetryBackoff is the first wait before a step, or the delivery of a
	// message to another object, runs again; it doubles up to 30 seconds
	// (default one second).
	RetryBackoff time.Duration
	// Watchdog is how long the alarm waits before it restarts a session
	// whose step stopped with its object (default one minute).
	Watchdog time.Duration
}

const (
	defaultMaxToolCalls = 32
	defaultMaxChildren  = 1000
	defaultStepAttempts = 3
	defaultRetryBackoff = time.Second
	retryBackoffMax     = 30 * time.Second
	defaultWatchdog     = time.Minute
)

// Object runs one session in one durable object. The platform makes one per
// object and calls it for every request and alarm. It implements Ref.
//
// A live object holds its state in memory: the session's meta, its mailbox,
// its status, its outbox and its stores' heads. It reads storage once, when
// it starts, and writes every change to storage before it changes memory,
// so memory never holds what storage does not.
type Object struct {
	host      Host
	cfg       Config
	snapshots *snapshots
	secrets   *secretStore
	events    *eventLog
	// deltas sends the session's deltas to the subscribers listening now.
	deltas *session.Live
	// ctx ends when the platform stops the object (Stop).
	ctx  context.Context
	stop context.CancelFunc

	mu sync.Mutex
	// loaded says memory holds what storage has.
	loaded bool
	// has says the object holds a session, m.
	has bool
	m   meta
	// head is the mailbox's last sequence number, and mail its wakes that
	// no journal entry took, in arrival order.
	head uint64
	mail []queued
	// st is what Status and Jobs answer.
	st statusRecord
	// running says a goroutine runs the session; kick says a request came
	// in while it ran, so a run that is ending looks again.
	running bool
	kick    bool
	// notify wakes the session when a wake reaches its mailbox.
	notify chan struct{}
	out    outbox
	// watchdog is when the alarm restarts a stopped step (zero: no step
	// runs), and armed the alarm the platform has.
	watchdog, armed time.Time
	// settled closes, and is replaced, whenever the object goes idle.
	settled chan struct{}

	harness harnessState
}

// statusRecord is what Status and Jobs answer: the session's status and
// its running jobs, as of its last write.
type statusRecord struct {
	Status session.Status `json:"status"`
	Jobs   []session.ID   `json:"jobs,omitempty"`
}

// queued is a mailbox wake and its sequence number.
type queued struct {
	seq  uint64
	wake session.Wake
}

// NewObject returns the object over host. It panics without cfg.Sealer.
func NewObject(host Host, cfg Config) *Object {
	if cfg.Sealer == nil {
		panic("durableobject: Config.Sealer is required")
	}
	cfg.MaxToolCalls = positive(cfg.MaxToolCalls, defaultMaxToolCalls)
	cfg.MaxChildren = positive(cfg.MaxChildren, defaultMaxChildren)
	cfg.StepAttempts = positive(cfg.StepAttempts, defaultStepAttempts)
	cfg.RetryBackoff = positive(cfg.RetryBackoff, defaultRetryBackoff)
	cfg.Watchdog = positive(cfg.Watchdog, defaultWatchdog)
	ctx, stop := context.WithCancel(context.Background())
	return &Object{
		host:      host,
		cfg:       cfg,
		snapshots: newSnapshots(host),
		secrets:   &secretStore{s: host, sealer: cfg.Sealer},
		events:    newEventLog(host),
		deltas:    session.NewLive(),
		ctx:       ctx,
		stop:      stop,
		notify:    make(chan struct{}, 1),
		settled:   make(chan struct{}),
	}
}

// positive is v, or def when v is zero or negative.
func positive[T int | time.Duration](v, def T) T {
	if v > 0 {
		return v
	}
	return def
}

// Stop ends what the object runs: its steps, and its deliveries. The
// platform calls it when it takes the object away; a new object over the
// same storage goes on from there.
func (o *Object) Stop() { o.stop() }

// Idle returns once the object runs nothing: no session goroutine, and no
// delivery to another object under way (one that waits for its retry is
// the alarm's). A platform keeps the object alive until then (on Workers,
// waitUntil).
func (o *Object) Idle(ctx context.Context) error {
	for {
		o.mu.Lock()
		idle, settled := !o.running && !o.out.sending, o.settled
		o.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-settled:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// settle wakes Idle's waiters to look again. Call it with o.mu held.
func (o *Object) settle() {
	close(o.settled)
	o.settled = make(chan struct{})
}

// meta is what the object stores about its session: how it started and,
// once it ended, how.
type meta struct {
	Init Init `json:"init"`
	// Closing says a close was queued: from then on the session is gone
	// for its callers, while it ends and deletes what it saved.
	Closing bool           `json:"closing,omitempty"`
	Ended   bool           `json:"ended,omitempty"`
	Closed  bool           `json:"closed,omitempty"`
	Final   session.Status `json:"final"`
}

// gone says callers no longer see the session: it was closed, or a close
// is queued.
func (m meta) gone() bool { return m.Closing || m.Closed }

// metaKey is the one key not under a session id: an object holds one
// session, which it learns from its first Create.
const metaKey = kindMeta

func writeMeta(m meta) Op {
	raw, _ := json.Marshal(m) // plain data: ids, recipes and JSON state
	return put(metaKey, raw)
}

func writeStatus(st statusRecord) Op {
	raw, _ := json.Marshal(st) // plain data
	return put(statusKey, raw)
}

// load reads the object's state from storage into memory, once. Call it
// with o.mu held.
func (o *Object) load() error {
	if o.loaded {
		return nil
	}
	raw, ok, err := o.host.Get(metaKey)
	if err != nil {
		return err
	}
	if ok {
		if err := json.Unmarshal(raw, &o.m); err != nil {
			return fmt.Errorf("meta: %w", err)
		}
		if err := o.loadSession(o.m.Init.SessionID); err != nil {
			return err
		}
	}
	if err := o.out.load(o.host); err != nil {
		return err
	}
	o.has, o.loaded = ok, true
	o.sendLocked()
	return nil
}

// loadSession reads id's mailbox and status.
func (o *Object) loadSession(id session.ID) error {
	raw, ok, err := o.host.Get(key(kindMailboxHead, id))
	if err != nil {
		return err
	}
	if ok {
		if o.head, err = strconv.ParseUint(string(raw), 10, 64); err != nil {
			return fmt.Errorf("mailbox head: %w", err)
		}
	}
	o.mail = nil
	err = scanAll(o.host, prefix(kindMailbox, id), func(e Entry) error {
		var w session.Wake
		if err := json.Unmarshal(e.Value, &w); err != nil {
			return fmt.Errorf("mailbox entry %s: %w", e.Key, err)
		}
		o.mail = append(o.mail, queued{seq: seqOf(e.Key), wake: w})
		return nil
	})
	if err != nil {
		return err
	}
	raw, ok, err = o.host.Get(statusKey)
	if err != nil || !ok {
		return err
	}
	return json.Unmarshal(raw, &o.st)
}

// Create implements Ref.
func (o *Object) Create(ctx context.Context, init Init) error {
	if init.SessionID == "" {
		return fmt.Errorf("%w: session id is required", session.ErrInvalid)
	}
	if init.Specialist != "" && init.Worker != "" {
		return fmt.Errorf("%w: specialist and worker are exclusive", session.ErrInvalid)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.load(); err != nil {
		return err
	}
	if o.has {
		return session.ErrExists
	}
	if err := o.secrets.Put(ctx, init.SessionID, session.Secrets{Auth: init.Auth}); err != nil {
		return err
	}
	init.Auth = session.AuthContext{}
	m := meta{Init: init}
	st := statusRecord{Status: o.turnOf(init, nil).Status()}
	ops := []Op{writeMeta(m), writeStatus(st)}
	var mail []queued
	if init.Task != "" {
		// A task session's task is its first prompt, in the same write.
		w := session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: init.Task}}
		raw, _ := json.Marshal(w) // plain data
		ops = append(ops, put(seqKey(kindMailbox, init.SessionID, 1), raw), put(key(kindMailboxHead, init.SessionID), []byte("1")))
		mail = []queued{{seq: 1, wake: w}}
	}
	if err := o.host.Write(ops...); err != nil {
		return err
	}
	o.has, o.m, o.st, o.mail = true, m, st, mail
	if init.Task != "" {
		o.head = 1
	}
	o.start()
	return nil
}

// Wake implements Ref. Credentials on a prompt or resume are sealed into
// the object's secrets; the mailbox, and so the journal, gets the wake
// without them. A close of an ended session deletes what it left.
func (o *Object) Wake(ctx context.Context, w session.Wake) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.load(); err != nil {
		return err
	}
	if !o.has {
		return session.ErrNotFound
	}
	m := o.m
	id := m.Init.SessionID
	switch {
	case w.Kind != session.WakeClose && (m.Ended || m.gone()):
		return session.ErrNotFound
	case w.Kind == session.WakeClose && m.gone():
		return nil
	case w.Kind == session.WakeClose && m.Ended:
		return o.purge(ctx)
	}
	var ops []Op
	switch w.Kind {
	case session.WakeClose:
		m.Closing = true
		ops = append(ops, writeMeta(m))
	case session.WakePrompt:
		if err := o.secrets.Put(ctx, id, session.Secrets{Auth: w.Prompt.Auth}); err != nil {
			return err
		}
		w.Prompt.Auth = w.Prompt.Auth.WithoutSecrets()
	case session.WakeResume:
		if err := o.secrets.Put(ctx, id, session.Secrets{Auth: w.Resume.Auth}); err != nil {
			return err
		}
		w.Resume.Auth = w.Resume.Auth.WithoutSecrets()
	}
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	head := o.head + 1
	ops = append(ops, put(seqKey(kindMailbox, id, head), raw), put(key(kindMailboxHead, id), []byte(strconv.FormatUint(head, 10))))
	if err := o.host.Write(ops...); err != nil {
		return err
	}
	o.m, o.head = m, head
	o.mail = append(o.mail, queued{seq: head, wake: w})
	select {
	case o.notify <- struct{}{}:
	default:
	}
	o.start()
	return nil
}

// Alarm is the platform's alarm: it restarts a session whose object
// stopped while a step ran, and delivers what the object still owes other
// objects.
func (o *Object) Alarm(context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.armed = time.Time{} // the platform fired it
	if err := o.load(); err != nil {
		return err
	}
	if o.has && !o.m.Ended {
		o.start()
	}
	o.sendLocked()
	return nil
}

// Status implements Ref: the status of the session's last write, from
// memory, without running the session.
func (o *Object) Status(context.Context) (session.Status, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.openLocked(); err != nil {
		return session.Status{State: session.Unknown}, err
	}
	return o.st.Status, nil
}

// Jobs implements Ref.
func (o *Object) Jobs(context.Context) ([]session.ID, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.openLocked(); err != nil {
		return nil, err
	}
	return o.st.Jobs, nil
}

// Head implements Ref.
func (o *Object) Head(ctx context.Context) (session.Seq, error) {
	id, err := o.openID()
	if err != nil {
		return 0, err
	}
	return o.events.Head(ctx, id)
}

// Subscribe implements Ref.
func (o *Object) Subscribe(ctx context.Context, after session.Seq) (session.Subscription, error) {
	id, err := o.openID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	deltas := o.deltas.Subscribe(ctx, id)
	ch, err := o.events.Subscribe(ctx, id, after)
	if err != nil {
		cancel()
		return nil, err
	}
	return &subscription{ch: session.Merge(ctx, ch, deltas), cancel: cancel}, nil
}

// openID is the id of the object's session unless it is unknown or closed.
func (o *Object) openID() (session.ID, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.openLocked(); err != nil {
		return "", err
	}
	return o.m.Init.SessionID, nil
}

// openLocked is session.ErrNotFound unless the object holds a session its
// callers see. Call it with o.mu held.
func (o *Object) openLocked() error {
	if err := o.load(); err != nil {
		return err
	}
	if !o.has || o.m.gone() {
		return session.ErrNotFound
	}
	return nil
}

// subscription is a session.Subscription over the event log's channel.
type subscription struct {
	ch     <-chan session.Event
	cancel context.CancelFunc
}

func (s *subscription) Events() <-chan session.Event { return s.ch }

// Err is nil: a log tail ends only when it is closed, or its session is.
func (s *subscription) Err() error { return nil }

func (s *subscription) Close() error {
	s.cancel()
	return nil
}

// start runs the session on a goroutine unless one runs it. Call it with
// o.mu held.
func (o *Object) start() {
	o.kick = true
	if !o.running {
		o.running = true
		go o.run()
	}
}

// run runs the session until it suspends (it has nothing to do), ends, or
// the object can no longer write its storage, then again while requests
// came in meanwhile or wakes wait: a replayed suspend ends a run that never
// looked at them.
func (o *Object) run() {
	for {
		ok := o.runOnce()
		o.mu.Lock()
		// A run that stopped on its storage waits for the alarm or a request.
		if !o.kick && (!ok || len(o.mail) == 0 || o.m.Ended) {
			o.running = false
			o.settle()
			o.mu.Unlock()
			return
		}
		o.mu.Unlock()
	}
}

// stopped is the panic an executor raises when the object's storage fails:
// the run stops, and the next request or alarm replays it from storage.
type stopped struct{ err error }

// fail stops the run on err (stopped).
func fail(err error) { panic(stopped{err}) }

// runOnce runs the session from its storage once: it replays the journal,
// then runs live until the session suspends or ends. ok is false when it
// stopped on its storage.
func (o *Object) runOnce() (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			s, isStop := r.(stopped)
			if !isStop {
				panic(r)
			}
			slog.Error("session object stopped", "area", telemetry.AreaRuntime, "error", s.err)
			// What the harness holds may be ahead of storage.
			o.harness.close()
			ok = false
		}
	}()
	o.mu.Lock()
	o.kick = false
	err := o.load()
	has, m := o.has, o.m
	o.mu.Unlock()
	if err != nil {
		fail(err)
	}
	if !has || m.Ended {
		return true
	}
	turn, err := o.turn(m.Init)
	if err != nil {
		fail(err)
	}
	ex := newExecutor(o, m.Init, turn)
	result, runErr := loop.Run(ex, turn)
	if errors.Is(runErr, loop.ErrSuspended) {
		ex.suspend()
		return true
	}
	ex.finish(result, runErr)
	return true
}

// turnOf is init's Turn, from c when the session suspended before.
func (o *Object) turnOf(init Init, c *loop.Carry) *loop.Turn {
	ident := loop.Identity{Specialist: init.Specialist, Worker: init.Worker, Parent: init.Parent, Mounts: init.Mounts, Task: init.Task != ""}
	return loop.NewTurn(init.SessionID, ident, init.MCPServers, init.State, c)
}

// turn is the session's Turn: from init, then from its carry
// (loop.Carry) when it suspended before.
func (o *Object) turn(init Init) (*loop.Turn, error) {
	raw, ok, err := o.host.Get(key(kindCarry, init.SessionID))
	if err != nil || !ok {
		return o.turnOf(init, nil), err
	}
	var c loop.Carry
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return o.turnOf(init, &c), nil
}

// purge deletes what an ended session left, when it is closed: everything
// but its final meta. Call it with o.mu held.
func (o *Object) purge(ctx context.Context) error {
	m := o.m
	m.Closing, m.Closed = true, true
	ops, err := o.leftovers(m.Init.SessionID, true)
	if err != nil {
		return err
	}
	if o.cfg.Agent.Brain != nil {
		ops = append(ops, o.out.post(letter{Forget: m.Init.SessionID})...)
	}
	if err := o.events.commit(m.Init.SessionID, nil, append(ops, writeMeta(m))...); err != nil {
		return err
	}
	o.m = m
	o.closed(ctx, m.Init.SessionID)
	o.sendLocked()
	return nil
}

// leftovers are the deletes of what an ended session no longer needs (its
// journal, mailbox, carry, call records and credentials) and, with all, of
// everything it saved: it was closed.
func (o *Object) leftovers(id session.ID, all bool) ([]Op, error) {
	kinds := []string{kindJournal, kindMailbox, kindCall}
	if all {
		kinds = append(kinds, kindChild, kindCheckpoint, kindBody, kindEvent)
	}
	var ops []Op
	for _, kind := range kinds {
		more, err := deleteAll(o.host, prefix(kind, id))
		if err != nil {
			return nil, err
		}
		ops = append(ops, more...)
	}
	ops = append(ops, del(key(kindCarry, id)), del(key(kindMailboxHead, id)), del(key(kindSecrets, id)))
	if all {
		ops = append(ops, del(key(kindSnapshot, id)), del(key(kindEventHead, id)))
	}
	return ops, nil
}

// closed drops from memory what a close deleted: the mailbox, the
// snapshot's revision, and the event log, whose tails end. Call it with
// o.mu held.
func (o *Object) closed(ctx context.Context, id session.ID) {
	o.mail, o.head = nil, 0
	o.snapshots.forget(id)
	o.events.forget(ctx, id)
}
