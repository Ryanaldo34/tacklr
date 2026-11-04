package durableobject

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/loop"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// JobHandler runs a named job: the body of a job session that a tool starts
// with CallContext.Schedule.
type JobHandler func(ctx context.Context, task string) (string, error)

// Config is what every session object of a deployment runs with.
type Config struct {
	// Agent is what every session runs; a specialist session runs its
	// specialist on it.
	Agent tacklr.AgentOptions
	// Sealer seals each session's credentials before they are stored.
	Sealer session.Sealer
	// Jobs are the named jobs a session can start.
	Jobs map[string]JobHandler
	// MaxToolCalls bounds how many tool calls of one reply run at once
	// (default 32).
	MaxToolCalls int
	// MaxChildren bounds a session's running children (default 1000).
	MaxChildren int
	// StepAttempts is how many times a step that failed on the network
	// runs (default 3).
	StepAttempts int
	// JournalLimit is how many entries a parent session journals before it
	// restarts from its state with an empty journal (default 1000).
	JournalLimit int
	// Watchdog is how long the alarm waits before it restarts a session
	// whose step stopped with its object (default one minute).
	Watchdog time.Duration
}

const (
	defaultMaxToolCalls = 32
	defaultMaxChildren  = 1000
	defaultStepAttempts = 3
	defaultJournalLimit = 1000
	defaultWatchdog     = time.Minute
)

// Object runs one session in one durable object. The platform makes one per
// object and calls it for every request and alarm. It implements Ref.
type Object struct {
	host      Host
	cfg       Config
	snapshots *snapshots
	secrets   *secretStore
	events    *eventLog

	mu sync.Mutex
	// running says a goroutine runs the session; ready closes once it has
	// replayed its journal, so the status it publishes is current. kick
	// says a request came in while it ran, so a run that is ending looks
	// again.
	running bool
	ready   chan struct{}
	kick    bool
	status  session.Status
	jobs    []session.ID
	// notify wakes the session when a wake reaches its mailbox.
	notify chan struct{}

	harness harnessState
}

// NewObject returns the object over host. It panics without cfg.Sealer.
func NewObject(host Host, cfg Config) *Object {
	if cfg.Sealer == nil {
		panic("durableobject: Config.Sealer is required")
	}
	cfg.MaxToolCalls = positive(cfg.MaxToolCalls, defaultMaxToolCalls)
	cfg.MaxChildren = positive(cfg.MaxChildren, defaultMaxChildren)
	cfg.StepAttempts = positive(cfg.StepAttempts, defaultStepAttempts)
	cfg.JournalLimit = positive(cfg.JournalLimit, defaultJournalLimit)
	cfg.Watchdog = positive(cfg.Watchdog, defaultWatchdog)
	return &Object{
		host:      host,
		cfg:       cfg,
		snapshots: &snapshots{s: host},
		secrets:   &secretStore{s: host, sealer: cfg.Sealer},
		events:    newEventLog(host),
		notify:    make(chan struct{}, 1),
	}
}

// positive is v, or def when v is zero or negative.
func positive[T int | time.Duration](v, def T) T {
	if v > 0 {
		return v
	}
	return def
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

func (o *Object) meta() (meta, bool, error) {
	raw, ok, err := o.host.Get(metaKey)
	if err != nil || !ok {
		return meta{}, false, err
	}
	var m meta
	err = json.Unmarshal(raw, &m)
	return m, true, err
}

func writeMeta(m meta) Op {
	raw, _ := json.Marshal(m) // plain data: ids, recipes and JSON state
	return put(metaKey, raw)
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
	if _, ok, err := o.meta(); err != nil || ok {
		return cmp.Or(err, session.ErrExists)
	}
	if err := o.secrets.Put(ctx, init.SessionID, session.Secrets{Auth: init.Auth}); err != nil {
		return err
	}
	init.Auth = session.AuthContext{}
	if err := o.host.Write(writeMeta(meta{Init: init})); err != nil {
		return err
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
	m, ok, err := o.meta()
	if err != nil {
		return err
	}
	if !ok {
		return session.ErrNotFound
	}
	id := m.Init.SessionID
	switch {
	case w.Kind != session.WakeClose && (m.Ended || m.gone()):
		return session.ErrNotFound
	case w.Kind == session.WakeClose && m.gone():
		return nil
	case w.Kind == session.WakeClose && m.Ended:
		m.Closing, m.Closed = true, true
		return o.clear(ctx, m, true)
	}
	ops := []Op{}
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
	head, err := o.mailboxHead(id)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	head++
	ops = append(ops, put(seqKey(kindMailbox, id, head), raw), put(key(kindMailboxHead, id), []byte(strconv.FormatUint(head, 10))))
	if err := o.host.Write(ops...); err != nil {
		return err
	}
	select {
	case o.notify <- struct{}{}:
	default:
	}
	o.start()
	return nil
}

func (o *Object) mailboxHead(id session.ID) (uint64, error) {
	raw, ok, err := o.host.Get(key(kindMailboxHead, id))
	if err != nil || !ok {
		return 0, err
	}
	return strconv.ParseUint(string(raw), 10, 64)
}

// Alarm is the platform's alarm: it restarts a session whose object
// stopped while a step ran.
func (o *Object) Alarm(context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	m, ok, err := o.meta()
	if err == nil && ok && !m.Ended {
		o.start()
	}
	return err
}

// Status implements Ref. It waits for a restarted session to replay its
// journal, so the status is current.
func (o *Object) Status(ctx context.Context) (session.Status, error) {
	st := session.Status{State: session.Unknown}
	m, live, err := o.live(ctx)
	switch {
	case err != nil:
		return st, err
	case !live:
		return m.Final, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status, nil
}

// Jobs implements Ref.
func (o *Object) Jobs(ctx context.Context) ([]session.ID, error) {
	_, live, err := o.live(ctx)
	if err != nil || !live {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.jobs, nil
}

// live is the session's meta, and whether it still runs: when it does, live
// starts it if it stopped and waits until it has replayed its journal. A
// closed session is not found.
func (o *Object) live(ctx context.Context) (meta, bool, error) {
	o.mu.Lock()
	m, ok, err := o.meta()
	if err != nil || !ok || m.gone() {
		o.mu.Unlock()
		return m, false, cmp.Or(err, session.ErrNotFound)
	}
	if m.Ended {
		o.mu.Unlock()
		return m, false, nil
	}
	ready := o.start()
	o.mu.Unlock()
	select {
	case <-ready:
		return m, true, nil
	case <-ctx.Done():
		return m, false, ctx.Err()
	}
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
	ch, err := o.events.Subscribe(ctx, id, after)
	if err != nil {
		cancel()
		return nil, err
	}
	return &subscription{ch: ch, cancel: cancel}, nil
}

// openID is the id of the object's session unless it is unknown or closed.
func (o *Object) openID() (session.ID, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	m, ok, err := o.meta()
	if err != nil || !ok || m.gone() {
		return "", cmp.Or(err, session.ErrNotFound)
	}
	return m.Init.SessionID, nil
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

// start runs the session on a goroutine unless one runs it, and returns the
// channel that closes once it has replayed its journal. Call it with o.mu
// held.
func (o *Object) start() <-chan struct{} {
	o.kick = true
	if !o.running {
		o.running = true
		o.ready = make(chan struct{})
		go o.run()
	}
	return o.ready
}

// run runs the session until it ends, or until the object can no longer
// write its storage. A suspend restarts it from its carried state.
func (o *Object) run() {
	for {
		again := o.runOnce()
		o.mu.Lock()
		if !again && !o.kick {
			o.running = false
			o.closeReady()
			o.mu.Unlock()
			return
		}
		o.mu.Unlock()
	}
}

// closeReady closes ready once. Call it with o.mu held.
func (o *Object) closeReady() {
	select {
	case <-o.ready:
	default:
		close(o.ready)
	}
}

// stopped is the panic an executor raises when the object's storage fails:
// the run stops, and the next request or alarm replays it from storage.
type stopped struct{ err error }

// runOnce runs the session from its storage once: it replays the journal,
// then runs live. It reports whether to run again (a suspend).
func (o *Object) runOnce() (again bool) {
	defer func() {
		if r := recover(); r != nil {
			s, ok := r.(stopped)
			if !ok {
				panic(r)
			}
			slog.Error("session object stopped", "area", telemetry.AreaRuntime, "error", s.err)
			again = false
		}
	}()
	o.mu.Lock()
	o.kick = false
	m, ok, err := o.meta()
	o.mu.Unlock()
	if err != nil {
		panic(stopped{err})
	}
	if !ok || m.Ended {
		return false
	}
	turn, err := o.turn(m.Init)
	if err != nil {
		panic(stopped{err})
	}
	ex := newExecutor(o, m.Init, turn)
	result, runErr := loop.Run(ex, turn, m.Init.Task)
	if errors.Is(runErr, loop.ErrSuspended) {
		ex.suspend()
		return true
	}
	ex.finish(m, result, runErr)
	return false
}

// carry is a session's state across a suspend: everything the session
// holds that is not in its snapshot. The session continues with no step
// running: idle, or in a turn that waits on jobs (Suspended).
type carry struct {
	MCP       []session.MCPServer `json:"mcp,omitempty"`
	Mounts    []session.MountRecipe
	Inbox     []*message.Message
	NextMCP   []session.MCPServer
	State     map[string]any
	Round     *tacklr.Round
	Rev       session.Revision
	Result    string
	Terminal  session.State
	Yielded   bool
	Finished  bool
	Suspended bool
	Jobs      []loop.Job
}

// turn is the session's Turn: from init, then from its carry when it
// suspended before.
func (o *Object) turn(init Init) (*loop.Turn, error) {
	t := &loop.Turn{
		SessionID: init.SessionID,
		Identity: loop.Identity{
			Specialist: init.Specialist,
			Worker:     init.Worker,
			Parent:     init.Parent,
			Mounts:     adapter.ApplyAuth(init.Mounts, session.AuthContext{}),
		},
		MCP:   init.MCPServers,
		State: maps.Clone(init.State),
	}
	raw, ok, err := o.host.Get(key(kindCarry, init.SessionID))
	if err != nil || !ok {
		return t, err
	}
	var c carry
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	t.MCP, t.Mounts, t.Inbox, t.NextMCP, t.State = c.MCP, c.Mounts, c.Inbox, c.NextMCP, c.State
	t.Round, t.Rev, t.Result, t.Terminal, t.Yielded = c.Round, c.Rev, c.Result, c.Terminal, c.Yielded
	t.Finished, t.Suspended, t.Jobs = c.Finished, c.Suspended, c.Jobs
	return t, nil
}

func carryOf(t *loop.Turn) carry {
	return carry{
		MCP: t.MCP, Mounts: t.Mounts, Inbox: t.Inbox, NextMCP: t.NextMCP, State: t.State,
		Round: t.Round, Rev: t.Rev, Result: t.Result, Terminal: t.Terminal, Yielded: t.Yielded,
		Finished: t.Finished, Suspended: t.Suspended, Jobs: t.Jobs,
	}
}

// publish copies what Status and Jobs report from t. The executor calls it
// from the goroutine that runs the session, while t does not change.
func (o *Object) publish(t *loop.Turn) {
	ids := make([]session.ID, len(t.Jobs))
	for i, j := range t.Jobs {
		ids[i] = j.ID
	}
	o.mu.Lock()
	o.status, o.jobs = t.Status(), ids
	o.mu.Unlock()
}

// clear deletes the state an ended session no longer needs (its journal,
// mailbox, carry and credentials) and writes m. With all, it also deletes
// its snapshot, outcomes and events: the session was closed. Call it with
// o.mu held.
func (o *Object) clear(ctx context.Context, m meta, all bool) error {
	id := m.Init.SessionID
	var ops []Op
	for _, kind := range []string{kindJournal, kindMailbox} {
		more, err := deleteAll(o.host, prefix(kind, id))
		if err != nil {
			return err
		}
		ops = append(ops, more...)
	}
	ops = append(ops, del(key(kindCarry, id)), del(key(kindMailboxHead, id)), writeMeta(m))
	if all {
		children, err := deleteAll(o.host, prefix(kindChild, id))
		if err != nil {
			return err
		}
		ops = append(ops, children...)
	}
	errs := []error{o.host.Write(ops...), o.secrets.Delete(ctx, id)}
	if all {
		errs = append(errs, o.snapshots.Delete(ctx, id), o.events.CloseSession(ctx, id))
	}
	return errors.Join(errs...)
}
