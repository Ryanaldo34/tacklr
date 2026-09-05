package inprocess

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

type signalKind int

const (
	sigPrompt signalKind = iota
	sigResume
	sigClose
)

type signal struct {
	kind    signalKind
	prompt  durable.Prompt
	resume  durable.Resume
	reply   chan error
	turnCtx context.Context
}

type sessionProc struct {
	id         durable.SessionID
	agentID    string
	specialist string
	parent     durable.SessionID
	mcp        []mcp.MCPConfig
	revision   durable.Revision
	mounts     []durable.MountRecipe
	auth       durable.AuthContext
	children   []durable.SessionID

	mu         sync.Mutex
	signals    chan signal
	cancelTurn context.CancelFunc
	turnDone   chan struct{}
	closed     bool
	// terminal is the last committed turn outcome. Status reads only this.
	// A new Prompt/Resume clears it before the caller’s Prompt returns so a
	// completed session that is prompted again is running, not stale-complete.
	// HITL yield is not terminal; Status stays running until Resume resolves it.
	terminal durable.SessionState
	result   string
	termErr  error
	yielded  bool
	// childParks maps a parent tool call id (get_child / blocking spawn) to the
	// child session that should receive the next Resume payload.
	childParks map[string]durable.SessionID
	// state is CreateSession.State until the first persist writes it into the checkpoint.
	state map[string]any
	// stateGen increments whenever state is replaced so persist can detect a concurrent merge.
	stateGen uint64

	// inbox is the wait-loop FIFO for Prompt-during-turn steers and auto-collected
	// job results. Not SnapshotStore. Dropped on Cancel/Close. Methods are
	// mutex-safe; hold mu around terminal checks plus Push so Cancel Drop
	// cannot race a live Queue.
	inbox adapter.Inbox
	// nextAgentID / nextMCP apply on the next idle construct, not the live harness.
	nextAgentID string
	nextMCP     []mcp.MCPConfig

	// kids is the parent context for child Prompt. Canceled by stopChildren
	// (Cancel, Close, client-stopped turn). Not canceled when a turn parks.
	kids     context.Context
	stopKids context.CancelFunc
}

// Runtime is the in-process durable.Runtime: one goroutine per session.
type Runtime struct {
	catalog    durable.Catalog
	snapshots  durable.SnapshotStore
	events     *MemoryEventLog
	projection vfs.Projection

	mu       sync.RWMutex
	sessions map[durable.SessionID]*sessionProc
}

// Config is the single in-process host config for New.
type Config struct {
	Catalog durable.Catalog
	// Snapshots is the session record. Required.
	Snapshots  durable.SnapshotStore
	Projection vfs.Projection
}

// New constructs an in-process Runtime.
func New(cfg Config) *Runtime {
	if cfg.Catalog == nil {
		panic("inprocess: Catalog is required")
	}
	if cfg.Snapshots == nil {
		panic("inprocess: Snapshots is required")
	}
	proj := cfg.Projection
	if proj == nil {
		proj = vfs.FuseProjection{}
	}
	return &Runtime{
		catalog:    cfg.Catalog,
		snapshots:  cfg.Snapshots,
		events:     NewMemoryEventLog(),
		projection: proj,
		sessions:   make(map[durable.SessionID]*sessionProc),
	}
}

// CreateSession implements durable.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req durable.CreateSession) (durable.SessionID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var parent *sessionProc
	if req.Parent != "" {
		var err error
		parent, err = r.get(req.Parent)
		if err != nil {
			return "", err
		}
	}
	agentID := strings.TrimSpace(req.AgentID)
	if agentID == "" && parent != nil {
		agentID = parent.agentID
	}
	if agentID == "" {
		agentID = r.catalog.DefaultID()
	}
	if agentID != "" {
		if _, ok := r.catalog.Lookup(agentID); !ok {
			return "", fmt.Errorf("%w: %s", durable.ErrAgentNotFound, agentID)
		}
	}
	if specName := strings.TrimSpace(req.Specialist); specName != "" {
		spec, ok := r.catalog.Lookup(agentID)
		if !ok {
			return "", fmt.Errorf("%w: %s", durable.ErrAgentNotFound, agentID)
		}
		if _, err := adapter.OverlaySpecialist(spec, specName); err != nil {
			return "", err
		}
	}
	seed, err := adapter.EncodeUserState(req.State)
	if err != nil {
		return "", err
	}
	id := req.SessionID
	if id == "" {
		id = durable.SessionID(uuid.NewString())
	}
	mcp := slices.Clone(req.MCPServers)
	mounts := adapter.ApplyAuth(req.Mounts, durable.AuthContext{})
	if parent != nil {
		if len(mcp) == 0 {
			mcp = slices.Clone(parent.mcp)
		}
		if len(mounts) == 0 {
			mounts = slices.Clone(parent.mounts)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[id]; exists {
		return "", fmt.Errorf("%w: %s", durable.ErrSessionExists, id)
	}
	p := &sessionProc{
		id:         id,
		agentID:    agentID,
		specialist: strings.TrimSpace(req.Specialist),
		parent:     req.Parent,
		mcp:        mcp,
		mounts:     mounts,
		signals:    make(chan signal, 8),
		childParks: make(map[string]durable.SessionID),
	}
	p.kids, p.stopKids = context.WithCancel(context.Background())
	if parent != nil {
		p.auth = parent.auth
	}
	p.state = seed
	r.sessions[id] = p
	if parent != nil {
		parent.mu.Lock()
		parent.children = append(parent.children, id)
		parent.mu.Unlock()
	}
	go r.loop(p) //nolint:gosec // G118: session wait loop outlives CreateSession
	return id, nil
}

func (r *Runtime) get(id durable.SessionID) (*sessionProc, error) {
	r.mu.RLock()
	p, ok := r.sessions[id]
	r.mu.RUnlock()
	if !ok {
		return nil, durable.ErrSessionNotFound
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, durable.ErrSessionNotFound
	}
	return p, nil
}

func (r *Runtime) send(ctx context.Context, id durable.SessionID, sig signal) error {
	p, err := r.get(id)
	if err != nil {
		return err
	}
	if sig.reply == nil {
		sig.reply = make(chan error, 1)
	}
	select {
	case p.signals <- sig:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-sig.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitPriorTurn waits for the in-flight turn. abort cancels it first (Cancel,
// Close). Prompt queues while a turn is live or parked; idle Prompt still
// waits so the previous goroutine has exited. Resume must not abort: HITL
// parks after leftover tools in the same batch finish; canceling them emits
// context.Canceled and drops function_call_output for the next model turn.
func (r *Runtime) waitPriorTurn(ctx context.Context, p *sessionProc, abort bool) error {
	p.mu.Lock()
	cancel := p.cancelTurn
	done := p.turnDone
	p.mu.Unlock()
	if abort && cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Prompt implements durable.Runtime.
func (r *Runtime) Prompt(ctx context.Context, sessionID durable.SessionID, msg durable.Prompt) error {
	encoded, err := adapter.EncodeUserState(msg.State)
	if err != nil {
		return err
	}
	msg.State = encoded
	return r.send(ctx, sessionID, signal{kind: sigPrompt, prompt: msg, turnCtx: ctx})
}

// Resume implements durable.Runtime.
func (r *Runtime) Resume(ctx context.Context, sessionID durable.SessionID, resume durable.Resume) error {
	encoded, err := adapter.EncodeUserState(resume.State)
	if err != nil {
		return err
	}
	resume.State = encoded
	return r.send(ctx, sessionID, signal{kind: sigResume, resume: resume, turnCtx: ctx})
}

// stopChildren Closes every nested session of p, including children already
// collected (dropped from p.children but still Status-able). Used by Close,
// Cancel, and when the original Prompt/Resume context is cancelled.
func (r *Runtime) stopChildren(ctx context.Context, p *sessionProc) {
	p.mu.Lock()
	p.children = nil
	if p.stopKids != nil {
		p.stopKids()
		p.kids, p.stopKids = context.WithCancel(context.Background())
	}
	parentID := p.id
	p.mu.Unlock()
	r.mu.RLock()
	var kids []durable.SessionID
	for id, child := range r.sessions {
		if child.parent == parentID {
			kids = append(kids, id)
		}
	}
	r.mu.RUnlock()
	for _, child := range kids {
		_ = r.Close(ctx, child)
	}
}

func (p *sessionProc) childCtx() context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kids
}

// Cancel implements durable.Runtime. It aborts the in-flight turn and stops
// child sessions. The parent session stays open for a later Prompt.
func (r *Runtime) Cancel(ctx context.Context, sessionID durable.SessionID) error {
	p, err := r.get(sessionID)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.inbox.Drop()
	p.nextAgentID = ""
	p.nextMCP = nil
	if p.cancelTurn != nil {
		p.cancelTurn()
	}
	p.mu.Unlock()
	r.stopChildren(ctx, p)
	_ = r.waitPriorTurn(ctx, p, true)
	r.events.EndSubscribers(sessionID)
	return nil
}

// Close implements durable.Runtime.
func (r *Runtime) Close(ctx context.Context, sessionID durable.SessionID) error {
	r.mu.RLock()
	p, ok := r.sessions[sessionID]
	r.mu.RUnlock()
	if !ok {
		return durable.ErrSessionNotFound
	}
	p.mu.Lock()
	already := p.closed
	p.closed = true
	p.inbox.Drop()
	p.nextAgentID = ""
	p.nextMCP = nil
	if p.cancelTurn != nil {
		p.cancelTurn()
	}
	p.mu.Unlock()
	if already {
		return durable.ErrSessionNotFound
	}
	r.stopChildren(ctx, p)
	_ = r.waitPriorTurn(ctx, p, true)
	reply := make(chan error, 1)
	select {
	case p.signals <- signal{kind: sigClose, reply: reply}:
		select {
		case <-reply:
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
	_ = r.snapshots.Delete(ctx, sessionID)
	_ = r.events.CloseSession(ctx, sessionID)
	r.mu.Lock()
	delete(r.sessions, sessionID)
	r.mu.Unlock()
	return nil
}

// Children implements durable.Runtime.
func (r *Runtime) Children(_ context.Context, parent durable.SessionID) ([]durable.SessionID, error) {
	p, err := r.get(parent)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.children), nil
}

// Status implements durable.Runtime. Yielded children stay running until HITL
// is resolved (parent-facing in-progress).
func (r *Runtime) Status(_ context.Context, id durable.SessionID) (durable.SessionStatus, error) {
	p, err := r.get(id)
	if err != nil {
		return durable.SessionStatus{ID: id, State: durable.SessionUnknown}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := durable.SessionStatus{
		ID:         p.id,
		Parent:     p.parent,
		Specialist: p.specialist,
		Kind:       "",
		State:      durable.SessionRunning,
		Result:     p.result,
		Err:        p.termErr,
	}
	if p.specialist != "" {
		st.Kind = durable.SessionKindSpecialist
	}
	switch p.terminal {
	case durable.SessionComplete, durable.SessionFailed:
		st.State = p.terminal
	default:
		st.State = durable.SessionRunning
		st.Waiting = p.yielded
	}
	return st, nil
}

func (r *Runtime) applyPromptMeta(p *sessionProc, msg durable.Prompt) error {
	p.mu.Lock()
	if msg.AgentID == "" {
		msg.AgentID = p.nextAgentID
	}
	if msg.MCPServers == nil {
		msg.MCPServers = p.nextMCP
	}
	p.nextAgentID = ""
	p.nextMCP = nil
	p.mu.Unlock()
	if msg.AgentID != "" {
		if _, ok := r.catalog.Lookup(msg.AgentID); !ok {
			return durable.ErrAgentNotFound
		}
	}
	if msg.AgentID == "" && msg.MCPServers == nil {
		return nil
	}
	p.mu.Lock()
	if msg.AgentID != "" {
		p.agentID = msg.AgentID
	}
	if msg.MCPServers != nil {
		p.mcp = slices.Clone(msg.MCPServers)
	}
	p.mu.Unlock()
	return nil
}

func (p *sessionProc) busy() bool {
	p.mu.Lock()
	yielded := p.yielded
	done := p.turnDone
	p.mu.Unlock()
	if yielded {
		return true
	}
	if done == nil {
		return false
	}
	select {
	case <-done:
		return false
	default:
		return true
	}
}

func (p *sessionProc) dropInbox() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inbox.Drop()
	p.nextAgentID = ""
	p.nextMCP = nil
}

// errPromptIdle means the turn already committed complete/failed, so this
// Prompt must wait and start an idle turn instead of joining the inbox.
var errPromptIdle = errors.New("prompt idle")

func (r *Runtime) queuePrompt(p *sessionProc, msg durable.Prompt) error {
	if msg.AgentID != "" {
		if _, ok := r.catalog.Lookup(msg.AgentID); !ok {
			return durable.ErrAgentNotFound
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal == durable.SessionComplete || p.terminal == durable.SessionFailed {
		return errPromptIdle
	}
	if msg.AgentID != "" {
		p.nextAgentID = msg.AgentID
	}
	if msg.MCPServers != nil {
		p.nextMCP = slices.Clone(msg.MCPServers)
	}
	p.mounts = adapter.ApplyAuth(p.mounts, msg.Auth)
	p.auth = msg.Auth
	p.state = adapter.MergeUserState(p.state, msg.State)
	p.stateGen++
	p.inbox.Push(adapter.UserFromPrompt(msg.Text, msg.UserMessage))
	return nil
}

type subscription struct {
	ch     <-chan tacklr.StreamEvent
	cancel context.CancelFunc
}

func (s *subscription) Events() <-chan tacklr.StreamEvent { return s.ch }
func (s *subscription) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Head is the current EventLog offset (protocol pumps subscribe after this).
func (r *Runtime) Head(ctx context.Context, sessionID durable.SessionID) (durable.Seq, error) {
	if _, err := r.get(sessionID); err != nil {
		return 0, err
	}
	return r.events.Head(ctx, sessionID)
}

// Subscribe implements durable.Runtime.
func (r *Runtime) Subscribe(ctx context.Context, sessionID durable.SessionID, after durable.Seq) (durable.Subscription, error) {
	if _, err := r.get(sessionID); err != nil {
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	ch, _ := r.events.Subscribe(subCtx, sessionID, after)
	return &subscription{ch: ch, cancel: cancel}, nil
}

func (r *Runtime) loop(p *sessionProc) {
	for sig := range p.signals {
		if sig.kind == sigClose {
			p.dropInbox()
			if sig.reply != nil {
				sig.reply <- nil
			}
			return
		}
		parent := sig.turnCtx
		if parent == nil {
			parent = context.Background()
		}
		if sig.kind == sigPrompt && p.busy() {
			err := r.queuePrompt(p, sig.prompt)
			if !errors.Is(err, errPromptIdle) {
				if sig.reply != nil {
					sig.reply <- err
				}
				continue
			}
			if err := r.waitPriorTurn(parent, p, false); err != nil {
				if sig.reply != nil {
					sig.reply <- err
				}
				continue
			}
		} else if err := r.waitPriorTurn(parent, p, sig.kind != sigResume); err != nil {
			if sig.reply != nil {
				sig.reply <- err
			}
			continue
		}
		kind := telemetry.TurnKindPrompt
		var user *tacklr.Message
		var resume map[string][]byte
		var auth durable.AuthContext
		var turnState map[string]any
		promptLen, resumeCount := 0, 0
		if sig.kind == sigResume {
			kind = telemetry.TurnKindResume
			resume = sig.resume.Responses
			resumeCount = len(resume)
			auth = sig.resume.Auth
			turnState = sig.resume.State
			r.forwardChildResume(parent, p, sig.resume)
		} else {
			if err := r.applyPromptMeta(p, sig.prompt); err != nil {
				if sig.reply != nil {
					sig.reply <- err
				}
				continue
			}
			user = adapter.UserFromPrompt(sig.prompt.Text, sig.prompt.UserMessage)
			auth = sig.prompt.Auth
			turnState = sig.prompt.State
			if user != nil {
				promptLen = len(user.Content)
			}
		}
		p.mu.Lock()
		p.mounts = adapter.ApplyAuth(p.mounts, auth)
		p.auth = auth
		p.yielded = false
		p.terminal = ""
		p.result = ""
		p.termErr = nil
		bindings := adapter.BindingsForTurn(p.mounts, auth)
		p.mu.Unlock()
		turnCtx, cancel := context.WithCancel(parent)
		done := make(chan struct{})
		p.mu.Lock()
		p.cancelTurn = cancel
		p.turnDone = done
		p.mu.Unlock()
		if sig.reply != nil {
			sig.reply <- nil
		}
		go func() {
			defer close(done)
			defer cancel()
			ctx, end := r.recordTurn(turnCtx, p.agentID, string(p.id), kind, promptLen, resumeCount)
			p.mu.Lock()
			overlay := adapter.MergeUserState(p.state, turnState)
			p.mu.Unlock()
			outcome := r.runTurn(ctx, p, user, resume, bindings, overlay)
			if outcome == turnCancelled {
				if err := parent.Err(); err != nil {
					r.stopChildren(context.WithoutCancel(parent), p)
				}
			}
			end(outcome)
		}()
	}
}

func (r *Runtime) noteOutcome(p *sessionProc, o turnOutcome) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch o {
	case turnComplete:
		p.terminal = durable.SessionComplete
		p.yielded = false
	case turnError, turnCancelled:
		p.terminal = durable.SessionFailed
		p.yielded = false
	case turnYield:
		p.yielded = true
		p.terminal = ""
	}
}
