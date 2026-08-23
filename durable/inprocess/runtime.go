package inprocess

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
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
	id      durable.SessionID
	agentID string
	mcp     []mcp.MCPConfig
	etag    string
	mounts  []durable.MountRecipe

	mu         sync.Mutex
	signals    chan signal
	cancelTurn context.CancelFunc
	turnDone   chan struct{}
	closed     bool
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

// Option configures New.
type Option func(*Runtime)

// WithSnapshotStore replaces the memory SnapshotStore.
func WithSnapshotStore(s durable.SnapshotStore) Option {
	return func(r *Runtime) {
		if s != nil {
			r.snapshots = s
		}
	}
}

// WithProjection sets how a turn tree is published. Nil is ignored (Fuse stays).
func WithProjection(p vfs.Projection) Option {
	return func(r *Runtime) {
		if p != nil {
			r.projection = p
		}
	}
}

// New constructs an in-process Runtime. Hosts pass a Catalog; there is no
// backend plugin registry.
func New(catalog durable.Catalog, opts ...Option) *Runtime {
	if catalog == nil {
		panic("inprocess: Catalog is required")
	}
	r := &Runtime{
		catalog:    catalog,
		snapshots:  NewMemorySnapshot(),
		events:     NewMemoryEventLog(),
		projection: vfs.FuseProjection{},
		sessions:   make(map[durable.SessionID]*sessionProc),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	if r.snapshots == nil {
		r.snapshots = NewMemorySnapshot()
	}
	return r
}

// Catalog returns the agent catalog.
func (r *Runtime) Catalog() durable.Catalog { return r.catalog }

// CreateSession implements durable.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req durable.CreateSession) (durable.SessionID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	agentID := strings.TrimSpace(req.AgentID)
	if agentID == "" {
		agentID = r.catalog.DefaultID()
	}
	if agentID != "" {
		if _, ok := r.catalog.Lookup(agentID); !ok {
			return "", fmt.Errorf("%w: %s", durable.ErrAgentNotFound, agentID)
		}
	}
	id := req.SessionID
	if id == "" {
		id = durable.SessionID(uuid.NewString())
	}
	r.mu.Lock()
	if _, exists := r.sessions[id]; exists {
		r.mu.Unlock()
		return "", fmt.Errorf("%w: %s", durable.ErrSessionExists, id)
	}
	p := &sessionProc{
		id:      id,
		agentID: agentID,
		mcp:     slices.Clone(req.MCPServers),
		mounts:  durable.ApplyAuth(req.Mounts, durable.AuthContext{}),
		signals: make(chan signal, 8),
	}
	r.sessions[id] = p
	r.mu.Unlock()
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

func (r *Runtime) waitPriorTurn(ctx context.Context, p *sessionProc) error {
	p.mu.Lock()
	cancel := p.cancelTurn
	done := p.turnDone
	p.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
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
	return r.send(ctx, sessionID, signal{kind: sigPrompt, prompt: msg, turnCtx: ctx})
}

// Resume implements durable.Runtime.
func (r *Runtime) Resume(ctx context.Context, sessionID durable.SessionID, resume durable.Resume) error {
	return r.send(ctx, sessionID, signal{kind: sigResume, resume: resume, turnCtx: ctx})
}

// Cancel implements durable.Runtime.
func (r *Runtime) Cancel(ctx context.Context, sessionID durable.SessionID) error {
	p, err := r.get(sessionID)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.cancelTurn != nil {
		p.cancelTurn()
	}
	p.mu.Unlock()
	_ = r.waitPriorTurn(ctx, p)
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
	if p.cancelTurn != nil {
		p.cancelTurn()
	}
	p.mu.Unlock()
	if already {
		return durable.ErrSessionNotFound
	}
	_ = r.waitPriorTurn(ctx, p)
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
	durable.ClearSessionVFS(r.catalog, sessionID)
	r.mu.Lock()
	delete(r.sessions, sessionID)
	r.mu.Unlock()
	return nil
}

func (r *Runtime) applyPromptMeta(p *sessionProc, msg durable.Prompt) error {
	if msg.AgentID != "" {
		if _, ok := r.catalog.Lookup(msg.AgentID); !ok {
			return durable.ErrAgentNotFound
		}
		p.mu.Lock()
		p.agentID = msg.AgentID
		p.mu.Unlock()
	}
	if msg.MCPServers != nil {
		p.mu.Lock()
		p.mcp = slices.Clone(msg.MCPServers)
		p.mu.Unlock()
	}
	return nil
}

type subscription struct {
	ch     <-chan streaming.StreamEvent
	cancel context.CancelFunc
}

func (s *subscription) Events() <-chan streaming.StreamEvent { return s.ch }
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
	ch, err := r.events.Subscribe(subCtx, sessionID, after)
	if err != nil {
		cancel()
		return nil, err
	}
	return &subscription{ch: ch, cancel: cancel}, nil
}

func (r *Runtime) loop(p *sessionProc) {
	for sig := range p.signals {
		if sig.kind == sigClose {
			if sig.reply != nil {
				sig.reply <- nil
			}
			return
		}
		parent := sig.turnCtx
		if parent == nil {
			parent = context.Background()
		}
		if err := r.waitPriorTurn(parent, p); err != nil {
			if sig.reply != nil {
				sig.reply <- err
			}
			continue
		}
		kind := "prompt"
		var user *tacklr.Message
		var resume map[string][]byte
		var auth durable.AuthContext
		if sig.kind == sigResume {
			kind = "resume"
			resume = sig.resume.Responses
			auth = sig.resume.Auth
		} else {
			if err := r.applyPromptMeta(p, sig.prompt); err != nil {
				if sig.reply != nil {
					sig.reply <- err
				}
				continue
			}
			user = promptMessage(sig.prompt)
			auth = sig.prompt.Auth
		}
		p.mounts = durable.ApplyAuth(p.mounts, auth)
		bindings := durable.BindingsForTurn(p.mounts, auth)
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
			ctx, end := recordTurn(turnCtx, p.agentID, string(p.id), kind)
			outcome := r.driveTurn(ctx, p, user, resume, bindings)
			end(nil, outcome == turnCancelled || turnCtx.Err() != nil)
		}()
	}
}

func promptMessage(msg durable.Prompt) *tacklr.Message {
	if msg.UserMessage != nil {
		return msg.UserMessage
	}
	return &tacklr.Message{Role: tacklr.RoleUser, Content: msg.Text}
}

var _ durable.Runtime = (*Runtime)(nil)
