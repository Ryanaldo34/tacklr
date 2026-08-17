package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/hostcontrol"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// turnHandle tracks one in-flight Registry turn so a follow-on prompt (steer)
// can cancel it and wait until the harness has finished and checkpointed.
type turnHandle struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when the forwarder exits (after harness channel drain)
}

type AgentSpec struct {
	Name string
	// Options is the canonical immutable agent definition. Registry overrides
	// only SessionID, Store fallback, MCP session overlays, and MountSession.
	Options tacklr.AgentOptions
	// FSRegistry resolves MountSpec.Profile (process-scoped). Required when FSBootstrap is set.
	FSRegistry *vfs.BackendRegistry
	// FSBootstrap mounts applied once when the host creates the session MountSession.
	// Requires a live VFSProjection (FUSE in production). If the projection
	// is not Available, the session has no MountSession and no VFS tools.
	FSBootstrap []vfs.MountSpec
}

// SessionView is the domain view of a wire session (returned by protocols).
type SessionView struct {
	SessionID     string
	ConfigOptions []ConfigOption
}

// TurnRequest describes a prompt or resume turn.
//
// Session mode (ACP): protocol BindTurn fills SessionID, AgentID, MCPServers,
// Load, and AllowMissingCheckpoint. Registry does not own wire envelopes.
//
// Direct mode (SSE): set AgentID and ThreadID. Load restores from harness store.
type TurnRequest struct {
	SessionID string
	AgentID   string
	ThreadID  string
	Prompt    string
	// UserMessage is multimodal user content (ACP). When set, preferred over Prompt.
	UserMessage *tacklr.Message
	Responses   map[string]json.RawMessage
	Load        bool

	// AllowMissingCheckpoint: when Load is true and the harness store has no
	// row, start a fresh agent instead of failing. Set by wire BindTurn for
	// sessions that may never have been checkpointed yet.
	AllowMissingCheckpoint bool

	// CWD is optional turn context (protocol may set from wire session).
	CWD string

	// MCPServers are session-scoped MCP configs for this turn.
	MCPServers []mcp.MCPConfig
}

// EventStream is a running agent turn. Events is closed when the current
// harness run finishes (complete, error, interrupt park) or the turn context
// is cancelled and the registry forwarder exits.
//
// Cancel cancels the turn context (session/cancel). Close ends the turn:
// cancel, Harness.Close (MCP/index), then nil the pointer.
// The next RunTurn reconstructs from the store checkpoint. Callers typically:
//
//	defer func() { stream.Cancel(); stream.Close() }()
//
// ResumeInterrupts must run before Close (same turn, same harness).
type EventStream struct {
	Events  <-chan streaming.StreamEvent
	harness *tacklr.AgentHarness
	runCtx  context.Context
	cancel  context.CancelFunc
	closed  bool
	mu      sync.Mutex
}

// SessionID is the durable harness thread id, or empty.
func (s *EventStream) SessionID() string {
	if s == nil || s.harness == nil {
		return ""
	}
	return s.harness.SessionID()
}

// AskUserQuestion returns the ask_user_choice question for toolCallID, or empty.
func (s *EventStream) AskUserQuestion(toolCallID string) string {
	if s == nil || s.harness == nil {
		return ""
	}
	return s.harness.AskUserQuestion(toolCallID)
}

// VFS is the session mount table, or nil.
func (s *EventStream) VFS() *vfs.MountSession {
	if s == nil || s.harness == nil {
		return nil
	}
	return s.harness.VFS()
}

// TurnContext is the context for this turn (cancelled by session/cancel or parent).
func (s *EventStream) TurnContext() context.Context {
	return s.runCtx
}

// Cancelled reports whether the turn context has been cancelled.
func (s *EventStream) Cancelled() bool {
	return s.runCtx.Err() != nil
}

// Cancel cancels the turn context so producers stop. Safe to call multiple times.
// Does not release the harness; call Close after the event pump finishes.
func (s *EventStream) Cancel() {
	s.cancel()
}

// Close ends the turn (idempotent): cancel, release the harness, nil the pointer.
func (s *EventStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	closeTurnHarness(s.harness)
	s.harness = nil
}

func closeTurnHarness(h *tacklr.AgentHarness) {
	if h == nil {
		return
	}
	h.Close()
}

// ResumeInterrupts resolves pending interrupts and returns a new event stream
// from the same harness (ACP mid-turn elicitation resume).
func (s *EventStream) ResumeInterrupts(ctx context.Context, responses map[string][]byte) (<-chan streaming.StreamEvent, error) {
	if s.harness == nil {
		return nil, fmt.Errorf("event stream: no harness for resume")
	}
	c := s.runCtx
	if c.Err() != nil {
		c = ctx
	}
	return s.harness.ReturnFromInterrupt(c, responses)
}

// Registry serves agents over wire protocols (ACP, SSE, and others).
type Registry struct {
	agents       map[string]AgentSpec
	defaultAgent string
	store        stores.BaseStore
	tracer       trace.Tracer           // turn and child spans; default global
	instruments  *telemetry.Instruments // turn/tool metrics; default global
	activeTurns  sync.Map               // thread id → *turnHandle
	mountsMu     sync.Mutex
	mounts       map[string]*vfs.MountSession // thread id → host-owned session tree
	projection   VFSProjection                // nil → FuseProjection
	vfsAuth      *vfs.SessionAuth             // user-owned backend tokens (Drive, …)
}

// RegistryOption configures NewRegistry.
type RegistryOption func(*Registry)

// WithTracerProvider sets the TracerProvider for turn telemetry
// (tracer name telemetry.InstrumentationName). Nil uses the process global.
func WithTracerProvider(tp trace.TracerProvider) RegistryOption {
	return func(r *Registry) {
		if tp != nil {
			r.tracer = telemetry.TracerFromProvider(tp)
		}
	}
}

// WithTracer sets an explicit Tracer for turn telemetry.
// WithTracerProvider is the usual choice for consistent instrumentation names.
func WithTracer(t trace.Tracer) RegistryOption {
	return func(r *Registry) {
		if t != nil {
			r.tracer = t
		}
	}
}

// WithMeterProvider sets the MeterProvider for turn and tool metrics.
// Nil uses the process global meter.
func WithMeterProvider(mp metric.MeterProvider) RegistryOption {
	return func(r *Registry) {
		if mp != nil {
			r.instruments = telemetry.MustInstruments(telemetry.MeterFromProvider(mp))
		}
	}
}

// WithVFSProjection sets how a session tree is published to the host.
// Nil or omitted uses FuseProjection. Tests that need VFS tools without a
// kernel mount pass DirectProjection{}.
func WithVFSProjection(p VFSProjection) RegistryOption {
	return func(r *Registry) {
		r.projection = p
	}
}

// WithVFSAuth sets the session credential store used by user-owned VFS
// backends. Hosts must pass the same *SessionAuth to DriveFactory (and later
// Dropbox/SharePoint factories). Nil or omitted creates one on first use.
func WithVFSAuth(a *vfs.SessionAuth) RegistryOption {
	return func(r *Registry) {
		if a != nil {
			r.vfsAuth = a
		}
	}
}

// NewRegistry builds a registry. opts may set telemetry providers.
func NewRegistry(store stores.BaseStore, defaultAgent string, opts ...RegistryOption) *Registry {
	r := &Registry{
		agents:       make(map[string]AgentSpec),
		defaultAgent: defaultAgent,
		store:        store,
		tracer:       telemetry.Tracer(),
		instruments:  telemetry.MustInstruments(telemetry.Meter()),
		mounts:       make(map[string]*vfs.MountSession),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	if r.tracer == nil {
		r.tracer = telemetry.Tracer()
	}
	if r.instruments == nil {
		r.instruments = telemetry.MustInstruments(telemetry.Meter())
	}
	if r.vfsAuth == nil {
		r.vfsAuth = vfs.NewSessionAuth()
	}
	return r
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
	if r == nil {
		panic("server: nil Registry")
	}
	if strings.TrimSpace(agentID) == "" {
		panic("server: agent id is required")
	}
	if spec.Options.SessionID != "" || spec.Options.MountSession != nil {
		panic("server: AgentSpec.Options cannot contain session-owned fields")
	}
	if err := spec.Options.Validate(); err != nil {
		panic(fmt.Sprintf("server: register agent %q: %v", agentID, err))
	}
	r.agents[agentID] = spec
}

// DefaultAgent returns the registry default agent id.
func (r *Registry) DefaultAgent() string {
	return r.defaultAgent
}

// HasAgent reports whether agentID is registered.
func (r *Registry) HasAgent(agentID string) bool {
	_, ok := r.agents[agentID]
	return ok
}

// AgentModel returns the inference strategy for a registered agent, or nil.
func (r *Registry) AgentModel(agentID string) tacklr.InferenceStrategy {
	if agentID == "" {
		agentID = r.defaultAgent
	}
	spec, ok := r.agents[agentID]
	if !ok {
		return nil
	}
	return spec.Options.Model
}

// RecordSessionCreated records a session-created metric (called by protocols).
func (r *Registry) RecordSessionCreated(ctx context.Context) {
	r.instruments.RecordSessionCreated(ctx)
}

// ConfigOptions returns selectable agent config options for wire session responses.
func (r *Registry) ConfigOptions(currentAgent string) []ConfigOption {
	return r.buildConfigOptions(currentAgent)
}

// CancelSession cancels the in-flight turn context for the session (if any).
// Session state is preserved. The turn context is the single cancel signal for
// harness work, the registry forwarder, and runTurnStream.
// This is abort-only (ACP session/cancel). It does not start a new turn.
func (r *Registry) CancelSession(sessionID string) {
	if h, ok := r.activeTurns.Load(sessionID); ok {
		if th, ok := h.(*turnHandle); ok {
			th.cancel()
		}
	}
}

// DropLiveHarness is kept for session/close callers. The harness is turn-scoped
// and already released by EventStream.Close; this cancels an in-flight turn and
// closes the host-owned MountSession (FUSE included).
func (r *Registry) DropLiveHarness(sessionID string) {
	r.CancelSession(sessionID)
	r.closeSessionVFS(sessionID)
}

func (r *Registry) closeSessionVFS(sessionID string) {
	if r.vfsAuth != nil {
		r.vfsAuth.Clear(sessionID)
	}
	r.mountsMu.Lock()
	ms := r.mounts[sessionID]
	delete(r.mounts, sessionID)
	r.mountsMu.Unlock()
	if ms == nil {
		return
	}
	dir := ms.HostDir()
	if dir != "" {
		telemetry.EmitEvent(context.Background(), telemetry.EventFuseUnmount)
	}
	_ = ms.Close()
	if dir != "" {
		_ = os.Remove(dir)
	}
}

func (r *Registry) sessionVFS(ctx context.Context, threadID string, spec *AgentSpec) (*vfs.MountSession, error) {
	hasBindings := r.vfsAuth != nil && r.vfsAuth.HasBindings(threadID)
	if spec.FSRegistry == nil || (len(spec.FSBootstrap) == 0 && !hasBindings) {
		return nil, nil
	}
	// Production: FUSE is the VFS. No device → no tree, no VFS tools.
	// Tests may still attach a MountSession without a kernel mount.
	if !r.vfsProjection().Available() {
		return nil, nil
	}
	r.mountsMu.Lock()
	defer r.mountsMu.Unlock()
	if ms, ok := r.mounts[threadID]; ok {
		return ms, nil
	}
	ms, err := vfs.NewMountSession(threadID, spec.FSRegistry)
	if err != nil {
		return nil, err
	}
	if err := ms.Materialize(ctx, spec.FSBootstrap); err != nil {
		return nil, err
	}
	if hasBindings {
		for _, b := range r.vfsAuth.Bindings(threadID) {
			if err := ms.Mount(ctx, vfs.BindingSpec(b)); err != nil && !errors.Is(err, vfs.ErrAlreadyMounted) {
				_ = ms.Close()
				return nil, err
			}
		}
	}
	r.mounts[threadID] = ms
	return ms, nil
}

// waitPriorTurnIfAny cancels any in-flight turn for threadID and waits until
// the harness finishes (drain + checkpoint). No Registry deadline — the
// harness owns tool/turn timeouts. Parent ctx cancel still aborts the wait.
func (r *Registry) waitPriorTurnIfAny(ctx context.Context, threadID string) error {
	if threadID == "" {
		return nil
	}
	h, ok := r.activeTurns.Load(threadID)
	if !ok {
		return nil
	}
	th, ok := h.(*turnHandle)
	if !ok {
		return nil
	}
	th.cancel()
	select {
	case <-th.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunTurn starts a prompt or resume turn and returns a stream of events.
// Setup errors (unknown session/agent, validation) are returned synchronously.
// Runtime errors are delivered as StreamEventError on the channel.
//
// If a turn is already in flight, it is cancelled and this call waits until
// that harness exits before starting the new turn (session/prompt steer).
func (r *Registry) RunTurn(ctx context.Context, req TurnRequest) (*EventStream, error) {
	// Protocol fills AgentID, ThreadID/SessionID, MCPServers, Load, CWD on the request.
	// Registry does not own wire-session envelopes.
	agentID := req.AgentID
	threadID := req.ThreadID
	load := req.Load
	mcpServers := req.MCPServers

	if req.SessionID != "" {
		threadID = req.SessionID
		if agentID == "" {
			agentID = r.defaultAgent
		}
		if agentID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured")
		}
	} else {
		if agentID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "agent_id is required")
		}
		if threadID == "" {
			threadID = uuid.New().String()
			load = false
		}
		if len(req.Responses) > 0 {
			load = true
		}
	}

	// Steer / single-flight: cancel prior turn and wait for it to finish.
	if err := r.waitPriorTurnIfAny(ctx, threadID); err != nil {
		return nil, err
	}

	h, _, err := r.loadAgent(ctx, agentID, threadID, load, mcpServers, req.AllowMissingCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("load agent %q: %w", agentID, err)
	}

	// Parked interrupt + new user prompt (ACP session/prompt): clear park and
	// pair cancelled tools before the new turn. Resume (Responses) is unchanged.
	if req.Prompt != "" && len(req.Responses) == 0 && h.HostHasOpenToolWork(hostcontrol.Token{}) {
		h.HostFinalizeCancelledWork(ctx, hostcontrol.Token{})
	}

	turnKind := "prompt"
	if len(req.Responses) > 0 {
		turnKind = "resume"
	}
	turnCtx, cancel := context.WithCancel(ctx)
	turnCtx = telemetry.ContextWithTracer(turnCtx, r.tracer)
	turnCtx = telemetry.ContextWithInstruments(turnCtx, r.instruments)
	turnCtx, turnSpan := telemetry.StartTurnSpan(turnCtx, telemetry.TurnAttrs{
		AgentID:     agentID,
		ThreadID:    threadID,
		SessionID:   req.SessionID,
		Kind:        turnKind,
		LoadSession: load,
	})
	if turnKind == "prompt" {
		telemetry.EmitEvent(turnCtx, telemetry.EventPromptReceived,
			log.Int(telemetry.EventAttrPromptLen, len(req.Prompt)),
		)
	} else {
		telemetry.EmitEvent(turnCtx, telemetry.EventResumeReceived,
			log.Int(telemetry.EventAttrResumeInterruptCount, len(req.Responses)),
		)
	}

	th := &turnHandle{cancel: cancel, done: make(chan struct{})}
	r.activeTurns.Store(threadID, th)

	pr := &parsedRequest{
		AgentID:     agentID,
		ThreadID:    threadID,
		Prompt:      req.Prompt,
		UserMessage: req.UserMessage,
		Responses:   req.Responses,
	}

	events, err := runHarness(turnCtx, h, pr)
	if err != nil {
		r.activeTurns.Delete(threadID)
		close(th.done)
		turnSpan.End(telemetry.OutcomeError, err)
		cancel()
		closeTurnHarness(h)
		return nil, fmt.Errorf("run harness: %w", err)
	}

	// Forward events until the harness channel closes. On turn cancel, keep
	// draining so cancelled tool_results and the cancel error are not dropped
	// and activeTurns is only cleared after checkpoint (run_exit defer).
	out := make(chan streaming.StreamEvent)
	go func() {
		defer close(out)
		defer close(th.done)
		defer r.activeTurns.Delete(threadID)
		var streamErr error
		cancelled := false
		for {
			ev, ok := <-events
			if !ok {
				if cancelled {
					turnSpan.End(telemetry.OutcomeCancelled, turnCtx.Err())
				} else {
					turnSpan.End("", streamErr)
				}
				return
			}
			if turnCtx.Err() != nil {
				cancelled = true
			}
			if ev.Type == streaming.StreamEventError {
				if ev.Error != nil {
					streamErr = ev.Error
				} else if ev.Content != "" {
					streamErr = fmt.Errorf("%s", ev.Content)
				}
			}
			// Blocking forward so cancel finalize tool_results are not dropped.
			out <- ev
		}
	}()

	return &EventStream{
		Events:  out,
		harness: h,
		runCtx:  turnCtx,
		cancel:  cancel,
	}, nil
}

// buildConfigOptions returns the ACP config options for a session, with
// currentAgent as the selected agent value (falls back to defaultAgent).
func (r *Registry) buildConfigOptions(currentAgent string) []ConfigOption {
	if currentAgent == "" {
		currentAgent = r.defaultAgent
	}
	ids := make([]string, 0, len(r.agents))
	for id := range r.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	opts := make([]ConfigOptionValue, 0, len(ids))
	for _, id := range ids {
		name := r.agents[id].Name
		if name == "" {
			name = id
		}
		opts = append(opts, ConfigOptionValue{
			Value: id,
			Name:  name,
		})
	}
	return []ConfigOption{
		{
			ID:           "model",
			Name:         "Agent",
			Description:  "Select which registered agent handles this session",
			Category:     "model",
			Type:         "select",
			CurrentValue: currentAgent,
			Options:      opts,
		},
	}
}

func (r *Registry) loadAgent(ctx context.Context, agentID, threadID string, load bool, sessionMCP []mcp.MCPConfig, allowMissingCheckpoint bool) (*tacklr.AgentHarness, *AgentSpec, error) {
	spec, ok := r.agents[agentID]
	if !ok {
		return nil, nil, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
	}

	store := r.store
	if spec.Options.Store != nil {
		store = spec.Options.Store
	}

	mcpConfigs := make([]mcp.MCPConfig, 0, len(spec.Options.MCPConfigs)+len(sessionMCP))
	mcpConfigs = append(mcpConfigs, spec.Options.MCPConfigs...)
	mcpConfigs = append(mcpConfigs, sessionMCP...)

	wantVFS := spec.FSRegistry != nil && (len(spec.FSBootstrap) > 0 || (r.vfsAuth != nil && r.vfsAuth.HasBindings(threadID)))
	ms, err := r.sessionVFS(ctx, threadID, &spec)
	if err != nil {
		return nil, nil, err
	}
	if wantVFS && ms == nil && !r.vfsProjection().Available() {
		telemetry.EmitEvent(ctx, telemetry.EventFuseUnavailable,
			log.String(telemetry.AttrSessionID, threadID),
		)
		r.instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeUnavailable)
	}
	opts := spec.Options
	opts.SessionID = threadID
	opts.Store = store
	opts.MCPConfigs = mcpConfigs
	opts.MountSession = ms

	var h *tacklr.AgentHarness
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		loaded, err := tacklr.NewAgentFromSession(ctx, threadID, opts)
		switch {
		case err == nil:
			h = loaded
		case errors.Is(err, stores.ErrSessionNotFound) && allowMissingCheckpoint:
			created, err := tacklr.NewAgent(ctx, opts)
			if err != nil {
				return nil, nil, err
			}
			h = created
		default:
			return nil, nil, err
		}
	} else {
		created, err := tacklr.NewAgent(ctx, opts)
		if err != nil {
			return nil, nil, err
		}
		h = created
	}

	if err := r.ensureSessionFuse(ctx, h, threadID); err != nil {
		return nil, nil, err
	}
	return h, &spec, nil
}

func (r *Registry) ensureSessionFuse(ctx context.Context, h *tacklr.AgentHarness, threadID string) error {
	ms := h.VFS()
	if ms == nil {
		return nil
	}
	for _, spec := range ms.Specs() {
		name := strings.TrimPrefix(spec.Point, "/")
		if name == "" || strings.Contains(name, "/") {
			err := fmt.Errorf("vfs: fuse requires single-segment mount points (got %q); use /work and /engram", spec.Point)
			h.Close()
			r.closeSessionVFS(threadID)
			return err
		}
	}
	if ms.HostDir() != "" {
		return nil
	}
	proj := r.vfsProjection()
	if !proj.Available() {
		telemetry.EmitEvent(ctx, telemetry.EventFuseUnavailable,
			log.String(telemetry.AttrSessionID, threadID),
		)
		r.instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeUnavailable)
		return nil
	}
	if err := proj.Attach(ms, threadID); err != nil {
		r.instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
		telemetry.EmitEventSeverity(ctx, telemetry.EventFuseMountError, log.SeverityError,
			log.String(telemetry.AttrSessionID, threadID),
			log.String(telemetry.AttrErrorClass, "mount_failed"),
		)
		h.Close()
		r.closeSessionVFS(threadID)
		return err
	}
	r.instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	telemetry.EmitEvent(ctx, telemetry.EventFuseMount,
		log.String(telemetry.AttrSessionID, threadID),
	)
	return nil
}

// logTurnError logs non-client errors from turn setup.
func logTurnError(err error, agentID, threadID string) {
	if IsClientError(err) {
		slog.Debug("client error", "error", err, "agent_id", agentID, "thread_id", threadID)
		return
	}
	slog.Error("agent turn failed", "error", err, "agent_id", agentID, "thread_id", threadID)
}
