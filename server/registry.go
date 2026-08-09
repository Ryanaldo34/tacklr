package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// turnHandle tracks one in-flight Registry turn so a follow-on prompt (steer)
// can cancel it and wait until the harness has finished and checkpointed.
type turnHandle struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when the forwarder exits (after harness channel drain)
}

// steerWaitTimeout is how long RunTurn waits for a cancelled prior turn to finish.
const steerWaitTimeout = 30 * time.Second

type AgentSpec struct {
	Name       string
	Config     tacklr.Config
	Model      tacklr.InferenceStrategy
	Tools      []*tacklr.Tool
	MCPConfigs []mcp.MCPConfig
	SubAgents  []*tacklr.SubAgent
	WatchDog   tacklr.AgentWatchDog
	Store      stores.BaseStore
	// ExaAPIKey enables built-in web_search and web_fetch (or use process EXA_API_KEY).
	ExaAPIKey string
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
	Responses map[string]json.RawMessage
	Load      bool

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
// Cancel cancels the turn context (session/cancel). Close releases harness
// resources after the turn has ended. Callers typically:
//
//	defer func() { stream.Cancel(); stream.Close() }()
//
// Harness remains usable after an interrupt park so ResumeInterrupts can run
// before Close.
type EventStream struct {
	Events  <-chan streaming.StreamEvent
	Harness *tacklr.AgentHarness
	runCtx  context.Context
	cancel  context.CancelFunc
	closed  bool
	mu      sync.Mutex
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

// Close releases harness resources (idempotent). Call after Cancel or stream end.
func (s *EventStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.Harness != nil {
		s.Harness.Close()
		s.Harness = nil
	}
}

// ResumeInterrupts resolves pending interrupts and returns a new event stream
// from the same harness (ACP mid-turn elicitation resume).
func (s *EventStream) ResumeInterrupts(ctx context.Context, responses map[string][]byte) (<-chan streaming.StreamEvent, error) {
	if s.Harness == nil {
		return nil, fmt.Errorf("event stream: no harness for resume")
	}
	c := s.runCtx
	if c.Err() != nil {
		c = ctx
	}
	return s.Harness.ReturnFromInterrupt(c, responses)
}

// Registry serves agents over wire protocols (ACP, SSE, and others).
type Registry struct {
	agents       map[string]AgentSpec
	defaultAgent string
	store        stores.BaseStore
	tracer       trace.Tracer           // turn and child spans; default global
	instruments  *telemetry.Instruments // turn/tool metrics; default global
	activeTurns  sync.Map               // thread id → *turnHandle
	// liveHarnesses caches session-scoped harnesses so consecutive turns reuse
	// in-memory state (window, plan, pending tools) without store reload.
	liveHarnesses sync.Map // session/thread id → *tacklr.AgentHarness
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

// NewRegistry builds a registry. opts may set telemetry providers.
func NewRegistry(store stores.BaseStore, defaultAgent string, opts ...RegistryOption) *Registry {
	r := &Registry{
		agents:       make(map[string]AgentSpec),
		defaultAgent: defaultAgent,
		store:        store,
		tracer:       telemetry.Tracer(),
		instruments:  telemetry.MustInstruments(telemetry.Meter()),
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
	return r
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
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

// DropLiveHarness removes a cached harness (e.g. session/close). Next prompt reloads from store.
func (r *Registry) DropLiveHarness(sessionID string) {
	if sessionID == "" {
		return
	}
	if v, ok := r.liveHarnesses.LoadAndDelete(sessionID); ok {
		if h, ok := v.(*tacklr.AgentHarness); ok {
			h.Close()
		}
	}
}

// waitPriorTurnIfAny cancels any in-flight turn for threadID and waits for it
// to finish (harness drain + checkpoint). Used so session/prompt while busy
// steers without concurrent Run on the same session (ACP has no session/steer).
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
	timer := time.NewTimer(steerWaitTimeout)
	defer timer.Stop()
	select {
	case <-th.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for prior turn on session %q to finish after cancel", threadID)
	}
}

// RunTurn starts a prompt or resume turn and returns a stream of events.
// Setup errors (unknown session/agent, validation) are returned synchronously.
// Runtime errors are delivered as StreamEventError on the channel.
//
// If a turn is already in flight for the session, it is cancelled and allowed
// to finalize before this turn starts (mid-turn steer via session/prompt).
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

	// Prefer warm harness (in-memory window/plan already updated by prior turn).
	h, _, err := r.loadAgent(ctx, agentID, threadID, load, mcpServers, req.AllowMissingCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("load agent %q: %w", agentID, err)
	}

	// Parked interrupt + new user prompt (ACP session/prompt): clear park and
	// pair cancelled tools before the new turn. Resume (Responses) is unchanged.
	if req.Prompt != "" && len(req.Responses) == 0 && h.HasOpenToolWork() {
		h.FinalizeCancelledWork(ctx)
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
		AgentID:   agentID,
		ThreadID:  threadID,
		Prompt:    req.Prompt,
		Responses: req.Responses,
	}

	events, err := runHarness(turnCtx, h, pr)
	if err != nil {
		r.activeTurns.Delete(threadID)
		close(th.done)
		turnSpan.End(telemetry.OutcomeError, err)
		cancel()
		h.Close()
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
		Harness: h,
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

	// Warm path: reuse in-memory harness when session MCP is not being
	// re-bound (resume with a new server list needs a fresh tool catalog).
	if threadID != "" && len(sessionMCP) == 0 {
		if v, ok := r.liveHarnesses.Load(threadID); ok {
			if h, ok := v.(*tacklr.AgentHarness); ok {
				return h, &spec, nil
			}
		}
	}
	if threadID != "" && len(sessionMCP) > 0 {
		r.DropLiveHarness(threadID)
	}

	store := r.store
	if spec.Store != nil {
		store = spec.Store
	}

	mcpConfigs := make([]mcp.MCPConfig, 0, len(spec.MCPConfigs)+len(sessionMCP))
	mcpConfigs = append(mcpConfigs, spec.MCPConfigs...)
	mcpConfigs = append(mcpConfigs, sessionMCP...)

	opts := tacklr.AgentOptions{
		Config:     spec.Config,
		SessionID:  threadID,
		Model:      spec.Model,
		Store:      store,
		WatchDog:   spec.WatchDog,
		Tools:      spec.Tools,
		MCPConfigs: mcpConfigs,
		SubAgents:  spec.SubAgents,
		ExaAPIKey:  spec.ExaAPIKey,
	}

	var h *tacklr.AgentHarness
	var err error
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		h, err = tacklr.NewAgentFromSession(ctx, threadID, opts)
		if err != nil {
			// Wire BindTurn may set AllowMissingCheckpoint when the harness never
			// wrote a row (e.g. cancelled first turn). Unknown store IDs still fail.
			if allowMissingCheckpoint && errors.Is(err, stores.ErrSessionNotFound) {
				h = tacklr.NewAgent(ctx, opts)
			} else {
				return nil, nil, err
			}
		}
	} else {
		h = tacklr.NewAgent(ctx, opts)
	}

	if threadID != "" {
		r.liveHarnesses.Store(threadID, h)
	}
	return h, &spec, nil
}

// logTurnError logs non-client errors from turn setup.
func logTurnError(err error, agentID, threadID string) {
	if IsClientError(err) {
		slog.Debug("client error", "error", err, "agent_id", agentID, "thread_id", threadID)
		return
	}
	slog.Error("agent turn failed", "error", err, "agent_id", agentID, "thread_id", threadID)
}
