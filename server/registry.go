package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

type AgentSpec struct {
	Name       string
	Config     tacklr.Config
	Model      tacklr.InferenceStrategy
	Tools      []*tacklr.Tool
	MCPConfigs []mcp.MCPConfig
	SubAgents  []*tacklr.SubAgent
	WatchDog   tacklr.AgentWatchDog
	Store      stores.BaseStore
}

// sessionState holds per-session configuration provided by the client at
// session creation time (session/new) and updated via session/set_config_option.
type sessionState struct {
	mu           sync.Mutex
	cwd          string
	mcpServers   []mcp.MCPConfig
	prompted     bool // true after the first prompt turn has been initiated
	configValues map[string]string
}

// SessionView is the domain view of a session returned by session lifecycle
// methods. Transports serialize this into protocol-specific responses.
type SessionView struct {
	SessionID     string
	ConfigOptions []ConfigOption
}

// TurnRequest describes a prompt or resume turn.
//
// Session mode (ACP): set SessionID. Agent is resolved from session config.
// For session/resume, set MCPServers to replace the stored list and set CWD
// for validation against the stored working directory.
//
// Direct mode (SSE/WS): set AgentID and ThreadID. Load indicates whether to
// restore the thread from the session store.
type TurnRequest struct {
	SessionID string
	AgentID   string
	ThreadID  string
	Prompt    string
	Responses map[string]json.RawMessage
	Load      bool

	// CWD is the client-supplied working directory for session/resume.
	// If non-empty it must match the session's stored cwd.
	CWD string

	// MCPServers carries the MCP server configs re-specified by the client
	// on session/resume. When empty, the list stored at session/new is used.
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
	if s == nil {
		return nil
	}
	return s.runCtx
}

// Cancelled reports whether the turn context has been cancelled.
func (s *EventStream) Cancelled() bool {
	return s != nil && s.runCtx != nil && s.runCtx.Err() != nil
}

// Cancel cancels the turn context so producers stop. Safe to call multiple times.
// Does not release the harness; call Close after the event pump finishes.
func (s *EventStream) Cancel() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
}

// Close releases harness resources (idempotent). Prefer after Cancel / stream end.
func (s *EventStream) Close() {
	if s == nil {
		return
	}
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
	if s == nil || s.Harness == nil {
		return nil, fmt.Errorf("event stream: no harness for resume")
	}
	c := s.runCtx
	if c == nil || c.Err() != nil {
		c = ctx
	}
	return s.Harness.ReturnFromInterrupt(c, responses)
}

type Registry struct {
	agents       map[string]AgentSpec
	defaultAgent string
	store        stores.BaseStore
	// activeTurns maps session/thread id → cancel for the in-flight turn context.
	activeTurns sync.Map // string → context.CancelFunc
	sessions    sync.Map // string → *sessionState
}

func NewRegistry(store stores.BaseStore, defaultAgent string) *Registry {
	return &Registry{
		agents:       make(map[string]AgentSpec),
		defaultAgent: defaultAgent,
		store:        store,
	}
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
	r.agents[agentID] = spec
}

// CreateSession registers a new session and returns its view.
// When a store is configured, it also writes an empty checkpoint so subsequent
// prompts can load without treating "registered but never checkpointed" as a
// special case (session load approach A).
func (r *Registry) CreateSession(cwd string, mcpServers []mcp.MCPConfig) *SessionView {
	threadID := uuid.New().String()
	configValues := map[string]string{}
	if r.defaultAgent != "" {
		configValues["agent"] = r.defaultAgent
	}
	r.sessions.Store(threadID, &sessionState{
		cwd:          cwd,
		mcpServers:   mcpServers,
		configValues: configValues,
	})
	if r.store != nil {
		// Empty checkpoint (all nil inputs) never fails to build.
		cp, _ := stores.NewCheckpoint(nil, nil, nil, nil, nil, nil)
		if err := r.store.SaveSession(context.Background(), threadID, *cp); err != nil {
			slog.Warn("failed to save empty session checkpoint", "session_id", threadID, "error", err)
		}
	}
	return &SessionView{
		SessionID:     threadID,
		ConfigOptions: r.buildConfigOptions(r.defaultAgent),
	}
}

// LoadSession refreshes the per-session MCP server list from the client's
// session/load request. The cwd must match the session's stored cwd.
func (r *Registry) LoadSession(sessionID, cwd string, mcpServers []mcp.MCPConfig) (*SessionView, error) {
	state, ok := r.sessions.Load(sessionID)
	if !ok {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	sess, ok := state.(*sessionState)
	if !ok {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	sess.mu.Lock()
	if cwd != sess.cwd {
		sess.mu.Unlock()
		return nil, clientErrorf(ErrInvalidRequest, "cwd %q does not match session cwd %q", cwd, sess.cwd)
	}
	sess.mcpServers = mcpServers
	sess.mu.Unlock()
	return &SessionView{
		SessionID:     sessionID,
		ConfigOptions: r.buildConfigOptions(r.sessionAgentID(sess)),
	}, nil
}

// SetConfigOption updates a session configuration value.
func (r *Registry) SetConfigOption(sessionID, configID, value string) (*SessionView, error) {
	state, ok := r.sessions.Load(sessionID)
	if !ok {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	sess, ok := state.(*sessionState)
	if !ok {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	sess.mu.Lock()
	if sess.configValues == nil {
		sess.configValues = map[string]string{}
	}
	switch configID {
	case "model":
		if _, exists := r.agents[value]; !exists {
			sess.mu.Unlock()
			return nil, clientErrorf(ErrAgentNotFound, "agent %q not found", value)
		}
		sess.configValues["agent"] = value
	default:
		sess.mu.Unlock()
		return nil, clientErrorf(ErrInvalidRequest, "unknown configId %q", configID)
	}
	sess.mu.Unlock()
	return &SessionView{
		SessionID:     sessionID,
		ConfigOptions: r.buildConfigOptions(r.sessionAgentID(sess)),
	}, nil
}

// CloseSession removes session state and cancels any active turn.
func (r *Registry) CloseSession(sessionID string) {
	r.sessions.Delete(sessionID)
	r.CancelSession(sessionID)
}

// CancelSession cancels the in-flight turn context for the session (if any).
// Session state is preserved. The turn context is the single cancel signal for
// harness work, the registry forwarder, and runTurnStream.
func (r *Registry) CancelSession(sessionID string) {
	if c, ok := r.activeTurns.Load(sessionID); ok {
		if cancel, ok := c.(context.CancelFunc); ok {
			cancel()
		}
	}
}

// RunTurn starts a prompt or resume turn and returns a stream of events.
// Setup errors (unknown session/agent, validation) are returned synchronously.
// Runtime errors are delivered as StreamEventError on the channel.
func (r *Registry) RunTurn(ctx context.Context, req TurnRequest) (*EventStream, error) {
	var sess *sessionState
	if req.SessionID != "" {
		if state, ok := r.sessions.Load(req.SessionID); ok {
			if s, ok := state.(*sessionState); ok {
				sess = s
			}
		}
	}

	agentID := req.AgentID
	threadID := req.ThreadID
	load := req.Load

	if req.SessionID != "" {
		threadID = req.SessionID
		if agentID == "" {
			agentID = r.sessionAgentID(sess)
		}
		if agentID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured")
		}
		if sess != nil && !sess.prompted {
			sess.prompted = true
			load = false // first turn: no checkpoint yet
		} else {
			load = true // subsequent turns: restore from store
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

	// Validate cwd on session/resume: if provided it must match the stored
	// working directory set at session/new.
	if req.CWD != "" && sess != nil {
		sess.mu.Lock()
		if req.CWD != sess.cwd {
			sess.mu.Unlock()
			return nil, clientErrorf(ErrInvalidRequest, "cwd does not match session cwd")
		}
		sess.mu.Unlock()
	}
	mcpServers := req.MCPServers
	if sess != nil {
		sess.mu.Lock()
		if len(mcpServers) > 0 {
			sess.mcpServers = mcpServers
		} else {
			mcpServers = sess.mcpServers
		}
		sess.mu.Unlock()
	}

	h, _, err := r.loadAgent(ctx, agentID, threadID, load, mcpServers)
	if err != nil {
		return nil, fmt.Errorf("load agent %q: %w", agentID, err)
	}

	// turnCtx is a child of the request/connection ctx so either parent cancel
	// or session/cancel stops the turn. Stored for CancelSession.
	turnCtx, cancel := context.WithCancel(ctx)
	r.activeTurns.Store(threadID, cancel)

	pr := &parsedRequest{
		AgentID:   agentID,
		ThreadID:  threadID,
		Prompt:    req.Prompt,
		Responses: req.Responses,
	}
	events, err := runHarness(turnCtx, h, pr)
	if err != nil {
		r.activeTurns.Delete(threadID)
		cancel()
		h.Close()
		return nil, fmt.Errorf("run harness: %w", err)
	}

	// Forward harness events until the turn context is done or the harness ends.
	// Closing out unblocks runTurnStream; do not Close the harness here (interrupt park).
	out := make(chan streaming.StreamEvent)
	go func() {
		defer close(out)
		defer r.activeTurns.Delete(threadID)
		for {
			select {
			case <-turnCtx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-turnCtx.Done():
					return
				}
			}
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

func (r *Registry) sessionAgentID(sess *sessionState) string {
	if sess != nil {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.configValues != nil {
			if id := sess.configValues["agent"]; id != "" {
				return id
			}
		}
	}
	return r.defaultAgent
}

func (r *Registry) loadAgent(ctx context.Context, agentID, threadID string, load bool, sessionMCP []mcp.MCPConfig) (*tacklr.AgentHarness, *AgentSpec, error) {
	spec, ok := r.agents[agentID]
	if !ok {
		return nil, nil, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
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
		Model:      spec.Model,
		Store:      store,
		WatchDog:   spec.WatchDog,
		Tools:      spec.Tools,
		MCPConfigs: mcpConfigs,
		SubAgents:  spec.SubAgents,
	}

	var h *tacklr.AgentHarness
	var err error
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		h, err = tacklr.NewAgentFromSession(ctx, threadID, opts)
		if err != nil {
			// ACP session may still be registered after a cancelled first turn that
			// never checkpointed. Only then start a fresh harness for re-prompt.
			// Unknown thread IDs (SSE resume without a store row) still fail.
			_, sessionKnown := r.sessions.Load(threadID)
			if sessionKnown && errors.Is(err, stores.ErrSessionNotFound) {
				h = tacklr.NewAgent(ctx, opts)
			} else {
				return nil, nil, err
			}
		}
	} else {
		h = tacklr.NewAgent(ctx, opts)
	}

	h.SessionId = threadID
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
