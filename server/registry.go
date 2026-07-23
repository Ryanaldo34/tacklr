package server

import (
	"context"
	"encoding/json"
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
	Name              string
	Config            tacklr.Config
	Model             tacklr.InferenceStrategy
	Tools             []*tacklr.Tool
	MCPConfigs        []mcp.MCPConfig
	WatchDog          tacklr.AgentWatchDog
	StreamingStrategy tacklr.StreamingStrategy
	Store             stores.BaseStore
}

// sessionState holds per-session configuration provided by the client at
// session creation time (session/new) and updated via session/set_config_option.
type sessionState struct {
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
// Resume forces a store load; otherwise load follows whether the session has
// already completed a turn.
//
// Direct mode (SSE/WS): set AgentID and ThreadID. Load indicates whether to
// restore the thread from the session store.
type TurnRequest struct {
	SessionID string
	AgentID   string
	ThreadID  string
	Prompt    string
	Responses map[string]json.RawMessage
	Resume    bool
	Load      bool
}

// EventStream is a running agent turn. Events is closed when the turn ends.
// Cancel aborts the turn; it is safe to call multiple times.
type EventStream struct {
	Events <-chan streaming.StreamEvent
	cancel context.CancelFunc
}

// Cancel aborts the running turn.
func (s *EventStream) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

type Registry struct {
	agents       map[string]AgentSpec
	defaultAgent string
	store        stores.BaseStore
	activeCtx    sync.Map // threadID → context.CancelFunc
	sessions     sync.Map // threadID → *sessionState
	cancelled    sync.Map // threadID → struct{}
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

// Capabilities returns the ACP initialize advertisement.
func (r *Registry) Capabilities() map[string]any {
	return map[string]any{
		"protocolVersion": 1,
		"agentCapabilities": map[string]any{
			// Full load requires conversation replay; advertise false until implemented.
			"loadSession": false,
			"promptCapabilities": map[string]any{
				"image":           false,
				"audio":           false,
				"embeddedContext": true,
			},
			"mcpCapabilities": map[string]any{
				"http": false,
				"sse":  false,
			},
			"sessionCapabilities": map[string]any{
				"close": struct{}{},
			},
		},
		"agentInfo": map[string]string{
			"name":    "tacklr",
			"title":   "Tacklr ACP",
			"version": "0.1.0",
		},
		"authMethods": []string{},
	}
}

// CreateSession registers a new session and returns its view.
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
	return &SessionView{
		SessionID:     threadID,
		ConfigOptions: r.buildConfigOptions(r.defaultAgent),
	}
}

// GetSession returns metadata for an existing session.
func (r *Registry) GetSession(sessionID string) (*SessionView, error) {
	state, ok := r.sessions.Load(sessionID)
	if !ok {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	sess := state.(*sessionState)
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
	sess := state.(*sessionState)
	if sess.configValues == nil {
		sess.configValues = map[string]string{}
	}
	switch configID {
	case "model":
		if _, exists := r.agents[value]; !exists {
			return nil, clientErrorf(ErrAgentNotFound, "agent %q not found", value)
		}
		sess.configValues["agent"] = value
	default:
		return nil, clientErrorf(ErrInvalidRequest, "unknown configId %q", configID)
	}
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

// CancelSession aborts any active turn for the session without removing it.
func (r *Registry) CancelSession(sessionID string) {
	r.cancelled.Store(sessionID, struct{}{})
	if c, ok := r.activeCtx.LoadAndDelete(sessionID); ok {
		c.(context.CancelFunc)()
	}
}

// WasCancelled reports whether CancelSession was called for this session.
func (r *Registry) WasCancelled(sessionID string) bool {
	_, ok := r.cancelled.Load(sessionID)
	return ok
}

// RunTurn starts a prompt or resume turn and returns a stream of events.
// Setup errors (unknown session/agent, validation) are returned synchronously.
// Runtime errors are delivered as StreamEventError on the channel.
func (r *Registry) RunTurn(ctx context.Context, req TurnRequest) (*EventStream, error) {
	var sess *sessionState
	if req.SessionID != "" {
		if state, ok := r.sessions.Load(req.SessionID); ok {
			sess = state.(*sessionState)
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
		// loadSession is not yet implemented — conversation history is not
		// persisted to the store, so loading would always fail. Always start
		// with a fresh harness. Session configuration (agent, MCP servers) is
		// preserved in r.sessions.
		load = false
		if sess != nil && !sess.prompted {
			sess.prompted = true
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

	h, _, err := r.loadAgent(ctx, agentID, threadID, load)
	if err != nil {
		return nil, fmt.Errorf("load agent %q: %w", agentID, err)
	}

	// Merge per-session MCP configs from session/new into the harness.
	if sess != nil && len(sess.mcpServers) > 0 {
		h.MCPConfigs = append(h.MCPConfigs, sess.mcpServers...)
	}

	runCtx, cancel := context.WithCancel(ctx)
	r.activeCtx.Store(threadID, cancel)

	pr := &parsedRequest{
		AgentID:   agentID,
		ThreadID:  threadID,
		Prompt:    req.Prompt,
		Responses: req.Responses,
	}
	events, err := runHarness(runCtx, h, pr)
	if err != nil {
		r.activeCtx.Delete(threadID)
		cancel()
		h.Close()
		return nil, fmt.Errorf("run harness: %w", err)
	}

	out := make(chan streaming.StreamEvent)
	go func() {
		defer close(out)
		defer func() {
			h.Close()
			r.activeCtx.Delete(threadID)
			r.cancelled.Delete(threadID)
			cancel()
		}()
		for {
			select {
			case <-runCtx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()

	return &EventStream{Events: out, cancel: cancel}, nil
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
	if sess != nil && sess.configValues != nil {
		if id := sess.configValues["agent"]; id != "" {
			return id
		}
	}
	return r.defaultAgent
}

func (r *Registry) loadAgent(ctx context.Context, agentID, threadID string, load bool) (*tacklr.AgentHarness, *AgentSpec, error) {
	spec, ok := r.agents[agentID]
	if !ok {
		return nil, nil, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
	}

	store := r.store
	if spec.Store != nil {
		store = spec.Store
	}

	var h *tacklr.AgentHarness
	var err error
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		h, err = tacklr.NewAgentHarnessFromSession(ctx, threadID, spec.Config, spec.Model, store, spec.WatchDog)
		if err != nil {
			return nil, nil, err
		}
	} else {
		h = tacklr.NewAgent(spec.Config, spec.Model, store, spec.WatchDog)
	}

	h.SessionId = threadID
	h.Tools = spec.Tools
	h.MCPConfigs = spec.MCPConfigs
	if spec.StreamingStrategy != nil {
		h.WithStreamingStrategy(spec.StreamingStrategy)
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
