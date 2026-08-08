package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
)

// acpWireSession is live ACP wire state for one session id (not harness state).
type acpWireSession struct {
	mu           sync.Mutex
	cwd          string
	mcpServers   []mcp.MCPConfig
	configValues map[string]string
	prompted     bool // true after the first prompt turn was bound
}

// acpWireEnvelope is the durable JSON blob in ProtocolWireStore.
type acpWireEnvelope struct {
	CWD          string            `json:"cwd"`
	ConfigValues map[string]string `json:"configValues"`
	MCPServers   []mcp.MCPConfig   `json:"mcpServers"`
	Prompted     bool              `json:"prompted"`
}

func (s *acpWireSession) envelope() acpWireEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configValues
	if cfg == nil {
		cfg = map[string]string{}
	}
	return acpWireEnvelope{
		CWD:          s.cwd,
		ConfigValues: cfg,
		MCPServers:   append([]mcp.MCPConfig(nil), s.mcpServers...),
		Prompted:     s.prompted,
	}
}

func wireSessionFromEnvelope(env acpWireEnvelope) *acpWireSession {
	cfg := env.ConfigValues
	if cfg == nil {
		cfg = map[string]string{}
	}
	return &acpWireSession{
		cwd:          env.CWD,
		mcpServers:   env.MCPServers,
		configValues: cfg,
		prompted:     env.Prompted,
	}
}

func (p *acpProtocol) persistWire(ctx context.Context, sessionID string, sess *acpWireSession) error {
	if p == nil || p.wire == nil || sess == nil {
		return nil
	}
	raw, err := json.Marshal(sess.envelope())
	if err != nil {
		return err
	}
	return p.wire.Put(ctx, sessionID, raw)
}

func (p *acpProtocol) resolveWireSession(ctx context.Context, sessionID string) (*acpWireSession, error) {
	if p == nil {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	p.mu.Lock()
	if sess, ok := p.sessions[sessionID]; ok {
		p.mu.Unlock()
		return sess, nil
	}
	p.mu.Unlock()

	if p.wire == nil {
		return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
	}
	raw, err := p.wire.Get(ctx, sessionID)
	if err != nil {
		if errors.Is(err, stores.ErrSessionNotFound) {
			return nil, clientErrorf(ErrSessionNotFound, "session %q not found", sessionID)
		}
		return nil, fmt.Errorf("load wire session %q: %w", sessionID, err)
	}
	var env acpWireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode wire session %q: %w", sessionID, err)
	}
	sess := wireSessionFromEnvelope(env)
	p.mu.Lock()
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	return sess, nil
}

// CreateSession implements Protocol wire session create for ACP.
func (p *acpProtocol) CreateSession(ctx context.Context, env ProtocolEnv, params json.RawMessage) (string, any, error) {
	var pr acpSessionParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &pr); err != nil {
			return "", nil, clientErrorf(ErrInvalidRequest, "invalid session/new params: %v", err)
		}
	}
	defaultAgent := ""
	if env.Registry != nil {
		defaultAgent = env.Registry.DefaultAgent()
	}
	cfg := map[string]string{}
	if defaultAgent != "" {
		cfg["agent"] = defaultAgent
	}
	sessionID := uuid.NewString()
	sess := &acpWireSession{
		cwd:          pr.Cwd,
		mcpServers:   pr.MCPServers,
		configValues: cfg,
	}
	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = make(map[string]*acpWireSession)
	}
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	if env.Registry != nil {
		env.Registry.RecordSessionCreated(ctx)
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return "", nil, err
	}
	opts := []ConfigOption{}
	if env.Registry != nil {
		opts = env.Registry.ConfigOptions(defaultAgent)
	}
	return sessionID, map[string]any{
		"sessionId":     sessionID,
		"configOptions": opts,
	}, nil
}

// LoadSession implements Protocol wire session load for ACP.
func (p *acpProtocol) LoadSession(ctx context.Context, env ProtocolEnv, sessionID string, params json.RawMessage) (any, error) {
	if sessionID == "" {
		return nil, clientErrorf(ErrInvalidRequest, "sessionId is required")
	}
	var pr acpSessionParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &pr); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/load params: %v", err)
		}
		if pr.SessionID != "" {
			sessionID = pr.SessionID
		}
	}
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	if pr.Cwd != "" && sess.cwd != "" && pr.Cwd != sess.cwd {
		sess.mu.Unlock()
		return nil, clientErrorf(ErrInvalidRequest, "cwd %q does not match session cwd %q", pr.Cwd, sess.cwd)
	}
	if pr.Cwd != "" && sess.cwd == "" {
		sess.cwd = pr.Cwd
	}
	if len(pr.MCPServers) > 0 {
		sess.mcpServers = pr.MCPServers
	}
	agent := ""
	if sess.configValues != nil {
		agent = sess.configValues["agent"]
	}
	sess.mu.Unlock()
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	if agent == "" && env.Registry != nil {
		agent = env.Registry.DefaultAgent()
	}
	opts := []ConfigOption{}
	if env.Registry != nil {
		opts = env.Registry.ConfigOptions(agent)
	}
	return map[string]any{
		"sessionId":     sessionID,
		"configOptions": opts,
	}, nil
}

// BindTurn implements Protocol: maps ACP session/prompt or session/resume params
// into a Registry TurnRequest. This is the only turn-binding path for ACP.
func (p *acpProtocol) BindTurn(ctx context.Context, env ProtocolEnv, sessionID string, turnParams json.RawMessage) (TurnRequest, error) {
	var prompt string
	var responses map[string]json.RawMessage
	var cwd string
	var mcpFromClient []mcp.MCPConfig

	if len(turnParams) > 0 {
		var pp acpPromptParams
		if err := json.Unmarshal(turnParams, &pp); err == nil {
			if pp.SessionID != "" {
				sessionID = pp.SessionID
			}
			if len(pp.Prompt) > 0 {
				text, err := concatenateACPPrompt(pp.Prompt)
				if err != nil {
					return TurnRequest{}, clientErrorf(ErrInvalidRequest, "invalid prompt content: %v", err)
				}
				prompt = text
			}
		}
		var sp acpSessionParams
		if err := json.Unmarshal(turnParams, &sp); err == nil {
			if sp.SessionID != "" && sessionID == "" {
				sessionID = sp.SessionID
			}
			cwd = sp.Cwd
			mcpFromClient = sp.MCPServers
		}
		var withResp struct {
			Responses map[string]json.RawMessage `json:"responses"`
		}
		_ = json.Unmarshal(turnParams, &withResp)
		responses = withResp.Responses
	}

	if sessionID == "" {
		return TurnRequest{}, clientErrorf(ErrInvalidRequest, "sessionId is required")
	}
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil {
		return TurnRequest{}, err
	}

	sess.mu.Lock()
	if cwd != "" && sess.cwd != "" && cwd != sess.cwd {
		sess.mu.Unlock()
		return TurnRequest{}, clientErrorf(ErrInvalidRequest, "cwd does not match session cwd")
	}
	mcpServers := mcpFromClient
	if len(mcpServers) > 0 {
		sess.mcpServers = mcpServers
	} else {
		mcpServers = sess.mcpServers
	}
	load := sess.prompted
	if !sess.prompted {
		sess.prompted = true
	}
	agentID := ""
	if sess.configValues != nil {
		agentID = sess.configValues["agent"]
	}
	sessCWD := sess.cwd
	sess.mu.Unlock()

	if agentID == "" && env.Registry != nil {
		agentID = env.Registry.DefaultAgent()
	}
	if agentID == "" {
		return TurnRequest{}, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured")
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return TurnRequest{}, err
	}

	return TurnRequest{
		SessionID:              sessionID,
		AgentID:                agentID,
		ThreadID:               sessionID,
		Prompt:                 prompt,
		Responses:              responses,
		Load:                   load,
		AllowMissingCheckpoint: true, // wire session may outlive harness rows
		CWD:                    sessCWD,
		MCPServers:             mcpServers,
	}, nil
}

// CloseSession implements Protocol for ACP.
func (p *acpProtocol) CloseSession(ctx context.Context, env ProtocolEnv, sessionID string) error {
	if p != nil {
		p.mu.Lock()
		delete(p.sessions, sessionID)
		p.mu.Unlock()
		if p.wire != nil {
			if err := p.wire.Delete(ctx, sessionID); err != nil {
				return err
			}
		}
	}
	if env.Registry != nil {
		env.Registry.CancelSession(sessionID)
	}
	return nil
}

func (p *acpProtocol) setConfig(ctx context.Context, env ProtocolEnv, sessionID, configID, value string) (any, error) {
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	if sess.configValues == nil {
		sess.configValues = map[string]string{}
	}
	switch configID {
	case "model":
		if env.Registry == nil || !env.Registry.HasAgent(value) {
			sess.mu.Unlock()
			return nil, clientErrorf(ErrAgentNotFound, "agent %q not found", value)
		}
		sess.configValues["agent"] = value
	default:
		sess.mu.Unlock()
		return nil, clientErrorf(ErrInvalidRequest, "unknown configId %q", configID)
	}
	agent := sess.configValues["agent"]
	sess.mu.Unlock()
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	return map[string]any{
		"configOptions": env.Registry.ConfigOptions(agent),
	}, nil
}
