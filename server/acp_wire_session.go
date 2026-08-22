package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

// acpWireSession is live ACP wire state for one session id (not harness state).
type acpWireSession struct {
	mu           sync.Mutex
	cwd          string
	mcpServers   []mcp.MCPConfig
	configValues map[string]string
	owner        string
	prompted     bool // true after the first prompt turn was bound
	vfs          []vfs.Binding
	vfsDrop      []string
}

// acpWireEnvelope is the durable JSON blob in ProtocolWireStore.
type acpWireEnvelope struct {
	CWD          string            `json:"cwd"`
	ConfigValues map[string]string `json:"configValues"`
	MCPServers   []mcp.MCPConfig   `json:"mcpServers"`
	Owner        string            `json:"owner,omitempty"`
	Prompted     bool              `json:"prompted"`
}

func (s *acpWireSession) envelope() acpWireEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy map/slice so concurrent setConfig/BindTurn cannot race json.Marshal
	// after this lock is released.
	cfg := make(map[string]string, len(s.configValues))
	for k, v := range s.configValues {
		cfg[k] = v
	}
	return acpWireEnvelope{
		CWD:          s.cwd,
		ConfigValues: cfg,
		MCPServers:   mcp.DurableConfigs(s.mcpServers),
		Owner:        s.owner,
		Prompted:     s.prompted,
	}
}

func (s *acpWireSession) takeAuth() durable.AuthContext {
	if s == nil {
		return durable.AuthContext{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := durable.AuthContext{
		Bindings: append([]vfs.Binding(nil), s.vfs...),
		Drop:     append([]string(nil), s.vfsDrop...),
	}
	s.vfsDrop = nil
	return out
}

func (s *acpWireSession) stashBind(b vfs.Binding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alias := strings.TrimSpace(b.Params[vfs.ParamName])
	if alias == "" {
		alias = strings.TrimPrefix(strings.TrimSpace(b.Point), "/")
	}
	for i, existing := range s.vfs {
		ex := strings.TrimSpace(existing.Params[vfs.ParamName])
		if ex == "" {
			ex = strings.TrimPrefix(strings.TrimSpace(existing.Point), "/")
		}
		if ex == alias && existing.Provider == b.Provider {
			s.vfs[i] = b
			return
		}
	}
	s.vfs = append(s.vfs, b)
}

func (s *acpWireSession) stashRefresh(provider string, c vfs.Credential) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for i, existing := range s.vfs {
		if existing.Provider != provider {
			continue
		}
		s.vfs[i].Auth = c
		found = true
	}
	return found
}

func (s *acpWireSession) stashUnbind(point, provider string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.vfs[:0]
	for _, existing := range s.vfs {
		alias := strings.TrimSpace(existing.Params[vfs.ParamName])
		if alias == "" {
			alias = strings.TrimPrefix(strings.TrimSpace(existing.Point), "/")
		}
		drop := false
		if point != "" && (alias == point || existing.Point == point) {
			drop = true
		}
		if provider != "" && existing.Provider == provider {
			drop = true
		}
		if drop {
			if alias != "" {
				s.vfsDrop = append(s.vfsDrop, alias)
			} else {
				s.vfsDrop = append(s.vfsDrop, existing.Provider)
			}
			continue
		}
		kept = append(kept, existing)
	}
	s.vfs = kept
}

func wireSessionFromEnvelope(env acpWireEnvelope) *acpWireSession {
	cfg := make(map[string]string, len(env.ConfigValues))
	for k, v := range env.ConfigValues {
		cfg[k] = v
	}
	return &acpWireSession{
		cwd:          env.CWD,
		mcpServers:   append([]mcp.MCPConfig(nil), env.MCPServers...),
		configValues: cfg,
		owner:        env.Owner,
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
	// Another goroutine may have loaded the same id while we read the store.
	if existing, ok := p.sessions[sessionID]; ok {
		p.mu.Unlock()
		return existing, nil
	}
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	return sess, nil
}

// CreateSession implements Protocol wire session create for ACP.
func (p *acpProtocol) CreateSession(ctx context.Context, env ProtocolEnv, params json.RawMessage) (string, any, error) {
	if err := authorizeOperation(ctx, env, actionSessionCreate, ""); err != nil {
		return "", nil, err
	}
	var pr acpSessionParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &pr); err != nil {
			return "", nil, clientErrorf(ErrInvalidRequest, "invalid session/new params: %v", err)
		}
	}
	defaultAgent := catalogDefault(env.Catalog)
	cfg := map[string]string{}
	if defaultAgent != "" {
		cfg["agent"] = defaultAgent
	}
	sessionID := uuid.NewString()
	sess := &acpWireSession{
		cwd:          pr.Cwd,
		mcpServers:   pr.MCPServers,
		configValues: cfg,
		owner:        securitySubject(env),
	}
	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = make(map[string]*acpWireSession)
	}
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	recordSessionCreated(ctx)
	if env.Runtime != nil {
		if _, err := env.Runtime.CreateSession(ctx, durable.CreateSession{
			AgentID:    defaultAgent,
			SessionID:  durable.SessionID(sessionID),
			MCPServers: pr.MCPServers,
		}); err != nil {
			return "", nil, err
		}
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return "", nil, err
	}
	opts := catalogConfigOptions(env.Catalog, defaultAgent)
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
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionLoad)
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
	if agent == "" {
		agent = catalogDefault(env.Catalog)
	}
	if env.Runtime != nil {
		_, err := env.Runtime.CreateSession(ctx, durable.CreateSession{
			AgentID:    agent,
			SessionID:  durable.SessionID(sessionID),
			MCPServers: sess.mcpServers,
		})
		if err != nil && !errors.Is(err, durable.ErrSessionExists) {
			return nil, err
		}
	}
	opts := catalogConfigOptions(env.Catalog, agent)
	return map[string]any{
		"sessionId":     sessionID,
		"configOptions": opts,
	}, nil
}

// BindTurn implements Protocol: maps ACP session/prompt or session/resume params
// into a Registry TurnRequest. This is the only turn-binding path for ACP.
func (p *acpProtocol) BindTurn(ctx context.Context, env ProtocolEnv, sessionID, method string, turnParams json.RawMessage) (TurnRequest, error) {
	var prompt string
	var userMsg *tacklr.Message
	var responses map[string]json.RawMessage
	var cwd string
	var mcpFromClient []mcp.MCPConfig

	if len(turnParams) > 0 {
		switch method {
		case "session/resume":
			var sp acpSessionParams
			if err := json.Unmarshal(turnParams, &sp); err == nil {
				if sp.SessionID != "" {
					sessionID = sp.SessionID
				}
				cwd = sp.Cwd
				mcpFromClient = sp.MCPServers
			}
			var withResp struct {
				Responses map[string]json.RawMessage `json:"responses"`
			}
			if err := json.Unmarshal(turnParams, &withResp); err == nil {
				responses = withResp.Responses
			}
		default:
			var pp acpPromptParams
			if err := json.Unmarshal(turnParams, &pp); err != nil {
				return TurnRequest{}, clientErrorf(ErrInvalidRequest, "invalid prompt params: %v", err)
			}
			if pp.SessionID != "" {
				sessionID = pp.SessionID
			}
			if len(pp.Prompt) > 0 {
				msg, err := parseACPPrompt(pp.Prompt)
				if err != nil {
					return TurnRequest{}, clientErrorf(ErrInvalidRequest, "invalid prompt content: %v", err)
				}
				userMsg = msg
				prompt = msg.Content
			}
			var sp acpSessionParams
			if err := json.Unmarshal(turnParams, &sp); err == nil {
				if sp.SessionID != "" && sessionID == "" {
					sessionID = sp.SessionID
				}
				cwd = sp.Cwd
				mcpFromClient = sp.MCPServers
			}
		}
	}

	if sessionID == "" {
		return TurnRequest{}, clientErrorf(ErrInvalidRequest, "sessionId is required")
	}
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionPrompt)
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

	if agentID == "" {
		agentID = catalogDefault(env.Catalog)
	}
	if agentID == "" {
		return TurnRequest{}, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured")
	}
	// Reject binary content the agent model cannot accept before the turn starts.
	if userMsg != nil {
		if mimes := userMsg.MIMETypes(); len(mimes) > 0 {
			if model := catalogAgentModel(env.Catalog, agentID); model != nil {
				if bad := tacklr.UnsupportedMIMEs(model, mimes); len(bad) > 0 {
					return TurnRequest{}, clientErrorf(ErrInvalidRequest, "unsupported content type(s): %s", strings.Join(bad, ", "))
				}
			} else {
				// No model: reject all non-text.
				var bad []string
				for _, m := range mimes {
					if !streaming.IsTextMIME(m) {
						bad = append(bad, streaming.NormalizeMIME(m))
					}
				}
				if len(bad) > 0 {
					return TurnRequest{}, clientErrorf(ErrInvalidRequest, "unsupported content type(s): %s", strings.Join(bad, ", "))
				}
			}
		}
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return TurnRequest{}, err
	}

	return TurnRequest{
		SessionID:              sessionID,
		AgentID:                agentID,
		ThreadID:               sessionID,
		Prompt:                 prompt,
		UserMessage:            userMsg,
		Responses:              responses,
		Load:                   load,
		AllowMissingCheckpoint: true, // wire session may outlive harness rows
		CWD:                    sessCWD,
		MCPServers:             mcpServers,
		Auth:                   sess.takeAuth(),
	}, nil
}

// CloseSession implements Protocol for ACP.
func (p *acpProtocol) CloseSession(ctx context.Context, env ProtocolEnv, sessionID string) error {
	if _, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionClose); err != nil {
		return err
	}
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
	if env.Runtime != nil {
		_ = env.Runtime.Close(ctx, durable.SessionID(sessionID))
	}
	return nil
}

func (p *acpProtocol) setConfig(ctx context.Context, env ProtocolEnv, sessionID, configID, value string) (any, error) {
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionConfig)
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	if sess.configValues == nil {
		sess.configValues = map[string]string{}
	}
	switch configID {
	case "model":
		if !catalogHasAgent(env.Catalog, value) {
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
		"configOptions": catalogConfigOptions(env.Catalog, agent),
	}, nil
}
