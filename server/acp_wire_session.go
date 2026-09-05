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
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// acpWireSession is live ACP wire state for one session id (not harness state).
type acpWireSession struct {
	mu           sync.Mutex
	cwd          string
	mcpServers   []mcp.MCPConfig
	configValues map[string]string
	owner        string
	vfs          []vfs.Binding
	vfsDrop      []string
}

// acpWireEnvelope is the durable JSON blob in ProtocolWireStore.
type acpWireEnvelope struct {
	CWD          string            `json:"cwd"`
	ConfigValues map[string]string `json:"configValues"`
	MCPServers   []mcp.MCPConfig   `json:"mcpServers"`
	Owner        string            `json:"owner,omitempty"`
}

func (s *acpWireSession) envelope() acpWireEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy map/slice so concurrent setConfig/bindTurn cannot race json.Marshal
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
	}
}

func (s *acpWireSession) cwdMismatch(cwd string) error {
	if cwd != "" && s.cwd != "" && cwd != s.cwd {
		return clientErrorf(ErrInvalidRequest, "cwd does not match session cwd")
	}
	return nil
}

func (s *acpWireSession) takeAuth() durable.AuthContext {
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
			s.vfsDrop = append(s.vfsDrop, alias)
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
	}
}

func (p *acpProtocol) persistWire(ctx context.Context, sessionID string, sess *acpWireSession) error {
	raw, _ := json.Marshal(sess.envelope())
	return p.wire.Put(ctx, sessionID, raw)
}

func (p *acpProtocol) resolveWireSession(ctx context.Context, sessionID string) (*acpWireSession, error) {
	p.mu.Lock()
	if sess, ok := p.sessions[sessionID]; ok {
		p.mu.Unlock()
		return sess, nil
	}
	p.mu.Unlock()

	raw, err := p.wire.Get(ctx, sessionID)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
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

func (p *acpProtocol) createSession(ctx context.Context, env ProtocolEnv, pr *parsedRequest) (string, any, error) {
	if err := authorizeOperation(ctx, env, actionSessionCreate, ""); err != nil {
		return "", nil, err
	}
	defaultAgent := ""
	if env.Catalog != nil {
		defaultAgent = env.Catalog.DefaultID()
	}
	cfg := map[string]string{}
	if defaultAgent != "" {
		cfg["agent"] = defaultAgent
	}
	sessionID := uuid.NewString()
	sess := &acpWireSession{
		cwd:          pr.CWD,
		mcpServers:   pr.MCPServers,
		configValues: cfg,
		owner:        securitySubject(env),
	}
	p.mu.Lock()
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	telemetry.MustInstruments(telemetry.Meter()).RecordSessionCreated(ctx)
	if _, err := env.Runtime.CreateSession(ctx, durable.CreateSession{
		AgentID:    defaultAgent,
		SessionID:  durable.SessionID(sessionID),
		MCPServers: pr.MCPServers,
	}); err != nil {
		return "", nil, err
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

func (p *acpProtocol) loadSession(ctx context.Context, env ProtocolEnv, pr *parsedRequest) (any, error) {
	sessionID := pr.ThreadID
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionLoad)
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	if err := sess.cwdMismatch(pr.CWD); err != nil {
		sess.mu.Unlock()
		return nil, err
	}
	if pr.CWD != "" && sess.cwd == "" {
		sess.cwd = pr.CWD
	}
	if len(pr.MCPServers) > 0 {
		sess.mcpServers = pr.MCPServers
	}
	agent := sess.configValues["agent"]
	sess.mu.Unlock()
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	if agent == "" && env.Catalog != nil {
		agent = env.Catalog.DefaultID()
	}
	_, err = env.Runtime.CreateSession(ctx, durable.CreateSession{
		AgentID:    agent,
		SessionID:  durable.SessionID(sessionID),
		MCPServers: sess.mcpServers,
	})
	if err != nil && !errors.Is(err, durable.ErrSessionExists) {
		return nil, err
	}
	opts := catalogConfigOptions(env.Catalog, agent)
	return map[string]any{
		"sessionId":     sessionID,
		"configOptions": opts,
	}, nil
}

// turnRequest is bindTurn output: one prompt or resume against Runtime.
type turnRequest struct {
	AgentID     string
	ThreadID    string
	Prompt      string
	UserMessage *tacklr.Message
	Responses   map[string]json.RawMessage
	MCPServers  []mcp.MCPConfig
	Auth        durable.AuthContext
}

func (p *acpProtocol) bindTurn(ctx context.Context, env ProtocolEnv, pr *parsedRequest) (turnRequest, error) {
	sessionID := pr.ThreadID
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionPrompt)
	if err != nil {
		return turnRequest{}, err
	}

	sess.mu.Lock()
	if err := sess.cwdMismatch(pr.CWD); err != nil {
		sess.mu.Unlock()
		return turnRequest{}, err
	}
	mcpServers := pr.MCPServers
	if len(mcpServers) > 0 {
		sess.mcpServers = mcpServers
	} else {
		mcpServers = sess.mcpServers
	}
	agentID := sess.configValues["agent"]
	sess.mu.Unlock()

	if agentID == "" && env.Catalog != nil {
		agentID = env.Catalog.DefaultID()
	}
	if agentID == "" {
		return turnRequest{}, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured")
	}
	// Reject binary content the agent model cannot accept before the turn starts.
	if pr.UserMessage != nil {
		if mimes := pr.UserMessage.MIMETypes(); len(mimes) > 0 {
			var model tacklr.InferenceStrategy
			if env.Catalog != nil {
				if spec, ok := env.Catalog.Lookup(agentID); ok {
					model = spec.Options.Model
				}
			}
			if model != nil {
				if bad := tacklr.UnsupportedMIMEs(model, mimes); len(bad) > 0 {
					return turnRequest{}, clientErrorf(ErrInvalidRequest, "unsupported content type(s): %s", strings.Join(bad, ", "))
				}
			}
		}
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return turnRequest{}, err
	}

	return turnRequest{
		AgentID:     agentID,
		ThreadID:    sessionID,
		Prompt:      pr.Prompt,
		UserMessage: pr.UserMessage,
		Responses:   pr.Responses,
		MCPServers:  mcpServers,
		Auth:        sess.takeAuth(),
	}, nil
}

func (p *acpProtocol) closeSession(ctx context.Context, env ProtocolEnv, sessionID string) error {
	if _, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionClose); err != nil {
		return err
	}
	p.mu.Lock()
	delete(p.sessions, sessionID)
	p.mu.Unlock()
	if err := p.wire.Delete(ctx, sessionID); err != nil {
		return err
	}
	_ = env.Runtime.Close(ctx, durable.SessionID(sessionID))
	return nil
}

func (p *acpProtocol) setConfig(ctx context.Context, env ProtocolEnv, sessionID, configID, value string) (any, error) {
	sess, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionConfig)
	if err != nil {
		return nil, err
	}
	sess.mu.Lock()
	if configID != "model" {
		sess.mu.Unlock()
		return nil, clientErrorf(ErrInvalidRequest, "unknown configId %q", configID)
	}
	ok := env.Catalog != nil
	if ok {
		_, ok = env.Catalog.Lookup(value)
	}
	if !ok {
		sess.mu.Unlock()
		return nil, clientErrorf(ErrAgentNotFound, "agent %q not found", value)
	}
	sess.configValues["agent"] = value
	agent := sess.configValues["agent"]
	sess.mu.Unlock()
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	return map[string]any{
		"configOptions": catalogConfigOptions(env.Catalog, agent),
	}, nil
}
