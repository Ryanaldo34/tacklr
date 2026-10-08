package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/server"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// acpWireSession is live ACP wire state for one session id (not harness state).
type acpWireSession struct {
	mu           sync.Mutex
	cwd          string
	mcpServers   []mcp.MCPConfig
	configValues map[string]string
	owner        string
	creds        session.CredentialBag
}

// acpWireEnvelope is the durable JSON blob in server.ProtocolWireStore.
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
		return server.Errorf(server.ErrInvalidRequest, "cwd does not match session cwd")
	}
	return nil
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
		if errors.Is(err, server.ErrSessionNotFound) {
			return nil, server.Errorf(server.ErrSessionNotFound, "session %q not found", sessionID)
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

func (p *acpProtocol) createSession(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (string, any, error) {
	if err := authorizeOperation(ctx, env, actionSessionCreate, ""); err != nil {
		return "", nil, err
	}
	sessionID := uuid.NewString()
	sess := &acpWireSession{
		cwd:          pr.CWD,
		mcpServers:   pr.MCPServers,
		configValues: map[string]string{},
		owner:        securitySubject(env),
	}
	p.mu.Lock()
	p.sessions[sessionID] = sess
	p.mu.Unlock()
	telemetry.MustInstruments(telemetry.Meter()).RecordSessionCreated(ctx)
	if _, err := env.Runtime.CreateSession(ctx, session.CreateSession{
		SessionID:  session.SessionID(sessionID),
		MCPServers: pr.MCPServers,
	}); err != nil {
		return "", nil, err
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return "", nil, err
	}
	return sessionID, map[string]any{
		"sessionId": sessionID,
	}, nil
}

func (p *acpProtocol) loadSession(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (any, error) {
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
	sess.mu.Unlock()
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	_, err = env.Runtime.CreateSession(ctx, session.CreateSession{
		SessionID:  session.SessionID(sessionID),
		MCPServers: sess.mcpServers,
	})
	if err != nil && !errors.Is(err, session.ErrSessionExists) {
		return nil, err
	}
	return map[string]any{
		"sessionId": sessionID,
	}, nil
}

// turnRequest is bindTurn output: one prompt or resume against Runtime.
type turnRequest struct {
	ThreadID    string
	Prompt      string
	UserMessage *tacklr.Message
	Responses   map[string]json.RawMessage
	MCPServers  []mcp.MCPConfig
	Auth        session.AuthContext
}

func (p *acpProtocol) bindTurn(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (turnRequest, error) {
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
	sess.mu.Unlock()

	if pr.UserMessage != nil {
		if mimes := pr.UserMessage.MIMETypes(); len(mimes) > 0 && env.Agent.Model != nil {
			if bad := tacklr.UnsupportedMIMEs(env.Agent.Model, mimes); len(bad) > 0 {
				return turnRequest{}, server.Errorf(server.ErrInvalidRequest, "unsupported content type(s): %s", strings.Join(bad, ", "))
			}
		}
	}
	if err := p.persistWire(ctx, sessionID, sess); err != nil {
		return turnRequest{}, err
	}

	return turnRequest{
		ThreadID:    sessionID,
		Prompt:      pr.Prompt,
		UserMessage: pr.UserMessage,
		Responses:   pr.Responses,
		MCPServers:  mcpServers,
		Auth:        sess.creds.Take(),
	}, nil
}

func (p *acpProtocol) closeSession(ctx context.Context, env server.ProtocolEnv, sessionID string) error {
	if _, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionClose); err != nil {
		return err
	}
	p.mu.Lock()
	delete(p.sessions, sessionID)
	p.mu.Unlock()
	if err := p.wire.Delete(ctx, sessionID); err != nil {
		return err
	}
	_ = env.Runtime.Close(ctx, session.SessionID(sessionID))
	return nil
}

func (p *acpProtocol) setConfig(ctx context.Context, env server.ProtocolEnv, sessionID, configID, _ string) (any, error) {
	if _, err := p.resolveOwnedWireSession(ctx, env, sessionID, actionSessionConfig); err != nil {
		return nil, err
	}
	return nil, server.Errorf(server.ErrInvalidRequest, "unknown configId %q", configID)
}

const (
	actionSessionCreate  = "session.create"
	actionSessionLoad    = "session.load"
	actionSessionPrompt  = "session.prompt"
	actionSessionConfig  = "session.configure"
	actionSessionClose   = "session.close"
	actionVFSCredentials = "vfs.credentials"
)

func securitySubject(env server.ProtocolEnv) string {
	if env.Conn != nil && env.Conn.Security != nil && env.Conn.Security.Authenticated() {
		return env.Conn.Security.Principal.Subject
	}
	return "local"
}

func authorizeOperation(ctx context.Context, env server.ProtocolEnv, action, resource string) error {
	if env.Security == nil {
		return nil
	}
	if env.Conn == nil || env.Conn.Security == nil || !env.Conn.Security.Authenticated() {
		return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
	}
	if err := env.Security.Authorize(ctx, *env.Conn.Security, server.Operation{
		Action:   action,
		Resource: resource,
	}); err != nil {
		return server.Errorf(server.ErrAuthorizationDenied, "authorization denied")
	}
	return nil
}

func (p *acpProtocol) resolveOwnedWireSession(ctx context.Context, env server.ProtocolEnv, sessionID, action string) (*acpWireSession, error) {
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if err := authorizeOperation(ctx, env, action, sessionID); err != nil {
		return nil, err
	}
	subject := securitySubject(env)
	sess.mu.Lock()
	owner := sess.owner
	sess.mu.Unlock()
	if owner != subject {
		return nil, server.Errorf(server.ErrAuthorizationDenied, "session is owned by another principal")
	}
	return sess, nil
}
