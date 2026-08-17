package server

import (
	"context"

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

const (
	actionSessionCreate  = "session.create"
	actionSessionLoad    = "session.load"
	actionSessionPrompt  = "session.prompt"
	actionSessionConfig  = "session.configure"
	actionSessionClose   = "session.close"
	actionVFSCredentials = "vfs.credentials"
)

func securitySubject(env ProtocolEnv) string {
	if env.Conn != nil && env.Conn.Security != nil && env.Conn.Security.Authenticated() {
		return env.Conn.Security.Principal.Subject
	}
	// Direct protocol use and stdio without a configured host security service
	// remain process-local for backwards compatibility.
	if env.Security == nil {
		return "local"
	}
	return ""
}

func authorizeOperation(ctx context.Context, env ProtocolEnv, action, resource string) error {
	if env.Security == nil {
		return nil
	}
	if env.Conn == nil || env.Conn.Security == nil || !env.Conn.Security.Authenticated() {
		return clientErrorf(ErrAuthenticationRequired, "authentication required")
	}
	if err := env.Security.Authorize(ctx, *env.Conn.Security, tacklrsecurity.Operation{
		Action:   action,
		Resource: resource,
	}); err != nil {
		return clientErrorf(ErrAuthorizationDenied, "authorization denied")
	}
	return nil
}

func (p *acpProtocol) resolveOwnedWireSession(ctx context.Context, env ProtocolEnv, sessionID, action string) (*acpWireSession, error) {
	sess, err := p.resolveWireSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	subject := securitySubject(env)
	if subject == "" {
		return nil, clientErrorf(ErrAuthenticationRequired, "authentication required")
	}

	sess.mu.Lock()
	claimed := false
	if sess.owner == "" {
		// Legacy envelopes did not include an owner. The first authenticated
		// load upgrades the envelope; new sessions are always born owned.
		sess.owner = subject
		claimed = true
	}
	owner := sess.owner
	sess.mu.Unlock()
	if owner != subject {
		return nil, clientErrorf(ErrAuthorizationDenied, "session is owned by another principal")
	}
	if err := authorizeOperation(ctx, env, action, sessionID); err != nil {
		return nil, err
	}
	if claimed {
		if err := p.persistWire(ctx, sessionID, sess); err != nil {
			return nil, err
		}
	}
	return sess, nil
}
