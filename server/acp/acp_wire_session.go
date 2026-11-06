package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// wireSession is what ACP owns of a session, as the wire store keeps it:
// its cwd and its owner. Each request reads it from the store, so every
// host and connection sees the same. The rest of the session (its MCP
// servers among it) is the runtime's.
type wireSession struct {
	CWD   string `json:"cwd"`
	Owner string `json:"owner,omitempty"`
}

func (s wireSession) cwdMismatch(cwd string) error {
	if cwd != "" && s.CWD != "" && cwd != s.CWD {
		return server.Errorf(server.ErrInvalidRequest, "cwd does not match session cwd")
	}
	return nil
}

func (c *conn) persistWire(ctx context.Context, sessionID string, sess wireSession) error {
	raw, _ := json.Marshal(sess)
	return c.p.wire.Put(ctx, sessionID, raw)
}

func (c *conn) wireSession(ctx context.Context, sessionID string) (wireSession, error) {
	raw, err := c.p.wire.Get(ctx, sessionID)
	if err != nil {
		if errors.Is(err, server.ErrSessionNotFound) {
			return wireSession{}, server.Errorf(server.ErrSessionNotFound, "session %q not found", sessionID)
		}
		return wireSession{}, fmt.Errorf("load wire session %q: %w", sessionID, err)
	}
	var sess wireSession
	if err := json.Unmarshal(raw, &sess); err != nil {
		return wireSession{}, fmt.Errorf("decode wire session %q: %w", sessionID, err)
	}
	return sess, nil
}

func (c *conn) createSession(ctx context.Context, pr *parsedRequest) (any, error) {
	if err := c.authorize(ctx, actionSessionCreate, ""); err != nil {
		return nil, err
	}
	sessionID := uuid.NewString()
	telemetry.MustInstruments(telemetry.Meter()).RecordSessionCreated(ctx)
	if _, err := c.env.Client.CreateSession(ctx, session.CreateSession{
		SessionID:  session.ID(sessionID),
		MCPServers: pr.MCPServers,
	}); err != nil {
		return nil, err
	}
	sess := wireSession{CWD: pr.CWD, Owner: c.subject()}
	if err := c.persistWire(ctx, sessionID, sess); err != nil {
		return nil, err
	}
	return map[string]any{"sessionId": sessionID}, nil
}

// loadSession loads a session the client owns. A session whose turn runs
// is followed from now on; one parked on an interrupt asks the client
// again. MCP servers on the load go to the session with its next prompt.
func (c *conn) loadSession(ctx context.Context, pr *parsedRequest) (any, error) {
	sessionID := pr.ThreadID
	sess, err := c.ownedSession(ctx, sessionID, actionSessionLoad)
	if err != nil {
		return nil, err
	}
	if err := sess.cwdMismatch(pr.CWD); err != nil {
		return nil, err
	}
	if sess.CWD == "" && pr.CWD != "" {
		sess.CWD = pr.CWD
		if err := c.persistWire(ctx, sessionID, sess); err != nil {
			return nil, err
		}
	}
	id := session.ID(sessionID)
	// The runtime owns whether the session is there, and how it runs.
	st, err := c.env.Client.Status(ctx, id)
	if err == nil {
		if len(pr.MCPServers) > 0 {
			c.nextOf(id).mcpServers = pr.MCPServers
		}
		if st.State == session.Running {
			err = c.follow(ctx, id, st.Waiting)
		}
	}
	// The client gets err, when there is one, in place of the result.
	return map[string]any{"sessionId": sessionID}, err
}

// turnRequest is bindTurn output: one prompt or resume against Runtime.
type turnRequest struct {
	ThreadID    string
	Prompt      string
	UserMessage *message.Message
	Responses   map[string]json.RawMessage
	MCPServers  []session.MCPServer
	Auth        session.AuthContext
}

func (c *conn) bindTurn(ctx context.Context, pr *parsedRequest) (turnRequest, error) {
	sessionID := pr.ThreadID
	sess, err := c.ownedSession(ctx, sessionID, actionSessionPrompt)
	if err != nil {
		return turnRequest{}, err
	}
	if err := sess.cwdMismatch(pr.CWD); err != nil {
		return turnRequest{}, err
	}
	// MCP servers on the request, or else on the last load, replace the
	// session's from this turn on; with none, the session keeps its own.
	next := c.nextOf(session.ID(sessionID))
	mcpServers := pr.MCPServers
	if len(mcpServers) == 0 {
		mcpServers = next.mcpServers
	}
	if pr.UserMessage != nil {
		if mimes := pr.UserMessage.MIMETypes(); len(mimes) > 0 && c.env.Agent.Model != nil {
			if bad := tacklr.UnsupportedMIMEs(c.env.Agent.Model, mimes); len(bad) > 0 {
				return turnRequest{}, server.Errorf(server.ErrInvalidRequest, "unsupported content type(s): %s", strings.Join(bad, ", "))
			}
		}
	}
	return turnRequest{
		ThreadID:    sessionID,
		Prompt:      pr.Prompt,
		UserMessage: pr.UserMessage,
		Responses:   pr.Responses,
		MCPServers:  mcpServers,
		Auth:        next.creds.Take(),
	}, nil
}

func (c *conn) closeSession(ctx context.Context, sessionID string) error {
	if _, err := c.ownedSession(ctx, sessionID, actionSessionClose); err != nil {
		return err
	}
	id := session.ID(sessionID)
	c.unfollow(id)
	delete(c.next, id)
	if err := c.p.wire.Delete(ctx, sessionID); err != nil {
		return err
	}
	_ = c.env.Client.Close(ctx, id)
	return nil
}

// setConfig refuses each config option: the agent advertises none.
func (c *conn) setConfig(ctx context.Context, sessionID, configID string) (any, error) {
	if _, err := c.ownedSession(ctx, sessionID, actionSessionConfig); err != nil {
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

// subject is who owns the sessions the connection makes: its principal
// (on a server with no security, "anonymous").
func (c *conn) subject() string { return c.security.Principal.Subject }

func (c *conn) authorize(ctx context.Context, action, resource string) error {
	if c.env.Security == nil {
		return nil
	}
	if !c.security.Authenticated() {
		return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
	}
	if err := c.env.Security.Authorize(ctx, c.security, server.Operation{Action: action, Resource: resource}); err != nil {
		return server.Errorf(server.ErrAuthorizationDenied, "authorization denied")
	}
	return nil
}

// ownedSession is the wire state of a session the connection's principal
// owns and may act on.
func (c *conn) ownedSession(ctx context.Context, sessionID, action string) (wireSession, error) {
	sess, err := c.wireSession(ctx, sessionID)
	if err != nil {
		return wireSession{}, err
	}
	if err := c.authorize(ctx, action, sessionID); err != nil {
		return wireSession{}, err
	}
	if sess.Owner != c.subject() {
		return wireSession{}, server.Errorf(server.ErrAuthorizationDenied, "session is owned by another principal")
	}
	return sess, nil
}
