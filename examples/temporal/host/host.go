// Package host is the example's protocol server: ACP over WebSocket on the
// Temporal session client, behind one bearer token. It keeps no session
// state, so any number of hosts can serve the same sessions.
package host

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/session"
)

// New serves sessions over ACP at /acp. model refuses a prompt with content
// the model cannot read before it reaches a worker. wire keeps each ACP
// session's wire state, so any host can load the session. Each request must
// carry token as its bearer.
func New(sessions session.Client, model tacklr.Model, wire server.ProtocolWireStore, token string) *server.Server {
	srv := server.NewServer(sessions, tacklr.AgentOptions{Model: model}, acp.New(wire))
	// The workers mount a workspace, so a client may bind its own sources.
	srv.Workspace = true
	return srv.WithSecurity(&server.Service{Authenticator: bearer(token)}, func(r *http.Request) (server.Attempt, bool) {
		credential, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		return server.Attempt{Scheme: "bearer", Credential: server.NewSecret([]byte(credential))}, ok
	})
}

// bearer accepts the one token, in constant time.
type bearer string

func (b bearer) Authenticate(_ context.Context, a server.Attempt) (server.Principal, error) {
	if subtle.ConstantTimeCompare(a.Credential.Bytes(), []byte(b)) != 1 {
		return server.Principal{}, errors.New("unknown token")
	}
	return server.NewPrincipal("operator")
}
