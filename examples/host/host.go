// Package host is the examples' protocol server: ACP over WebSocket on a
// session client, behind one bearer token. It keeps no session state, so
// any number of hosts can serve the same sessions. The Temporal example
// serves it with net/http (examples/temporal/cmd/host), and the Cloudflare
// example serves the same server from a connection object for each
// WebSocket (examples/cloudflare). docs/protocol.md follows a turn through
// it.
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
// the model cannot read before it reaches the session. wire keeps each ACP
// session's wire state, so any host can load the session. Each request must
// carry token as its bearer. A host whose sessions mount a workspace sets
// the server's Workspace, so a client may bind its own sources.
func New(sessions session.Client, model tacklr.Model, wire server.ProtocolWireStore, token string) *server.Server {
	srv := server.NewServer(sessions, tacklr.AgentOptions{Model: model}, acp.New(wire))
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
