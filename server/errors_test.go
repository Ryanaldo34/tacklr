package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	wirepg "github.com/ryanaldo34/tacklr/server/postgres"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
)

// bearerOf is a host's HTTP edge: the bearer token is the credential.
func bearerOf(r *http.Request) (server.Attempt, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return server.Attempt{Scheme: "bearer", Credential: server.NewSecret([]byte(token))}, ok
}

// bearer is the header of a request with token as its bearer.
func bearer(token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}}
}

// secured is a server for k on proto with service as its security, behind
// bearerOf.
func secured(t *testing.T, k *acptest.Runtime, proto server.Protocol, service *server.Service) string {
	t.Helper()
	return acptest.Listen(t, server.NewServer(k.Client, k.Agent, proto).WithSecurity(service, bearerOf))
}

// connectAs connects to url with token as its bearer and says hello.
func connectAs(t *testing.T, url, token string) *acptest.Client {
	t.Helper()
	c := acptest.Dial(t, url, bearer(token))
	acptest.Result(t, c.Call("initialize", `{"protocolVersion":1}`))
	return c
}

// anonymous is an anonymous server for k on proto.
func anonymous(t *testing.T, k *acptest.Runtime, proto server.Protocol) string {
	t.Helper()
	return acptest.Listen(t, server.NewServer(k.Client, k.Agent, proto).AllowAnonymousNetwork())
}

// TestACP_errorContract is the host/client recovery contract: each
// sentinel reaches the client with the JSON-RPC code consumers switch on,
// and with a message that does not leak internal details.
func TestACP_errorContract(t *testing.T) {
	t.Parallel()
	k := memoryHost(t)
	login := []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	failing := &server.Service{Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) {
		return server.Principal{}, server.ErrAuthenticationFailed
	})}
	denyLoad := &server.Service{
		Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) { return alice, nil }),
		Authorizer: authorizerFunc(func(_ context.Context, _ server.Principal, op server.Operation) error {
			if op.Action == "session.load" {
				return errors.New("denied")
			}
			return nil
		}),
	}
	provider := acptest.NewRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventError, Error: errors.New("provider down"), Content: "provider down"}
	}), tacklr.AgentOptions{})
	plain := acptest.Connect(t, anonymous(t, k, acp.New(nil)), `{"protocolVersion":1}`)
	sid := plain.NewSession("/tmp")
	denied := connectAs(t, secured(t, k, acp.NewWithAuth(nil, login, false), denyLoad), "alice")
	deniedSID := denied.NewSession("/tmp")
	down := acptest.Open(t, provider)
	downSID := down.NewSession("/tmp")
	// A turn whose stream breaks: its object is evicted while it streams.
	platform, stalling := objectHost(t, dotest.NewWire, testkit.HTTPModel(t, func(ctx context.Context, _ []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "early"}
		<-ctx.Done()
	}))
	broken := acptest.Open(t, stalling)
	brokenSID := broken.NewSession("/tmp")
	brokenID := broken.Prompt(brokenSID, `[{"type":"text","text":"hi"}]`)
	for len(acptest.Updates([]map[string]any{broken.Frame()})) == 0 {
	}
	platform.Evict(session.ID(brokenSID))
	streamBroke, _ := broken.Until(brokenID, nil)

	for name, c := range map[string]struct {
		resp map[string]any
		code int
		msg  string
	}{
		"method not found":  {plain.Call("session/foo", `{}`), acp.CodeMethodNotFound, "method not found"},
		"invalid request":   {plain.Call("session/load", `{}`), acp.CodeInvalidRequest, "sessionId is required"},
		"session not found": {plain.Call("session/load", `{"sessionId":"missing"}`), acp.CodeApplication, `session "missing" not found`},
		"unknown config": {plain.Call("session/set_config_option", `{"sessionId":"`+sid+`","configId":"model","value":"ghost"}`),
			acp.CodeInvalidRequest, `unknown configId "model"`},
		"authentication required": {acptest.Connect(t, secured(t, k, acp.NewWithAuth(nil, login, false), failing), `{"protocolVersion":1}`).
			Call("session/new", `{"cwd":"/tmp"}`), acp.CodeApplication, "authentication required"},
		"authentication failed": {acptest.Connect(t, secured(t, k, acp.NewWithAuth(nil, login, false), failing), `{"protocolVersion":1}`).
			Call("authenticate", `{"methodId":"login"}`), acp.CodeApplication, "authentication failed"},
		"authorization denied": {denied.Call("session/load", `{"sessionId":"`+deniedSID+`"}`), acp.CodeApplication, "authorization denied"},
		"provider failure":     {first(down.Say(downSID, "hi")), acp.CodeInternal, "provider down"},
		"stream broken":        {streamBroke, acp.CodeInternal, server.ErrInternal.Error()},
	} {
		errObj, _ := c.resp["error"].(map[string]any)
		if errObj == nil {
			t.Errorf("%s: %v, want an error", name, c.resp)
			continue
		}
		if code, _ := errObj["code"].(float64); int(code) != c.code || !strings.Contains(errObj["message"].(string), c.msg) {
			t.Errorf("%s: %v, want code %d and %q", name, errObj, c.code, c.msg)
		}
	}
	// A failure that is not the client's is internal on the wire.
	if body := acp.ErrorBody(nil, context.Canceled)["error"].(map[string]any); body["code"] != acp.CodeCancelled || body["message"] != server.ErrInternal.Error() {
		t.Errorf("cancelled = %v, want the cancelled code and an internal message", body)
	}
}

// first is a turn's response, without the frames before it.
func first(resp map[string]any, _ []map[string]any) map[string]any { return resp }

// TestACP_sessionWireOutcomes: the protocol answers each request it can
// serve, and refuses each one it cannot: a bad request, an unknown session
// or auth method, a client that has not said hello, a principal the host
// denies, a wire store or a Temporal cluster that is down, a session's
// object whose storage fails, and a session the host closed.
func TestACP_sessionWireOutcomes(t *testing.T) {
	t.Parallel()
	k := memoryHost(t)
	wire := server.NewMemoryWireStore()
	if err := wire.Put(t.Context(), "corrupt", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	url := anonymous(t, k, acp.New(wire))
	c := acptest.Connect(t, url, `{"protocolVersion":1}`)
	sid := c.NewSession("/tmp")
	emptyCWD := acptest.SessionID(t, c.Call("session/new", `{}`))
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	denyCreate := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if attempt.Scheme != "login" && attempt.Scheme != "bearer" {
				return server.Principal{}, fmt.Errorf("scheme %q", attempt.Scheme)
			}
			return alice, nil
		}),
		Authorizer: authorizerFunc(func(_ context.Context, _ server.Principal, op server.Operation) error {
			if op.Action == "session.create" {
				return errors.New("denied")
			}
			return nil
		}),
	}
	authURL := anonymous(t, k, acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, true))
	scheme := acptest.Connect(t, secured(t, k, acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login"}}, false), denyCreate), `{"protocolVersion":1}`)
	pool, err := pgxpool.New(t.Context(), pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	pg := wirepg.NewWireStore(pool, "")
	if err := pg.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	onPG := acptest.Connect(t, anonymous(t, k, acp.New(pg)), `{"protocolVersion":1}`)
	pgSID := onPG.NewSession("/tmp")
	pool.Close()
	downK := &acptest.Runtime{Client: livesess.Down(t), Agent: k.Agent}
	onDown := acptest.Connect(t, anonymous(t, downK, acp.New(wire)), `{"protocolVersion":1}`)
	prompt := func(id string) string { return `{"sessionId":"` + id + `","prompt":[{"type":"text","text":"hi"}]}` }
	// Sessions whose wire state is in their objects: one whose storage
	// cannot write, and one the host closed itself.
	objects, objK := objectHost(t, dotest.New, testkit.HTTPModel(t, nil))
	onObjects := acptest.Connect(t, anonymous(t, objK, acp.New(objK.Client.(*durableobject.Client).WireStore())), `{"protocolVersion":1}`)
	unwritable := acptest.SessionID(t, onObjects.Call("session/new", `{}`))
	hostClosed := onObjects.NewSession("/tmp")
	objects.FailWrites(session.ID(unwritable), func([]durableobject.Op) bool { return true })
	if err := objK.Client.Close(t.Context(), session.ID(hostClosed)); err != nil {
		t.Fatal(err)
	}

	resumed := c.Call("session/resume", `{"sessionId":"`+sid+`","responses":{"intr-1":"{}"}}`)
	c.Send(`{"jsonrpc":"2.0","method":"session/cancel","params":{}}`)
	served := map[string]map[string]any{
		"cancel":               c.Call("session/cancel", `{"sessionId":"`+sid+`"}`),
		"load filling the cwd": c.Call("session/load", `{"sessionId":"`+emptyCWD+`","cwd":"/filled","mcpServers":[{"type":"http","name":"x","url":"https://example.com/mcp"}]}`),
		"authenticate with the method id as scheme": scheme.Call("authenticate", `{"methodId":"login"}`),
	}
	afterAuthenticate := scheme.Call("session/new", `{"cwd":"/tmp"}`)
	loggedOut := acptest.Connect(t, url, `{"protocolVersion":1}`).Call("logout", `{}`)
	refused := map[string]map[string]any{
		"old protocol version":         c.Call("initialize", `{"protocolVersion":0}`),
		"unknown config":               c.Call("session/set_config_option", `{"sessionId":"`+sid+`","configId":"nope","value":"x"}`),
		"unknown auth method":          acptest.Connect(t, authURL, `{"protocolVersion":1}`).Call("authenticate", `{"methodId":"ghost"}`),
		"authenticate with no service": acptest.Connect(t, authURL, `{"protocolVersion":1}`).Call("authenticate", `{"methodId":"login"}`),
		"authenticate with no method":  acptest.Connect(t, authURL, `{"protocolVersion":1}`).Call("authenticate", `{}`),
		"prompt before hello":          acptest.Dial(t, url, nil).Call("session/prompt", prompt(sid)),
		"corrupt wire envelope":        c.Call("session/load", `{"sessionId":"corrupt","cwd":"/tmp"}`),
		"close unknown":                c.Call("session/close", `{"sessionId":"missing"}`),
		"cancel unknown":               c.Call("session/cancel", `{"sessionId":"missing"}`),
		"prompt unknown":               c.Call("session/prompt", prompt("missing")),
		"configure unknown":            c.Call("session/set_config_option", `{"sessionId":"missing","configId":"model","value":"default"}`),
		"bind unknown":                 c.Call("_tacklr/vfs/bind", `{"sessionId":"missing","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"t"}}]}`),
		"refresh unknown":              c.Call("_tacklr/vfs/refresh", `{"sessionId":"missing","provider":"local","auth":{"token":"t"}}`),
		"create denied":                connectAs(t, secured(t, k, acp.New(nil), denyCreate), "alice").Call("session/new", `{"cwd":"/tmp"}`),
		"load unauthenticated": acptest.Connect(t, secured(t, k, acp.New(wire), denyCreate), `{"protocolVersion":1}`).
			Call("session/load", `{"sessionId":"`+sid+`"}`),
		"create on a down Temporal": onDown.Call("session/new", `{"cwd":"/tmp"}`),
		"load on a down Temporal":   onDown.Call("session/load", `{"sessionId":"`+sid+`"}`),
		"prompt on a down Temporal": onDown.Call("session/prompt", prompt(sid)),
		"prompt on a down store":    onPG.Call("session/prompt", prompt(pgSID)),
		"load on a down store":      onPG.Call("session/load", `{"sessionId":"`+pgSID+`","cwd":"/tmp"}`),
		"create on a down store":    onPG.Call("session/new", `{"cwd":"/tmp"}`),
		"close on a down store":     onPG.Call("session/close", `{"sessionId":"`+pgSID+`"}`),
		"load that cannot write":    onObjects.Call("session/load", `{"sessionId":"`+unwritable+`","cwd":"/filled"}`),
		"prompt that cannot write":  onObjects.Call("session/prompt", prompt(unwritable)),
		"close that cannot write":   onObjects.Call("session/close", `{"sessionId":"`+unwritable+`"}`),
		"load a closed session":     onObjects.Call("session/load", `{"sessionId":"`+hostClosed+`"}`),
		"prompt a closed session":   onObjects.Call("session/prompt", prompt(hostClosed)),
	}
	c.Send(`{"jsonrpc":"1.0","id":11,"method":"initialize"}`)
	invalid := c.Frame()

	for name, resp := range served {
		if resp["error"] != nil {
			t.Errorf("%s: %v, want it served", name, resp)
		}
	}
	if loggedOut["error"] != nil {
		t.Errorf("logout = %v, want it served", loggedOut)
	}
	if msg := acptest.Message(t, afterAuthenticate); msg != "authorization denied" {
		t.Errorf("session/new after authenticate = %q, want the authenticated principal denied", msg)
	}
	if msg := acptest.Message(t, resumed); !strings.Contains(msg, "intr-1") {
		t.Errorf("resume of an idle session = %q, want the unknown interrupt named", msg)
	}
	for name, resp := range refused {
		if resp["error"] == nil {
			t.Errorf("%s: %v, want it refused", name, resp)
		}
	}
	if invalid["error"] == nil {
		t.Errorf("invalid JSON-RPC = %v, want it refused", invalid)
	}
}
