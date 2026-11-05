package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
)

// TestHandleInbound_errorContract is the host/client recovery contract:
// each sentinel is reachable from HandleInbound, errors.Is holds, and the
// JSON-RPC code on the wire is the one consumers should switch on.
func TestHandleInbound_errorContract(t *testing.T) {
	assert := func(t *testing.T, err, sentinel error, code int, public error) {
		t.Helper()
		if !errors.Is(err, sentinel) {
			t.Fatalf("errors.Is(%v, %v) = false", err, sentinel)
		}
		if got := acp.ErrorCode(err); got != code {
			t.Fatalf("acp.ErrorCode = %d, want %d", got, code)
		}
		if pub := server.PublicError(err); !errors.Is(pub, public) {
			t.Fatalf("server.PublicError = %v, want %v", pub, public)
		}
		if wire := acp.ErrorBody(json.RawMessage(`1`), err)["error"].(map[string]any)["code"]; wire != code {
			t.Fatalf("wire code = %v, want %d", wire, code)
		}
	}
	k := memoryHost(t)
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}
	login := []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}

	t.Run("methodNotFound", func(t *testing.T) {
		err := acptest.WrittenError(t, acp.New(nil), env, `{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{}}`)
		assert(t, err, server.ErrMethodNotFound, acp.CodeMethodNotFound, server.ErrMethodNotFound)
	})

	t.Run("invalidRequest", func(t *testing.T) {
		err := acptest.WrittenError(t, acp.New(nil), env, `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{}}`)
		assert(t, err, server.ErrInvalidRequest, acp.CodeInvalidRequest, server.ErrInvalidRequest)
	})

	t.Run("sessionNotFound", func(t *testing.T) {
		err := acptest.WrittenError(t, acp.New(nil), env, `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"missing"}}`)
		assert(t, err, server.ErrSessionNotFound, acp.CodeApplication, server.ErrSessionNotFound)
	})

	t.Run("unknownConfig", func(t *testing.T) {
		proto := acp.New(nil)
		sid := acptest.SessionID(t, acptest.Serve(t, k, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		err := acptest.WrittenError(t, proto, env,
			`{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"model","value":"ghost"}}`)
		assert(t, err, server.ErrInvalidRequest, acp.CodeInvalidRequest, server.ErrInvalidRequest)
	})

	t.Run("authenticationRequired", func(t *testing.T) {
		err := acptest.WrittenError(t, acp.NewWithAuth(nil, login, false), env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		assert(t, err, server.ErrAuthenticationRequired, acp.CodeApplication, server.ErrAuthenticationRequired)
	})

	t.Run("authenticationFailed", func(t *testing.T) {
		service := &server.Service{Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) {
			return server.Principal{}, server.ErrAuthenticationFailed
		})}
		err := acptest.WrittenError(t, acp.NewWithAuth(nil, login, false), server.ProtocolEnv{
			Client: k.Client, Agent: k.Agent, Security: service, Conn: &server.Conn{Security: &server.Context{}},
		}, `{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{"methodId":"login"}}`)
		assert(t, err, server.ErrAuthenticationFailed, acp.CodeApplication, server.ErrAuthenticationFailed)
	})

	t.Run("authorizationDenied", func(t *testing.T) {
		alice, err := server.NewPrincipal("alice")
		if err != nil {
			t.Fatal(err)
		}
		service := &server.Service{
			Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) { return alice, nil }),
			Authorizer: authorizerFunc(func(_ context.Context, _ server.Principal, op server.Operation) error {
				if op.Action == "session.load" {
					return errors.New("denied")
				}
				return nil
			}),
		}
		proto := acp.NewWithAuth(nil, login, false)
		aliceEnv := server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Security: service, Conn: &server.Conn{Security: &server.Context{Principal: alice}}}
		sid := sessionIDFromInbound(t, proto, aliceEnv, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		got := acptest.WrittenError(t, proto, aliceEnv, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`"}}`)
		assert(t, got, server.ErrAuthorizationDenied, acp.CodeApplication, server.ErrAuthorizationDenied)
	})

	t.Run("cancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := acp.New(nil).HandleInbound(ctx, server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder())}},
			[]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
		if !errors.Is(err, context.Canceled) || acp.ErrorCode(err) != acp.CodeCancelled || !errors.Is(server.PublicError(err), server.ErrInternal) {
			t.Fatalf("cancelled = %v (code %d), want cancelled, redacted to internal on the wire", err, acp.ErrorCode(err))
		}
	})

	t.Run("internalProviderFailure", func(t *testing.T) {
		failing := acptest.NewRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventError, Error: errors.New("provider down"), Content: "provider down"}
		}), tacklr.AgentOptions{})
		proto := acp.New(nil)
		sid := acptest.SessionID(t, acptest.Serve(t, failing, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		rec := acptest.Serve(t, failing, proto, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`)
		var code any
		for _, f := range acptest.Frames(t, rec.Body) {
			if errObj, ok := f["error"].(map[string]any); ok {
				code = errObj["code"]
			}
		}
		if code != float64(acp.CodeInternal) {
			t.Fatalf("wire code = %v, want %d: %s", code, acp.CodeInternal, rec.Body.String())
		}
	})
}

// TestHandleInbound_sessionWireOutcomes: the protocol answers each request
// it can serve, and refuses each one it cannot: a bad request, an unknown
// session or auth method, a client that has not said hello, a principal the
// host denies, a wire store or a Temporal cluster that is down.
func TestHandleInbound_sessionWireOutcomes(t *testing.T) {
	k := memoryHost(t)
	proto := acp.New(nil)
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}
	sid := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	emptyCWD := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}`)
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	authProto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, true)
	schemeProto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login"}}, false)
	denyCreate := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if attempt.Scheme != "login" {
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
	mem := server.NewMemoryWireStore()
	if err := mem.Put(t.Context(), "corrupt", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	wire := wirepg.NewWireStore(pool, "")
	if err := wire.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	pgProto := acp.New(wire)
	pgSID := sessionIDFromInbound(t, pgProto, env, `{"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":"/tmp"}}`)
	pool.Close()
	downEnv := server.ProtocolEnv{Client: livesess.Down(t), Agent: k.Agent}
	prompt := func(id string) string {
		return `{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"` + id + `","prompt":[{"type":"text","text":"hi"}]}}`
	}

	resume := httptest.NewRecorder()
	_ = proto.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Conn: &server.Conn{Writer: acp.HTTPWriter(resume)}},
		[]byte(`{"jsonrpc":"2.0","id":5,"method":"session/resume","params":{"sessionId":"`+sid+`","responses":{"intr-1":"{}"}}}`))
	var stored server.Context
	served := map[string]error{
		"cancel": proto.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder())}},
			[]byte(`{"jsonrpc":"2.0","id":6,"method":"session/cancel","params":{"sessionId":"`+sid+`"}}`)),
		"cancel notification": proto.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder())}},
			[]byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{}}`)),
		"logout": proto.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder())}},
			[]byte(`{"jsonrpc":"2.0","id":7,"method":"logout","params":{}}`)),
		"load filling the cwd": proto.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder())}},
			[]byte(`{"jsonrpc":"2.0","id":8,"method":"session/load","params":{"sessionId":"`+emptyCWD+`","cwd":"/filled","mcpServers":[{"type":"http","name":"x","url":"https://example.com/mcp"}]}}`)),
		"authenticate with the method id as scheme": schemeProto.HandleInbound(t.Context(), server.ProtocolEnv{
			Client: k.Client, Security: denyCreate,
			Conn: &server.Conn{Writer: acp.HTTPWriter(httptest.NewRecorder()), SetSecurity: func(c server.Context) { stored = c }},
		}, []byte(`{"jsonrpc":"2.0","id":9,"method":"authenticate","params":{"methodId":"login"}}`)),
	}
	refused := map[string]error{}
	for name, req := range map[string]struct {
		proto server.Protocol
		env   server.ProtocolEnv
		body  string
	}{
		"old protocol version":         {proto, env, `{"jsonrpc":"2.0","id":10,"method":"initialize","params":{"protocolVersion":0}}`},
		"invalid JSON-RPC":             {proto, env, `{"jsonrpc":"1.0","id":11,"method":"initialize"}`},
		"unknown config":               {proto, env, `{"jsonrpc":"2.0","id":12,"method":"session/set_config_option","params":{"sessionId":"` + sid + `","configId":"nope","value":"x"}}`},
		"unknown auth method":          {authProto, env, `{"jsonrpc":"2.0","id":13,"method":"authenticate","params":{"methodId":"ghost"}}`},
		"authenticate with no service": {authProto, env, `{"jsonrpc":"2.0","id":14,"method":"authenticate","params":{"methodId":"login"}}`},
		"authenticate with no method":  {authProto, env, `{"jsonrpc":"2.0","id":15,"method":"authenticate","params":{}}`},
		"prompt before hello": {proto, server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Conn: &server.Conn{Ask: acp.NewClientBridge(acp.HTTPWriter(httptest.NewRecorder()))}},
			prompt(sid)},
		"corrupt wire envelope": {acp.New(mem), env, `{"jsonrpc":"2.0","id":16,"method":"session/load","params":{"sessionId":"corrupt","cwd":"/tmp"}}`},
		"close unknown":         {proto, env, `{"jsonrpc":"2.0","id":17,"method":"session/close","params":{"sessionId":"missing"}}`},
		"cancel unknown":        {proto, env, `{"jsonrpc":"2.0","id":18,"method":"session/cancel","params":{"sessionId":"missing"}}`},
		"prompt unknown":        {proto, env, prompt("missing")},
		"configure unknown":     {proto, env, `{"jsonrpc":"2.0","id":19,"method":"session/set_config_option","params":{"sessionId":"missing","configId":"model","value":"default"}}`},
		"bind unknown":          {proto, env, `{"jsonrpc":"2.0","id":20,"method":"_tacklr/vfs/bind","params":{"sessionId":"missing","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"t"}}]}}`},
		"refresh unknown":       {proto, env, `{"jsonrpc":"2.0","id":21,"method":"_tacklr/vfs/refresh","params":{"sessionId":"missing","provider":"local","auth":{"token":"t"}}}`},
		"create denied": {acp.New(nil), server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Security: denyCreate, Conn: &server.Conn{Security: &server.Context{Principal: alice}}},
			`{"jsonrpc":"2.0","id":22,"method":"session/new","params":{"cwd":"/tmp"}}`},
		"load unauthenticated": {proto, server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Security: denyCreate, Conn: &server.Conn{Security: &server.Context{}}},
			`{"jsonrpc":"2.0","id":23,"method":"session/load","params":{"sessionId":"` + sid + `"}}`},
		"create on a down Temporal": {acp.New(nil), downEnv, `{"jsonrpc":"2.0","id":24,"method":"session/new","params":{"cwd":"/tmp"}}`},
		"load on a down Temporal":   {proto, downEnv, `{"jsonrpc":"2.0","id":25,"method":"session/load","params":{"sessionId":"` + sid + `"}}`},
		"prompt on a down Temporal": {proto, downEnv, prompt(sid)},
		"prompt on a down store":    {pgProto, env, prompt(pgSID)},
		"load on a down store":      {pgProto, env, `{"jsonrpc":"2.0","id":26,"method":"session/load","params":{"sessionId":"` + pgSID + `","cwd":"/tmp"}}`},
		"create on a down store":    {pgProto, env, `{"jsonrpc":"2.0","id":27,"method":"session/new","params":{"cwd":"/tmp"}}`},
		"read from a down store":    {pgProto, env, `{"jsonrpc":"2.0","id":28,"method":"session/load","params":{"sessionId":"x","cwd":"/tmp"}}`},
	} {
		ctx, cancel := t.Context(), context.CancelFunc(func() {})
		if req.env.Conn != nil && req.env.Conn.Ask != nil {
			ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
		}
		conn := server.Conn{}
		if req.env.Conn != nil {
			conn = *req.env.Conn
		}
		conn.Writer = acp.HTTPWriter(httptest.NewRecorder())
		req.env.Conn = &conn
		refused[name] = req.proto.HandleInbound(ctx, req.env, []byte(req.body))
		cancel()
	}
	closeDown := acptest.WrittenError(t, pgProto, env, `{"jsonrpc":"2.0","id":29,"method":"session/close","params":{"sessionId":"`+pgSID+`"}}`)

	for name, err := range served {
		if err != nil {
			t.Errorf("%s: %v, want it served", name, err)
		}
	}
	if stored.Principal.Subject != "alice" {
		t.Errorf("authenticate stored %q, want alice", stored.Principal.Subject)
	}
	if !strings.Contains(resume.Body.String(), "intr-1") {
		t.Errorf("resume of an idle session = %s, want the unknown interrupt named", resume.Body.String())
	}
	for name, err := range refused {
		if err == nil {
			t.Errorf("%s: served, want it refused", name)
		}
	}
	if closeDown == nil {
		t.Error("close on a down store: served, want it refused")
	}
}

// sessionIDFromInbound is the sessionId the session/new body gets.
func sessionIDFromInbound(t *testing.T, proto server.Protocol, env server.ProtocolEnv, body string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	conn := server.Conn{}
	if env.Conn != nil {
		conn = *env.Conn
	}
	conn.Writer = acp.HTTPWriter(rec)
	env.Conn = &conn
	if err := proto.HandleInbound(t.Context(), env, []byte(body)); err != nil {
		t.Fatal(err)
	}
	return acptest.SessionID(t, rec)
}
