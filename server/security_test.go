package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
)

// TestACPAuthentication_ownsSessionsByGenericPrincipal: a client must
// authenticate through the advertised method before it makes a session;
// the session is its principal's, so another principal cannot load it; and
// after logout the client cannot make one.
func TestACPAuthentication_ownsSessionsByGenericPrincipal(t *testing.T) {
	service := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if attempt.Scheme != "host-login" {
				return server.Principal{}, errors.New("unexpected scheme")
			}
			return server.NewPrincipal("alice")
		}),
	}
	protocol := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{
		ID:          "agent-login",
		Name:        "Agent login",
		Description: "Authenticate with the host",
		Scheme:      "host-login",
	}}, true)
	k := memoryHost(t)
	bob, err := server.NewPrincipal("bob")
	if err != nil {
		t.Fatal(err)
	}
	connection := &server.Conn{}
	bobConnection := &server.Conn{Security: &server.Context{Principal: bob}}
	call := func(conn *server.Conn, body string) map[string]any {
		t.Helper()
		recorder := httptest.NewRecorder()
		conn.Writer = acp.HTTPWriter(recorder)
		_ = protocol.HandleInbound(t.Context(), server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Conn: conn, Security: service}, []byte(body))
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode %q: %v", recorder.Body.String(), err)
		}
		return response
	}

	initialize := call(connection, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	unauthenticated := call(connection, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	authenticated := call(connection, `{"jsonrpc":"2.0","id":3,"method":"authenticate","params":{"methodId":"agent-login"}}`)
	created := call(connection, `{"jsonrpc":"2.0","id":4,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID, _ := created["result"].(map[string]any)["sessionId"].(string)
	bobLoad := call(bobConnection, `{"jsonrpc":"2.0","id":5,"method":"session/load","params":{"sessionId":"`+sessionID+`"}}`)
	loggedOut := call(connection, `{"jsonrpc":"2.0","id":6,"method":"logout","params":{}}`)
	afterLogout := call(connection, `{"jsonrpc":"2.0","id":7,"method":"session/new","params":{"cwd":"/tmp"}}`)

	methods := initialize["result"].(map[string]any)["authMethods"].([]any)
	if got := methods[0].(map[string]any)["id"]; got != "agent-login" {
		t.Fatalf("auth method id = %v", got)
	}
	if unauthenticated["error"] == nil || authenticated["error"] != nil || sessionID == "" {
		t.Fatalf("before = %v, authenticate = %v, then %v; want refused, then a session", unauthenticated, authenticated, created)
	}
	if bobLoad["error"] == nil || loggedOut["error"] != nil || afterLogout["error"] == nil {
		t.Fatalf("bob's load = %v, logout = %v, after = %v; want bob refused and the logged-out client refused", bobLoad, loggedOut, afterLogout)
	}
}

// TestServer_WithSecurity_authenticatesHTTPRequests: a route that needs
// authentication serves a request with a credential the host accepts, and
// answers 401 to one with no credential or a bad one; the ACP route, which
// authenticates in its own protocol, asks a plain GET to upgrade.
func TestServer_WithSecurity_authenticatesHTTPRequests(t *testing.T) {
	service := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if string(attempt.Credential.Bytes()) == "bad" {
				return server.Principal{}, errors.New("nope")
			}
			return server.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}
	k := memoryHost(t)
	mux := server.NewServer(k.Client, k.Agent, hostProtocol{}, acp.New(nil)).WithSecurity(service, func(r *http.Request) (server.Attempt, bool) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		return server.Attempt{Scheme: "bearer", Credential: server.NewSecret([]byte(token))}, ok
	}).HTTPMux()
	serve := func(path, token string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		mux.ServeHTTP(rec, req)
		return rec
	}

	authorized := serve("/healthz", "alice")
	anonymous := serve("/healthz", "")
	bad := serve("/healthz", "bad")
	acpGet := serve("/acp", "")

	if authorized.Code != http.StatusOK || authorized.Body.String() != "ok" {
		t.Fatalf("authorized = %d %s", authorized.Code, authorized.Body.String())
	}
	if anonymous.Code != http.StatusUnauthorized || bad.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous = %d, bad credential = %d; want 401 each", anonymous.Code, bad.Code)
	}
	if acpGet.Code != http.StatusUpgradeRequired {
		t.Fatalf("unauthenticated ACP GET = %d, want upgrade required", acpGet.Code)
	}
}
