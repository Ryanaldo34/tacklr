package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

type testAuthenticator func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error)

func (f testAuthenticator) Authenticate(ctx context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
	return f(ctx, attempt)
}

type testAuthorizer func(context.Context, tacklrsecurity.Principal, tacklrsecurity.Operation) error

func (f testAuthorizer) Authorize(ctx context.Context, principal tacklrsecurity.Principal, operation tacklrsecurity.Operation) error {
	return f(ctx, principal, operation)
}

func TestACPAuthentication_ownsSessionsByGenericPrincipal(t *testing.T) {
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			if attempt.Scheme != "host-login" {
				return tacklrsecurity.Principal{}, errors.New("unexpected scheme")
			}
			return tacklrsecurity.NewPrincipal("alice")
		}),
	}
	protocol := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{
		ID:          "agent-login",
		Name:        "Agent login",
		Description: "Authenticate with the host",
		Scheme:      "host-login",
	}}, true)
	k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	connection := &Conn{}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: connection, Security: service}
	call := func(body string) map[string]any {
		t.Helper()
		recorder := httptest.NewRecorder()
		connection.Writer = &jsonRPCMessageWriter{w: recorder}
		_ = protocol.HandleInbound(t.Context(), env, []byte(body))
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode %q: %v", recorder.Body.String(), err)
		}
		return response
	}

	initialize := call(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	unauthenticated := call(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	authenticated := call(`{"jsonrpc":"2.0","id":3,"method":"authenticate","params":{"methodId":"agent-login"}}`)
	created := call(`{"jsonrpc":"2.0","id":4,"method":"session/new","params":{"cwd":"/tmp"}}`)

	result := initialize["result"].(map[string]any)
	methods := result["authMethods"].([]any)
	if got := methods[0].(map[string]any)["id"]; got != "agent-login" {
		t.Fatalf("auth method id = %v", got)
	}
	if unauthenticated["error"] == nil {
		t.Fatalf("unauthenticated response = %v", unauthenticated)
	}
	if authenticated["error"] != nil || !connection.Security.Authenticated() {
		t.Fatalf("authenticated response = %v context = %#v", authenticated, connection.Security)
	}
	sessionID := created["result"].(map[string]any)["sessionId"].(string)

	bob, err := tacklrsecurity.NewPrincipal("bob")
	if err != nil {
		t.Fatal(err)
	}
	bobContext := tacklrsecurity.Context{Principal: bob}
	bobConnection := &Conn{Security: &bobContext}
	bobRecorder := httptest.NewRecorder()
	bobConnection.Writer = &jsonRPCMessageWriter{w: bobRecorder}
	bobEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: bobConnection, Security: service}
	load := `{"jsonrpc":"2.0","id":5,"method":"session/load","params":{"sessionId":"` + sessionID + `"}}`
	_ = protocol.HandleInbound(t.Context(), bobEnv, []byte(load))
	var bobResponse map[string]any
	if err := json.Unmarshal(bobRecorder.Body.Bytes(), &bobResponse); err != nil {
		t.Fatal(err)
	}
	if bobResponse["error"] == nil {
		t.Fatalf("cross-principal load = %v", bobResponse)
	}

	loggedOut := call(`{"jsonrpc":"2.0","id":6,"method":"logout","params":{}}`)
	if loggedOut["error"] != nil {
		t.Fatalf("logout = %v", loggedOut)
	}
	afterLogout := call(`{"jsonrpc":"2.0","id":7,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if afterLogout["error"] == nil {
		t.Fatalf("session/new after logout = %v", afterLogout)
	}
}

func TestServer_networkPolicyMustBeExplicit(t *testing.T) {
	server := newTestServer(t)
	err := server.ServeHTTP(t.Context(), "127.0.0.1:0")
	if !errors.Is(err, ErrNetworkSecurityPolicyRequired) {
		t.Fatalf("ServeHTTP error = %v", err)
	}
}

func TestServer_WithSecurity_authenticatesHTTPRequests(t *testing.T) {
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return tacklrsecurity.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}
	k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := NewServer(k.Runtime, k.Catalog, healthProtocol{}).WithSecurity(service, func(r *http.Request) (tacklrsecurity.Attempt, bool) {
		if token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); token != r.Header.Get("Authorization") {
			return tacklrsecurity.Attempt{Scheme: "bearer", Credential: tacklrsecurity.NewSecret([]byte(token))}, true
		}
		return tacklrsecurity.Attempt{}, false
	})

	authorized := httptest.NewRecorder()
	authorizedReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	authorizedReq.Header.Set("Authorization", "Bearer alice")
	srv.HTTPMux().ServeHTTP(authorized, authorizedReq)

	unauthorized := httptest.NewRecorder()
	srv.HTTPMux().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if authorized.Code != http.StatusOK || authorized.Body.String() != "ok" {
		t.Fatalf("authorized = %d %s", authorized.Code, authorized.Body.String())
	}
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d body = %s", unauthorized.Code, unauthorized.Body.String())
	}

	denied := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	badReq.Header.Set("Authorization", "Bearer bad")
	failAuth := NewServer(k.Runtime, k.Catalog, healthProtocol{}).WithSecurity(&tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			if string(attempt.Credential.Bytes()) == "bad" {
				return tacklrsecurity.Principal{}, errors.New("nope")
			}
			return tacklrsecurity.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}, func(r *http.Request) (tacklrsecurity.Attempt, bool) {
		if token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); token != r.Header.Get("Authorization") {
			return tacklrsecurity.Attempt{Scheme: "bearer", Credential: tacklrsecurity.NewSecret([]byte(token))}, true
		}
		return tacklrsecurity.Attempt{}, false
	})
	failAuth.HTTPMux().ServeHTTP(denied, badReq)
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("failed authenticate status = %d", denied.Code)
	}

	acp := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil)).WithSecurity(service, func(*http.Request) (tacklrsecurity.Attempt, bool) {
		return tacklrsecurity.Attempt{}, false
	})
	open := httptest.NewRecorder()
	acp.HTTPMux().ServeHTTP(open, httptest.NewRequest(http.MethodGet, "/acp", nil))
	if open.Code != http.StatusUpgradeRequired {
		t.Fatalf("unauthenticated ACP GET = %d, want upgrade required", open.Code)
	}
}
