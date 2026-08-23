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
	// Arrange
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
	k := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})
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

	// Act
	initialize := call(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	unauthenticated := call(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	authenticated := call(`{"jsonrpc":"2.0","id":3,"method":"authenticate","params":{"methodId":"agent-login"}}`)
	created := call(`{"jsonrpc":"2.0","id":4,"method":"session/new","params":{"cwd":"/tmp"}}`)

	// Assert
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
}

func TestServer_networkPolicyMustBeExplicit(t *testing.T) {
	// Arrange
	server := newTestServer(t)

	// Act
	err := server.ServeHTTP(t.Context(), "127.0.0.1:0")

	// Assert
	if !errors.Is(err, ErrNetworkSecurityPolicyRequired) {
		t.Fatalf("ServeHTTP error = %v", err)
	}
}

func TestServer_networkContext_enforcesExplicitPolicy(t *testing.T) {
	// Arrange
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			if string(attempt.Credential.Bytes()) != "token" {
				return tacklrsecurity.Principal{}, errors.New("bad token")
			}
			return tacklrsecurity.NewPrincipal("alice")
		}),
	}
	extract := func(r *http.Request) (tacklrsecurity.Attempt, bool) {
		if r.Header.Get("Authorization") == "Bearer token" {
			return tacklrsecurity.Attempt{
				Scheme:     "bearer",
				Credential: tacklrsecurity.NewSecret([]byte("token")),
			}, true
		}
		return tacklrsecurity.Attempt{}, false
	}
	srv := newTestServer(t).WithSecurity(service, extract)

	// Act
	anonymousCtx, anonymousStatus := srv.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), false)
	allowedCtx, allowedStatus := srv.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), true)
	authReq := httptest.NewRequest(http.MethodGet, "/", nil)
	authReq.Header.Set("Authorization", "Bearer token")
	authenticatedCtx, authenticatedStatus := srv.networkContext(t.Context(), authReq, false)
	badReq := httptest.NewRequest(http.MethodGet, "/", nil)
	badReq.Header.Set("Authorization", "Bearer bad")
	_, badStatus := srv.networkContext(t.Context(), badReq, false)

	explicit := newTestServer(t)
	explicit.networkPolicyConfigured = true
	_, blockedStatus := explicit.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), false)

	allowedAnonymous := newTestServer(t).AllowAnonymousNetwork()
	anonCtx, anonStatus := allowedAnonymous.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), false)

	// Assert
	if anonymousStatus != http.StatusUnauthorized || anonymousCtx != nil {
		t.Fatalf("anonymous status = %d context = %#v", anonymousStatus, anonymousCtx)
	}
	if allowedStatus != 0 || allowedCtx == nil || allowedCtx.Authenticated() {
		t.Fatalf("allowed status = %d context = %#v", allowedStatus, allowedCtx)
	}
	if authenticatedStatus != 0 || authenticatedCtx == nil || authenticatedCtx.Principal.Subject != "alice" {
		t.Fatalf("authenticated status = %d context = %#v", authenticatedStatus, authenticatedCtx)
	}
	if badStatus != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d", badStatus)
	}

	rejecting := newTestServer(t).WithSecurity(service, func(r *http.Request) (tacklrsecurity.Attempt, bool) {
		if r.Header.Get("Authorization") != "" {
			return tacklrsecurity.Attempt{
				Scheme:     "bearer",
				Credential: tacklrsecurity.NewSecret([]byte("rejected")),
			}, true
		}
		return tacklrsecurity.Attempt{}, false
	})
	rejectReq := httptest.NewRequest(http.MethodGet, "/", nil)
	rejectReq.Header.Set("Authorization", "Bearer rejected")
	_, rejectedStatus := rejecting.networkContext(t.Context(), rejectReq, false)
	if rejectedStatus != http.StatusUnauthorized {
		t.Fatalf("rejected credential status = %d", rejectedStatus)
	}

	extractorless := newTestServer(t).WithSecurity(service, nil)
	_, extractorlessStatus := extractorless.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), true)
	if extractorlessStatus != 0 {
		t.Fatalf("extractorless allowed status = %d", extractorlessStatus)
	}

	if blockedStatus != http.StatusServiceUnavailable {
		t.Fatalf("blocked status = %d", blockedStatus)
	}
	if anonStatus != 0 || anonCtx == nil || anonCtx.Principal.Subject != "anonymous" {
		t.Fatalf("anonymous network status = %d context = %#v", anonStatus, anonCtx)
	}
}

func TestServer_WithSecurity_authenticatesHTTPRequests(t *testing.T) {
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return tacklrsecurity.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}
	k := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})
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
}

func TestACP_sessionLoad_authorizerDeniesAndUnauthenticated(t *testing.T) {
	alice, err := tacklrsecurity.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return alice, nil
		}),
		Authorizer: testAuthorizer(func(_ context.Context, _ tacklrsecurity.Principal, operation tacklrsecurity.Operation) error {
			if operation.Action == actionSessionLoad {
				return errors.New("denied")
			}
			return nil
		}),
	}
	protocol := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
	k := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	aliceContext := tacklrsecurity.Context{Principal: alice}
	conn := &Conn{Security: &aliceContext}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: conn, Security: service}
	call := func(body string) map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		conn.Writer = &jsonRPCMessageWriter{w: rec}
		_ = protocol.HandleInbound(t.Context(), env, []byte(body))
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
		return response
	}

	created := call(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := created["result"].(map[string]any)["sessionId"].(string)
	denied := call(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `"}}`)
	if denied["error"] == nil {
		t.Fatalf("authorizer deny = %v", denied)
	}

	unauthConn := &Conn{}
	unauthRec := httptest.NewRecorder()
	unauthConn.Writer = &jsonRPCMessageWriter{w: unauthRec}
	unauthEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: unauthConn, Security: service}
	_ = protocol.HandleInbound(t.Context(), unauthEnv, []byte(`{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"`+sessionID+`"}}`))
	var unauth map[string]any
	if err := json.Unmarshal(unauthRec.Body.Bytes(), &unauth); err != nil {
		t.Fatal(err)
	}
	if unauth["error"] == nil {
		t.Fatalf("unauthenticated load = %v", unauth)
	}
}

func TestServer_securityBuildersPanicOnInvalidReceiverOrService(t *testing.T) {
	assertPanics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s did not panic", name)
			}
		}()
		fn()
	}

	assertPanics("WithSecurity nil server", func() {
		(*Server)(nil).WithSecurity(&tacklrsecurity.Service{}, nil)
	})
	assertPanics("WithSecurity nil service", func() {
		k := newEmptyKernel()
		NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil)).WithSecurity(nil, nil)
	})
	assertPanics("AllowAnonymousNetwork nil server", func() {
		(*Server)(nil).AllowAnonymousNetwork()
	})
}

func TestConnectionRemoval_unregistersConnection(t *testing.T) {
	k := newTestKernel(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	server := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil))
	connection := server.Connections.Create(nil, nil)
	connection.noteSession("session")

	server.Connections.Remove(connection.ID)

	if got := server.Connections.Get(connection.ID); got != nil {
		t.Fatal("connection still registered after Remove")
	}
}
