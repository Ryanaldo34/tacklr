package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tacklr "github.com/ryanaldo34/tacklr"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/vfs"
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
	registry := NewRegistry(testStore(t), "")
	connection := &Conn{}
	env := ProtocolEnv{Registry: registry, Conn: connection, Security: service}
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
	bobEnv := ProtocolEnv{Registry: registry, Conn: bobConnection, Security: service}
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
	server := NewServer(NewRegistry(testStore(t), ""), SSE)

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
	srv := NewServer(NewRegistry(testStore(t), ""), SSE).WithSecurity(service, extract)

	// Act
	anonymousCtx, anonymousStatus := srv.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), false)
	allowedCtx, allowedStatus := srv.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), true)
	authReq := httptest.NewRequest(http.MethodGet, "/", nil)
	authReq.Header.Set("Authorization", "Bearer token")
	authenticatedCtx, authenticatedStatus := srv.networkContext(t.Context(), authReq, false)
	badReq := httptest.NewRequest(http.MethodGet, "/", nil)
	badReq.Header.Set("Authorization", "Bearer bad")
	_, badStatus := srv.networkContext(t.Context(), badReq, false)

	explicit := NewServer(NewRegistry(testStore(t), ""), SSE)
	explicit.networkPolicyConfigured = true
	_, blockedStatus := explicit.networkContext(t.Context(), httptest.NewRequest(http.MethodGet, "/", nil), false)

	allowedAnonymous := NewServer(NewRegistry(testStore(t), ""), SSE).AllowAnonymousNetwork()
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

	rejecting := NewServer(NewRegistry(testStore(t), ""), SSE).WithSecurity(service, func(r *http.Request) (tacklrsecurity.Attempt, bool) {
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

	extractorless := NewServer(NewRegistry(testStore(t), ""), SSE).WithSecurity(service, nil)
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
	// Arrange
	service := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return tacklrsecurity.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "secured", IsComplete: true}
		},
	}
	srv := NewServer(newTestRegistry(testStore(t), strategy, nil), SSE).WithSecurity(service, func(r *http.Request) (tacklrsecurity.Attempt, bool) {
		if token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); token != r.Header.Get("Authorization") {
			return tacklrsecurity.Attempt{Scheme: "bearer", Credential: tacklrsecurity.NewSecret([]byte(token))}, true
		}
		return tacklrsecurity.Attempt{}, false
	})

	// Act
	authorized := httptest.NewRecorder()
	authorizedReq := newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`)))
	authorizedReq.Header.Set("Authorization", "Bearer alice")
	srv.HTTPMux().ServeHTTP(authorized, authorizedReq)

	unauthorized := httptest.NewRecorder()
	srv.HTTPMux().ServeHTTP(unauthorized, newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hi"}`))))

	// Assert
	if !strings.Contains(authorized.Body.String(), "secured") {
		t.Fatalf("authorized body = %s", authorized.Body.String())
	}
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d body = %s", unauthorized.Code, unauthorized.Body.String())
	}
}

func TestSessionSecurity_authorizeOperationAndSubject(t *testing.T) {
	// Arrange
	alice, err := tacklrsecurity.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	aliceContext := tacklrsecurity.Context{Principal: alice}
	service := &tacklrsecurity.Service{
		Authorizer: testAuthorizer(func(_ context.Context, principal tacklrsecurity.Principal, operation tacklrsecurity.Operation) error {
			if principal.Subject != "alice" || operation.Action != actionSessionLoad {
				return errors.New("denied")
			}
			return nil
		}),
	}
	authenticatedEnv := ProtocolEnv{
		Security: service,
		Conn:     &Conn{Security: &aliceContext},
	}
	unauthenticatedEnv := ProtocolEnv{
		Security: service,
		Conn:     &Conn{},
	}

	// Act
	localSubject := securitySubject(ProtocolEnv{})
	authenticatedSubject := securitySubject(authenticatedEnv)
	missingSubject := securitySubject(unauthenticatedEnv)
	authorizedErr := authorizeOperation(t.Context(), authenticatedEnv, actionSessionLoad, "session-1")
	unauthenticatedErr := authorizeOperation(t.Context(), unauthenticatedEnv, actionSessionLoad, "session-1")
	deniedErr := authorizeOperation(t.Context(), authenticatedEnv, actionSessionPrompt, "session-1")

	// Assert
	if localSubject != "local" {
		t.Fatalf("local subject = %q", localSubject)
	}
	if authenticatedSubject != "alice" {
		t.Fatalf("authenticated subject = %q", authenticatedSubject)
	}
	if missingSubject != "" {
		t.Fatalf("missing subject = %q", missingSubject)
	}
	if authorizedErr != nil {
		t.Fatal(authorizedErr)
	}
	if !errors.Is(unauthenticatedErr, ErrAuthenticationRequired) {
		t.Fatalf("unauthenticated error = %v", unauthenticatedErr)
	}
	if !errors.Is(deniedErr, ErrAuthorizationDenied) {
		t.Fatalf("denied error = %v", deniedErr)
	}
}

func TestACPProtocol_resolveOwnedWireSession_requiresAuthentication(t *testing.T) {
	// Arrange
	protocol := NewACPProtocol(nil).(*acpProtocol)
	protocol.sessions = map[string]*acpWireSession{
		"session": {owner: "alice"},
	}
	env := ProtocolEnv{
		Conn:     &Conn{},
		Security: &tacklrsecurity.Service{},
	}

	// Act
	_, loadErr := protocol.resolveOwnedWireSession(t.Context(), env, "session", actionSessionLoad)

	// Assert
	if !errors.Is(loadErr, ErrAuthenticationRequired) {
		t.Fatalf("load error = %v", loadErr)
	}
}

func TestACPProtocol_resolveOwnedWireSession_enforcesAuthorizer(t *testing.T) {
	// Arrange
	protocol := NewACPProtocol(nil).(*acpProtocol)
	protocol.sessions = map[string]*acpWireSession{
		"session": {owner: "alice"},
	}
	alice, err := tacklrsecurity.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	aliceContext := tacklrsecurity.Context{Principal: alice}
	env := ProtocolEnv{
		Conn: &Conn{Security: &aliceContext},
		Security: &tacklrsecurity.Service{
			Authorizer: testAuthorizer(func(_ context.Context, _ tacklrsecurity.Principal, operation tacklrsecurity.Operation) error {
				if operation.Action == actionSessionLoad {
					return errors.New("denied")
				}
				return nil
			}),
		},
	}

	// Act
	_, loadErr := protocol.resolveOwnedWireSession(t.Context(), env, "session", actionSessionLoad)

	// Assert
	if !errors.Is(loadErr, ErrAuthorizationDenied) {
		t.Fatalf("load error = %v", loadErr)
	}
}

func TestACPProtocol_resolveOwnedWireSession_rejectsMissingOwner(t *testing.T) {
	// Arrange
	protocol := NewACPProtocol(nil).(*acpProtocol)
	protocol.sessions = map[string]*acpWireSession{
		"session": {owner: ""},
	}
	alice, err := tacklrsecurity.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	aliceContext := tacklrsecurity.Context{Principal: alice}
	env := ProtocolEnv{
		Conn:     &Conn{Security: &aliceContext},
		Security: &tacklrsecurity.Service{},
	}

	// Act
	_, loadErr := protocol.resolveOwnedWireSession(t.Context(), env, "session", actionSessionLoad)

	// Assert
	if loadErr == nil || !strings.Contains(loadErr.Error(), "no owner") {
		t.Fatalf("load error = %v", loadErr)
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
		NewServer(NewRegistry(testStore(t), ""), SSE).WithSecurity(nil, nil)
	})
	assertPanics("AllowAnonymousNetwork nil server", func() {
		(*Server)(nil).AllowAnonymousNetwork()
	})
}

func TestConnectionRemoval_clearsEphemeralVFSCredentials(t *testing.T) {
	// Arrange
	auth := vfs.NewSessionAuth()
	if err := auth.Bind("session", vfs.Binding{
		Provider: "gdrive",
		Point:    "/drive",
		Auth:     vfs.Credential{Token: "secret"},
	}); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry(testStore(t), "", WithVFSAuth(auth))
	server := NewServer(registry, ACP)
	connection := server.Connections.Create(nil, nil)
	connection.noteSession("session")

	// Act
	server.Connections.Remove(connection.ID)

	// Assert
	if auth.HasBindings("session") {
		t.Fatal("connection removal retained VFS credentials")
	}
}
