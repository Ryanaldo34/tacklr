package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/vfs"
)

type testAuthenticator func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error)

func (f testAuthenticator) Authenticate(ctx context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
	return f(ctx, attempt)
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
