package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
)

// TestACPAuthentication_ownsSessionsByGenericPrincipal: a client must
// authenticate through the advertised method before it makes a session;
// the session is its principal's, so a connection of another principal
// cannot load it; and after logout the client cannot make one.
func TestACPAuthentication_ownsSessionsByGenericPrincipal(t *testing.T) {
	t.Parallel()
	service := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			switch attempt.Scheme {
			case "host-login":
				return server.NewPrincipal("alice")
			case "bearer":
				return server.NewPrincipal(string(attempt.Credential.Bytes()))
			}
			return server.Principal{}, errors.New("unexpected scheme")
		}),
	}
	protocol := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{
		ID:          "agent-login",
		Name:        "Agent login",
		Description: "Authenticate with the host",
		Scheme:      "host-login",
	}}, true)
	url := secured(t, memoryHost(t), protocol, service)
	c := acptest.Dial(t, url, nil)

	initialize := c.Call("initialize", `{"protocolVersion":1,"clientCapabilities":{}}`)
	unauthenticated := c.Call("session/new", `{"cwd":"/tmp"}`)
	authenticated := c.Call("authenticate", `{"methodId":"agent-login"}`)
	created := c.Call("session/new", `{"cwd":"/tmp"}`)
	sessionID, _ := created["result"].(map[string]any)["sessionId"].(string)
	bobLoad := connectAs(t, url, "bob").Call("session/load", `{"sessionId":"`+sessionID+`"}`)
	aliceLoad := connectAs(t, url, "alice").Call("session/load", `{"sessionId":"`+sessionID+`"}`)
	loggedOut := c.Call("logout", `{}`)
	afterLogout := c.Call("session/new", `{"cwd":"/tmp"}`)

	methods := initialize["result"].(map[string]any)["authMethods"].([]any)
	if got := methods[0].(map[string]any)["id"]; got != "agent-login" {
		t.Fatalf("auth method id = %v", got)
	}
	if unauthenticated["error"] == nil || authenticated["error"] != nil || sessionID == "" {
		t.Fatalf("before = %v, authenticate = %v, then %v; want refused, then a session", unauthenticated, authenticated, created)
	}
	if bobLoad["error"] == nil || aliceLoad["error"] != nil {
		t.Fatalf("bob's load = %v, alice's = %v; want bob refused and alice served", bobLoad, aliceLoad)
	}
	if loggedOut["error"] != nil || afterLogout["error"] == nil {
		t.Fatalf("logout = %v, after = %v; want the logged-out client refused", loggedOut, afterLogout)
	}
}

// TestServer_WithSecurity_admitsConnections: a connection whose request
// carries a credential the host refuses gets 401, and one with no
// credential opens anonymous; a GET that does not upgrade is asked to,
// and an upgrade with no WebSocket version is a bad request; a platform's
// request for a path no protocol serves is not found.
func TestServer_WithSecurity_admitsConnections(t *testing.T) {
	t.Parallel()
	service := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if string(attempt.Credential.Bytes()) == "bad" {
				return server.Principal{}, errors.New("nope")
			}
			return server.NewPrincipal(string(attempt.Credential.Bytes()))
		}),
	}
	k := memoryHost(t)
	srv := server.NewServer(k.Client, k.Agent, acp.New(nil)).WithSecurity(service, bearerOf)
	url := acptest.Listen(t, srv)
	serve := func(header ...string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/acp", nil)
		for i := 0; i < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		srv.HTTPMux().ServeHTTP(rec, req)
		return rec.Code
	}

	_, bad, badErr := websocket.Dial(t.Context(), url, &websocket.DialOptions{HTTPHeader: bearer("bad")})
	opened := acptest.Connect(t, url, `{"protocolVersion":1}`).Call("session/new", `{"cwd":"/tmp"}`)
	plainGet := serve()
	noVersion := serve("Connection", "Upgrade", "Upgrade", "websocket")
	_, elsewhere := srv.Connect(httptest.NewRequest(http.MethodGet, "/elsewhere", nil))

	if badErr == nil || bad == nil || bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a refused credential = %v (%v), want 401", bad, badErr)
	}
	if msg := acptest.Message(t, opened); !strings.Contains(msg, "authentication required") {
		t.Fatalf("session/new on an anonymous connection = %q, want authentication required", msg)
	}
	if plainGet != http.StatusUpgradeRequired || noVersion != http.StatusBadRequest || elsewhere != http.StatusNotFound {
		t.Fatalf("plain GET = %d, upgrade with no version = %d, other path = %d; want upgrade required, bad request, not found", plainGet, noVersion, elsewhere)
	}
}

// TestServer_panicsOnAConfigurationItCannotServe: a server with no session
// client or no protocol, security with no service, and ACP auth methods
// with no id, no name or the same id twice are programming errors of the
// host, so each panics.
func TestServer_panicsOnAConfigurationItCannotServe(t *testing.T) {
	t.Parallel()
	k := memoryHost(t)
	for name, configure := range map[string]func(){
		"no session client": func() { server.NewServer(nil, k.Agent, acp.New(nil)) },
		"no protocol":       func() { server.NewServer(k.Client, k.Agent) },
		"no security":       func() { server.NewServer(k.Client, k.Agent, acp.New(nil)).WithSecurity(nil, nil) },
		"auth method no id": func() { acp.NewWithAuth(nil, []acp.ACPAuthMethod{{Name: "Login"}}, false) },
		"auth method twice": func() {
			acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login"}, {ID: "login", Name: "Again"}}, false)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("configured, want a panic")
				}
			}()

			configure()
		})
	}
}
