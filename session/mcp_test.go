package session_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

type credentialResolverFunc func(context.Context, string) (session.MCPCredentials, error)

func (f credentialResolverFunc) ResolveMCP(ctx context.Context, ref string) (session.MCPCredentials, error) {
	return f(ctx, ref)
}

// Durable copies of MCP servers keep only a credential reference, and a
// copy resolves its credentials through the host's resolver, which it needs.
func TestMCPServer_durableCopiesHoldNoInlineCredentialsAndResolveThroughTheHost(t *testing.T) {
	// Arrange
	vault := session.MCPServer{
		Name:          "remote",
		Type:          session.MCPHTTP,
		URL:           "https://example.test/mcp",
		Headers:       []session.MCPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		CredentialRef: "vault://remote",
	}
	inline := session.MCPServer{Name: "inline", Type: session.MCPHTTP, URL: "https://example.test/a",
		Headers: []session.MCPHeader{{Name: "Authorization", Value: "Bearer inline"}}}
	resolver := credentialResolverFunc(func(_ context.Context, ref string) (session.MCPCredentials, error) {
		return session.MCPCredentials{Headers: []session.MCPHeader{{Name: "Authorization", Value: "Bearer " + ref}}}, nil
	})

	// Act
	durable := session.DurableMCPServers([]session.MCPServer{inline, vault})
	resolved, err := durable[1].Resolve(t.Context(), resolver)
	_, noResolver := durable[1].Resolve(t.Context(), nil)

	// Assert
	if len(durable) != 2 || len(durable[0].Headers) != 0 || len(durable[1].Headers) != 0 || len(durable[1].Env) != 0 || durable[1].CredentialRef != "vault://remote" {
		t.Fatalf("durable copies = %#v, want no inline credentials and the reference kept", durable)
	}
	if err != nil || len(resolved.Headers) != 1 || resolved.Headers[0].Value != "Bearer vault://remote" {
		t.Fatalf("resolved = %#v %v, want the host's credentials", resolved.Headers, err)
	}
	if noResolver == nil || !strings.Contains(noResolver.Error(), "credential resolver is required") {
		t.Fatalf("resolve without a resolver = %v", noResolver)
	}
}

func TestMCPServer_validateRejectsUnsafeOrIncompleteDefinitions(t *testing.T) {
	// Arrange
	cases := map[string]session.MCPServer{
		"no name":                  {},
		"stdio without command":    {Name: "stdio"},
		"a non-network url":        {Name: "remote", Type: session.MCPHTTP, URL: "file:///tmp/socket"},
		"a header with CRLF":       {Name: "remote", Type: session.MCPHTTP, URL: "https://example.test", Headers: []session.MCPHeader{{Name: "X-Test\r\nInjected", Value: "x"}}},
		"an env name with =":       {Name: "stdio", Command: "server", Env: []session.MCPEnv{{Name: "BAD=NAME", Value: "x"}}},
		"an unsupported transport": {Name: "remote", Type: "websocket", URL: "https://example.test"},
	}

	// Act and assert
	for name, config := range cases {
		if err := config.Validate(); err == nil {
			t.Errorf("%s was accepted: %#v", name, config)
		}
	}
	if err := (session.MCPServer{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
