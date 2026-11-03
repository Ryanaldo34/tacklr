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

func TestMCPServer_durableTopologyResolvesEphemeralCredentials(t *testing.T) {
	// Arrange
	config := session.MCPServer{
		Name:          "remote",
		Type:          session.MCPHTTP,
		URL:           "https://example.test/mcp",
		Headers:       []session.MCPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		CredentialRef: "vault://remote",
	}
	resolver := credentialResolverFunc(func(_ context.Context, ref string) (session.MCPCredentials, error) {
		if ref != "vault://remote" {
			t.Fatalf("reference = %q", ref)
		}
		return session.MCPCredentials{
			Headers: []session.MCPHeader{{Name: "Authorization", Value: "Bearer resolved"}},
		}, nil
	})

	// Act
	durable := config.Durable()
	resolved, err := durable.Resolve(t.Context(), resolver)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if len(durable.Headers) != 0 || len(durable.Env) != 0 {
		t.Fatalf("durable credentials = %#v %#v", durable.Headers, durable.Env)
	}
	if durable.CredentialRef != "vault://remote" {
		t.Fatalf("credential reference = %q", durable.CredentialRef)
	}
	if len(resolved.Headers) != 1 || resolved.Headers[0].Value != "Bearer resolved" {
		t.Fatalf("resolved headers = %#v", resolved.Headers)
	}
}

func TestMCPServer_validateRejectsUnsafeOrIncompleteDefinitions(t *testing.T) {
	// Arrange
	cases := []session.MCPServer{
		{},
		{Name: "stdio"},
		{Name: "remote", Type: session.MCPHTTP, URL: "file:///tmp/socket"},
		{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test", Headers: []session.MCPHeader{{Name: "X-Test\r\nInjected", Value: "x"}}},
		{Name: "stdio", Command: "server", Env: []session.MCPEnv{{Name: "BAD=NAME", Value: "x"}}},
	}

	// Act and assert
	for i, config := range cases {
		if err := config.Validate(); err == nil {
			t.Fatalf("case %d was accepted: %#v", i, config)
		}
	}
	if err := (session.MCPServer{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test"}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMCPServer_resolveRequiresHostResolver(t *testing.T) {
	// Act
	_, err := (session.MCPServer{Name: "remote", CredentialRef: "vault://remote"}).Resolve(t.Context(), nil)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "credential resolver is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestMCPServer_validateRejectsUnsupportedTransport(t *testing.T) {
	// Act
	err := (session.MCPServer{Name: "remote", Type: "websocket", URL: "https://example.test"}).Validate()

	// Assert
	if err == nil || !strings.Contains(err.Error(), "unsupported transport") {
		t.Fatalf("error = %v", err)
	}
}

func TestDurableConfigs_stripsInlineCredentialsFromEachConfig(t *testing.T) {
	// Arrange
	configs := []session.MCPServer{
		{
			Name:    "inline",
			Type:    session.MCPHTTP,
			URL:     "https://example.test/a",
			Headers: []session.MCPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		},
		{
			Name:          "vault",
			Type:          session.MCPHTTP,
			URL:           "https://example.test/b",
			CredentialRef: "vault://remote",
			Headers:       []session.MCPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		},
	}

	// Act
	durable := session.DurableMCPServers(configs)

	// Assert
	if len(durable) != 2 || len(durable[0].Headers) != 0 || len(durable[1].Headers) != 0 {
		t.Fatalf("durable configs = %#v", durable)
	}
	if durable[1].CredentialRef != "vault://remote" {
		t.Fatalf("credential ref = %q", durable[1].CredentialRef)
	}
}
