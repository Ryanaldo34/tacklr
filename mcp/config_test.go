package mcp_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/mcp"
)

type credentialResolverFunc func(context.Context, string) (mcp.Credentials, error)

func (f credentialResolverFunc) ResolveMCP(ctx context.Context, ref string) (mcp.Credentials, error) {
	return f(ctx, ref)
}

func TestMCPConfig_durableTopologyResolvesEphemeralCredentials(t *testing.T) {
	// Arrange
	config := mcp.MCPConfig{
		Name:          "remote",
		Type:          mcp.TransportHTTP,
		URL:           "https://example.test/mcp",
		Headers:       []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		CredentialRef: "vault://remote",
	}
	resolver := credentialResolverFunc(func(_ context.Context, ref string) (mcp.Credentials, error) {
		if ref != "vault://remote" {
			t.Fatalf("reference = %q", ref)
		}
		return mcp.Credentials{
			Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer resolved"}},
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

func TestMCPConfig_validateRejectsUnsafeOrIncompleteDefinitions(t *testing.T) {
	// Arrange
	cases := []mcp.MCPConfig{
		{},
		{Name: "stdio"},
		{Name: "remote", Type: mcp.TransportHTTP, URL: "file:///tmp/socket"},
		{Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test", Headers: []mcp.HTTPHeader{{Name: "X-Test\r\nInjected", Value: "x"}}},
		{Name: "stdio", Command: "server", Env: []mcp.EnvVariable{{Name: "BAD=NAME", Value: "x"}}},
	}

	// Act and assert
	for i, config := range cases {
		if err := config.Validate(); err == nil {
			t.Fatalf("case %d was accepted: %#v", i, config)
		}
	}
	if err := (mcp.MCPConfig{Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test"}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMCPConfig_resolveRequiresHostResolver(t *testing.T) {
	// Act
	_, err := (mcp.MCPConfig{Name: "remote", CredentialRef: "vault://remote"}).Resolve(t.Context(), nil)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "credential resolver is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestMCPConfig_validateRejectsUnsupportedTransport(t *testing.T) {
	// Act
	err := (mcp.MCPConfig{Name: "remote", Type: "websocket", URL: "https://example.test"}).Validate()

	// Assert
	if err == nil || !strings.Contains(err.Error(), "unsupported transport") {
		t.Fatalf("error = %v", err)
	}
}

func TestMCPConfig_resolveReportsResolverFailure(t *testing.T) {
	// Arrange
	resolver := credentialResolverFunc(func(context.Context, string) (mcp.Credentials, error) {
		return mcp.Credentials{}, fmt.Errorf("vault unavailable")
	})

	// Act
	_, err := (mcp.MCPConfig{
		Name:          "remote",
		Type:          mcp.TransportHTTP,
		URL:           "https://example.test",
		CredentialRef: "vault://remote",
	}).Resolve(t.Context(), resolver)

	// Assert
	if err == nil || !strings.Contains(err.Error(), "vault unavailable") {
		t.Fatalf("error = %v", err)
	}
}

func TestDurableConfigs_emptyReturnsNil(t *testing.T) {
	// Act
	durable := mcp.DurableConfigs(nil)

	// Assert
	if durable != nil {
		t.Fatalf("durable = %#v", durable)
	}
}

func TestDurableConfigs_stripsInlineCredentialsFromEachConfig(t *testing.T) {
	// Arrange
	configs := []mcp.MCPConfig{
		{
			Name:    "inline",
			Type:    mcp.TransportHTTP,
			URL:     "https://example.test/a",
			Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		},
		{
			Name:          "vault",
			Type:          mcp.TransportHTTP,
			URL:           "https://example.test/b",
			CredentialRef: "vault://remote",
			Headers:       []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer inline"}},
		},
	}

	// Act
	durable := mcp.DurableConfigs(configs)

	// Assert
	if len(durable) != 2 || len(durable[0].Headers) != 0 || len(durable[1].Headers) != 0 {
		t.Fatalf("durable configs = %#v", durable)
	}
	if durable[1].CredentialRef != "vault://remote" {
		t.Fatalf("credential ref = %q", durable[1].CredentialRef)
	}
}
