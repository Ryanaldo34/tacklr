package mcp_test

import (
	"context"
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
