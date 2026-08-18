package mcp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// Transport types for MCP server connections. The set mirrors the transports
// an ACP client may specify in session lifecycle requests.
const (
	// TransportStdio launches the MCP server as a subprocess and communicates
	// over stdin/stdout. It is the default when Type is empty.
	TransportStdio = "stdio"
	// TransportHTTP connects to a remote MCP server over streamable HTTP.
	TransportHTTP = "http"
	// TransportSSE connects to a remote MCP server over server-sent events.
	// Deprecated by the MCP spec; prefer TransportHTTP.
	TransportSSE = "sse"
)

// EnvVariable is a single environment variable for a stdio MCP server.
type EnvVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// HTTPHeader is a single HTTP header sent with requests to an HTTP-based MCP
// server.
type HTTPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Credentials are ephemeral values returned for a durable CredentialRef.
type Credentials struct {
	Env     []EnvVariable
	Headers []HTTPHeader
}

// CredentialResolver resolves host-owned references immediately before an MCP
// connection. Implementations should return short-lived credentials.
type CredentialResolver interface {
	ResolveMCP(context.Context, string) (Credentials, error)
}

// MCPConfig describes a single MCP server connection for the harness or ACP
// session lifecycle. It matches the ACP mcpServers wire shape: a stdio server
// sets Command/Args/Env with no Type, while HTTP and SSE servers set Type plus
// URL/Headers. Connection and tool discovery are handled internally by the
// harness — library consumers only supply configs.
type MCPConfig struct {
	// Name is a human-readable identifier for the server. It is used as the
	// tool namespace so that tools from this server are grouped together and
	// disambiguated from tools provided by other servers or the harness itself.
	Name string `json:"name"`

	// Type selects the MCP transport: TransportHTTP ("http") or
	// TransportSSE ("sse"). When empty or TransportStdio ("stdio"), the
	// server is launched as a subprocess over stdio.
	Type string `json:"type,omitempty"`

	// Command is the path to the MCP server executable (stdio transport).
	Command string `json:"command,omitempty"`

	// Args are command-line arguments passed to the server (stdio transport).
	Args []string `json:"args,omitempty"`

	// Env are explicit environment variables set when launching the server.
	// Host process variables are inherited only through host-only policy.
	Env []EnvVariable `json:"env,omitempty"`

	// URL is the endpoint URL (http and sse transports).
	URL string `json:"url,omitempty"`

	// Headers are HTTP headers included in requests to the server (http and
	// sse transports). Authorization and other credentials are carried here.
	Headers []HTTPHeader `json:"headers,omitempty"`

	// CredentialRef is a durable opaque reference resolved by the host. Inline
	// Env and Headers remain connection-scoped and are never persisted.
	CredentialRef string `json:"credentialRef,omitempty"`

	// HostEnv is a host-only allowlist of environment variable names inherited
	// by a stdio subprocess. It is never accepted from ACP JSON.
	HostEnv []string `json:"-"`

	// InheritHostEnv is a host-only trusted escape hatch. Client-supplied JSON
	// cannot enable full process environment inheritance.
	InheritHostEnv bool `json:"-"`
}

// Durable returns the non-secret topology safe for a wire session store.
func (c MCPConfig) Durable() MCPConfig {
	c.Args = append([]string(nil), c.Args...)
	c.Env = nil
	c.Headers = nil
	c.HostEnv = nil
	c.InheritHostEnv = false
	return c
}

// DurableConfigs copies MCP topology while removing inline credentials.
func DurableConfigs(configs []MCPConfig) []MCPConfig {
	if len(configs) == 0 {
		return nil
	}
	out := make([]MCPConfig, len(configs))
	for i := range configs {
		out[i] = configs[i].Durable()
	}
	return out
}

// Resolve applies a host credential reference to an ephemeral config copy.
func (c MCPConfig) Resolve(ctx context.Context, resolver CredentialResolver) (MCPConfig, error) {
	if c.CredentialRef == "" {
		return c, nil
	}
	if resolver == nil {
		return MCPConfig{}, fmt.Errorf("mcp server %q: credential resolver is required for reference %q", c.Name, c.CredentialRef)
	}
	credentials, err := resolver.ResolveMCP(ctx, c.CredentialRef)
	if err != nil {
		return MCPConfig{}, fmt.Errorf("mcp server %q: resolve credential %q: %w", c.Name, c.CredentialRef, err)
	}
	c.Env = append(append([]EnvVariable(nil), c.Env...), credentials.Env...)
	c.Headers = append(append([]HTTPHeader(nil), c.Headers...), credentials.Headers...)
	return c, nil
}

// Validate rejects incomplete transports and unsafe environment/header names.
func (c MCPConfig) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("mcp: server name is required")
	}
	switch c.Type {
	case "", TransportStdio:
		if strings.TrimSpace(c.Command) == "" {
			return fmt.Errorf("mcp server %q: command is required for stdio transport", c.Name)
		}
	case TransportHTTP, TransportSSE:
		parsed, err := url.Parse(c.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("mcp server %q: valid http(s) URL is required", c.Name)
		}
	default:
		return fmt.Errorf("mcp server %q: unsupported transport %q", c.Name, c.Type)
	}
	for _, env := range c.Env {
		if strings.TrimSpace(env.Name) == "" || strings.ContainsAny(env.Name, "=\r\n") {
			return fmt.Errorf("mcp server %q: invalid environment variable name", c.Name)
		}
	}
	for _, header := range c.Headers {
		if strings.TrimSpace(header.Name) == "" || strings.ContainsAny(header.Name, "\r\n") ||
			strings.ContainsAny(header.Value, "\r\n") {
			return fmt.Errorf("mcp server %q: invalid HTTP header", c.Name)
		}
	}
	return nil
}
