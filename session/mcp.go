package session

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// MCP transports for MCPServer.Type. The set mirrors the transports
// an ACP client may specify in session lifecycle requests.
const (
	// MCPStdio launches the MCP server as a subprocess and communicates
	// over stdin/stdout. It is the default when Type is empty.
	MCPStdio = "stdio"
	// MCPHTTP connects to a remote MCP server over streamable HTTP.
	MCPHTTP = "http"
	// MCPSSE connects to a remote MCP server over server-sent events.
	// Deprecated by the MCP spec; prefer MCPHTTP.
	MCPSSE = "sse"
)

// MCPEnv is a single environment variable for a stdio MCP server.
type MCPEnv struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MCPHeader is a single HTTP header sent with requests to an HTTP-based MCP
// server.
type MCPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MCPCredentials are ephemeral values returned for a durable CredentialRef.
type MCPCredentials struct {
	Env     []MCPEnv
	Headers []MCPHeader
}

// MCPCredentialResolver resolves host-owned MCP credential references immediately before an MCP
// connection. Implementations should return short-lived credentials.
type MCPCredentialResolver interface {
	ResolveMCP(context.Context, string) (MCPCredentials, error)
}

// MCPClient connects a harness to its MCP servers. Package mcp is the
// implementation; a harness with MCP servers requires one.
type MCPClient interface {
	// Connect connects to each server and returns the tools of those that
	// answered, and a func that closes the connections. A server that
	// fails is logged and skipped.
	Connect(ctx context.Context, servers []MCPServer) ([]MCPTool, func())
}

// MCPTool is one tool an MCP server offers.
type MCPTool struct {
	// Server is the name of the server, the tool's namespace.
	Server      string
	Name        string
	Description string
	// Schema is the tool's JSON Schema for its arguments.
	Schema map[string]any
	// Call runs the tool on the server and returns its text result.
	Call func(ctx context.Context, args map[string]any) (string, error)
}

// MCPServer is one MCP server a session connects to, for the harness or
// the ACP session lifecycle. It matches the ACP mcpServers wire shape: a stdio server
// sets Command/Args/Env with no Type, while HTTP and SSE servers set Type plus
// URL/Headers. The harness connects through AgentOptions.MCP.
type MCPServer struct {
	// Name is a human-readable identifier for the server. It is used as the
	// tool namespace so that tools from this server are grouped together and
	// disambiguated from tools provided by other servers or the harness itself.
	Name string `json:"name"`

	// Type selects the MCP transport: MCPHTTP ("http") or
	// MCPSSE ("sse"). When empty or MCPStdio ("stdio"), the
	// server is launched as a subprocess over stdio.
	Type string `json:"type,omitempty"`

	// Command is the path to the MCP server executable (stdio transport).
	Command string `json:"command,omitempty"`

	// Args are command-line arguments passed to the server (stdio transport).
	Args []string `json:"args,omitempty"`

	// Env are explicit environment variables set when launching the server.
	// Host process variables are inherited only through host-only policy.
	Env []MCPEnv `json:"env,omitempty"`

	// URL is the endpoint URL (http and sse transports).
	URL string `json:"url,omitempty"`

	// Headers are HTTP headers included in requests to the server (http and
	// sse transports). Authorization and other credentials are carried here.
	Headers []MCPHeader `json:"headers,omitempty"`

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
func (c MCPServer) Durable() MCPServer {
	c.Args = append([]string(nil), c.Args...)
	c.Env = nil
	c.Headers = nil
	c.HostEnv = nil
	c.InheritHostEnv = false
	return c
}

// DurableMCPServers copies MCP server topology while removing inline credentials.
func DurableMCPServers(configs []MCPServer) []MCPServer {
	if len(configs) == 0 {
		return nil
	}
	out := make([]MCPServer, len(configs))
	for i := range configs {
		out[i] = configs[i].Durable()
	}
	return out
}

// Resolve applies a host credential reference to an ephemeral config copy.
func (c MCPServer) Resolve(ctx context.Context, resolver MCPCredentialResolver) (MCPServer, error) {
	if c.CredentialRef == "" {
		return c, nil
	}
	if resolver == nil {
		return MCPServer{}, fmt.Errorf("mcp server %q: credential resolver is required for reference %q", c.Name, c.CredentialRef)
	}
	credentials, err := resolver.ResolveMCP(ctx, c.CredentialRef)
	if err != nil {
		return MCPServer{}, fmt.Errorf("mcp server %q: resolve credential %q: %w", c.Name, c.CredentialRef, err)
	}
	c.Env = append(append([]MCPEnv(nil), c.Env...), credentials.Env...)
	c.Headers = append(append([]MCPHeader(nil), c.Headers...), credentials.Headers...)
	return c, nil
}

// Validate rejects incomplete transports and unsafe environment/header names.
func (c MCPServer) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("mcp: server name is required")
	}
	switch c.Type {
	case "", MCPStdio:
		if strings.TrimSpace(c.Command) == "" {
			return fmt.Errorf("mcp server %q: command is required for stdio transport", c.Name)
		}
	case MCPHTTP, MCPSSE:
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
