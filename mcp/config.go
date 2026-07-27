package mcp

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

	// Env are environment variables set when launching the server, in
	// addition to the inherited process environment (stdio transport).
	Env []EnvVariable `json:"env,omitempty"`

	// URL is the endpoint URL (http and sse transports).
	URL string `json:"url,omitempty"`

	// Headers are HTTP headers included in requests to the server (http and
	// sse transports). Authorization and other credentials are carried here.
	Headers []HTTPHeader `json:"headers,omitempty"`
}
