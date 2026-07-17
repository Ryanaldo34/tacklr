package mcp

const (
	TransportStreamable = "streamable"
	TransportSSE        = "sse"
)

// MCPConfig describes a single MCP server connection. The harness uses it to
// connect to the server, discover its tools, and expose them to the LLM as
// namespaced tools.
type MCPConfig struct {
	// Name is a human-readable identifier for the server. It is used as the
	// tool namespace so that tools from this server are grouped together and
	// disambiguated from tools provided by other servers or the harness itself.
	Name string `json:"name"`

	// Transport selects the MCP transport. Valid values are
	// TransportStreamable ("streamable") and TransportSSE ("sse").
	Transport string `json:"transport"`

	// URL is the endpoint URL for HTTP-based transports (streamable and SSE).
	URL string `json:"url"`

	// AuthRequired indicates whether the MCP server requires authentication.
	// When true, either AuthToken or OAuthURL must be provided.
	AuthRequired bool `json:"authRequired"`

	// AuthToken is a pre-obtained bearer token sent in the Authorization
	// header. It is not serialized to JSON (json:"-") to avoid leaking
	// secrets into session state. Use this for simple bearer-token auth.
	AuthToken string `json:"-"`

	// OAuthURL is the token endpoint URL for the OAuth 2.0 client credentials
	// flow. When set (along with OAuthClientID and OAuthClientSecret), the
	// harness automatically acquires and refreshes access tokens.
	OAuthURL string `json:"oauthUrl,omitempty"`

	// OAuthClientID is the client ID for the OAuth client credentials flow.
	OAuthClientID string `json:"oauthClientId,omitempty"`

	// OAuthClientSecret is the client secret for the OAuth client credentials
	// flow. Not serialized to JSON to avoid leaking secrets.
	OAuthClientSecret string `json:"-"`

	// OAuthScopes is the list of scopes to request when acquiring an OAuth
	// token via the client credentials flow.
	OAuthScopes []string `json:"oauthScopes,omitempty"`

	// RequiredScopes are the OAuth scopes the user's access token must
	// include for the MCP server to accept calls (e.g. "openid",
	// "https://www.googleapis.com/auth/gmail.readonly"). When AuthRequired
	// is true and the user's token does not include all of these, the MCP
	// server will return 403 Forbidden. Consumers should request these
	// scopes (union) when initiating an authorization code flow.
	RequiredScopes []string `json:"requiredScopes,omitempty"`

	// Headers is an optional set of custom HTTP headers appended to every
	// request to the MCP server.
	Headers map[string]string `json:"headers,omitempty"`

	// DisableStandaloneSSE controls whether the streamable transport opens a
	// separate HTTP GET request to establish a server-sent events stream for
	// server-initiated messages.
	//
	// When false (the default), the streamable transport opens the SSE stream.
	// Some servers (e.g. Google's MCP endpoints) reject the GET request with
	// HTTP 405, in which case this should be set to true. The trade-off is that
	// server-initiated notifications such as ToolListChangedNotification will
	// not be received.
	DisableStandaloneSSE bool `json:"disableStandaloneSSE,omitempty"`
}
