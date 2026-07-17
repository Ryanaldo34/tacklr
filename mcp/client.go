package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	mcpjsonrpc "github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Client wraps a connection to a single MCP server. It is responsible for
// connecting, listing tools, calling tools, and closing the connection.
type Client struct {
	config  MCPConfig
	sdk     *mcpsdk.Client
	session *mcpsdk.ClientSession
}

// NewClient creates a Client for the given MCPConfig. Call Connect to
// establish the connection.
func NewClient(cfg MCPConfig) *Client {
	return &Client{config: cfg}
}

// Connect establishes a connection to the MCP server.
func (c *Client) Connect(ctx context.Context) error {
	httpClient, err := buildHTTPClient(ctx, c.config)
	if err != nil {
		return fmt.Errorf("mcp server %q: build http client: %w", c.config.Name, err)
	}

	var transport mcpsdk.Transport
	switch c.config.Transport {
	case TransportStreamable:
		transport = &mcpsdk.StreamableClientTransport{
			Endpoint:             c.config.URL,
			HTTPClient:           httpClient,
			DisableStandaloneSSE: c.config.DisableStandaloneSSE,
		}
	case TransportSSE:
		transport = &mcpsdk.SSEClientTransport{
			Endpoint:   c.config.URL,
			HTTPClient: httpClient,
		}
	default:
		return fmt.Errorf("mcp server %q: unsupported transport %q: %w", c.config.Name, c.config.Transport, ErrMCPTransportNotSupported)
	}

	c.sdk = mcpsdk.NewClient(&mcpsdk.Implementation{
		Name:    "tackle-harness",
		Version: "v1.0.0",
	}, nil)

	session, err := c.sdk.Connect(ctx, transport, nil)
	if err != nil {
		return c.interpretError("connect", err)
	}
	c.session = session
	return nil
}

// ListTools returns all tools exposed by the MCP server, following
// pagination cursors as needed.
func (c *Client) ListTools(ctx context.Context) ([]*mcpsdk.Tool, error) {
	var all []*mcpsdk.Tool
	cursor := ""
	for {
		params := &mcpsdk.ListToolsParams{}
		if cursor != "" {
			params.Cursor = cursor
		}
		res, err := c.session.ListTools(ctx, params)
		if err != nil {
			return nil, c.interpretError("list tools", err)
		}
		all = append(all, res.Tools...)
		if res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
	}
	return all, nil
}

// CallTool invokes a tool by name with the given arguments and returns the
// text content of the result. If the tool returns a structured result with no
// text content, the structured content is JSON-marshaled as the output.
func (c *Client) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	res, err := c.session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		return "", c.interpretError(fmt.Sprintf("call tool %q", name), err)
	}
	if res.IsError {
		var parts []string
		for _, content := range res.Content {
			if tc, ok := content.(*mcpsdk.TextContent); ok {
				parts = append(parts, tc.Text)
			}
		}
		return "", fmt.Errorf("mcp server %q: tool %q error: %s", c.config.Name, name, strings.Join(parts, "; "))
	}

	var text string
	for _, content := range res.Content {
		if tc, ok := content.(*mcpsdk.TextContent); ok {
			text += tc.Text
		}
	}
	if text == "" && res.StructuredContent != nil {
		b, err := json.Marshal(res.StructuredContent)
		if err != nil {
			return "", fmt.Errorf("mcp server %q: marshal structured result: %w", c.config.Name, err)
		}
		text = string(b)
	}
	return text, nil
}

// interpretError converts an opaque MCP SDK or transport error into a
// descriptive, actionable error. It unwraps typed JSON-RPC errors and HTTP
// errors and adds re-authentication hints when the failure looks auth-related.
func (c *Client) interpretError(operation string, err error) error {
	var rpcErr *mcpjsonrpc.Error
	if errors.As(err, &rpcErr) {
		suffix := ""
		if len(rpcErr.Data) > 0 {
			suffix = fmt.Sprintf(" (data: %s)", string(rpcErr.Data))
		}
		return fmt.Errorf("mcp server %q: %s: JSON-RPC error %d: %s%s: %w",
			c.config.Name, operation, rpcErr.Code, rpcErr.Message, suffix, err)
	}

	var httpErr *MCPHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.Status == http.StatusForbidden {
			return fmt.Errorf("mcp server %q: %s: request forbidden (HTTP %d): %s; re-authenticate at /auth/google with scopes %v: %w",
				c.config.Name, operation, httpErr.Status, httpErr.Body, c.config.RequiredScopes, err)
		}
		return fmt.Errorf("mcp server %q: %s: HTTP error (status %d): %s: %w",
			c.config.Name, operation, httpErr.Status, httpErr.Body, err)
	}

	if errors.Is(err, mcpsdk.ErrSessionMissing) {
		return fmt.Errorf("mcp server %q: %s: session missing; the server endpoint may be invalid or the session may have expired: %w",
			c.config.Name, operation, err)
	}
	if errors.Is(err, mcpsdk.ErrConnectionClosed) {
		return fmt.Errorf("mcp server %q: %s: connection closed: %w", c.config.Name, operation, err)
	}

	return fmt.Errorf("mcp server %q: %s: %w", c.config.Name, operation, err)
}

// Name returns the configured name of the MCP server.
func (c *Client) Name() string {
	return c.config.Name
}

// Close tears down the MCP server connection.
func (c *Client) Close() error {
	if c.session == nil {
		return nil
	}
	err := c.session.Close()
	if err == nil {
		return nil
	}
	// Google's MCP endpoints do not support the session DELETE request that
	// the streamable transport issues on close. A 405 here is benign; treat
	// it as a clean close.
	var httpErr *MCPHTTPError
	if errors.As(err, &httpErr) && httpErr.Status == http.StatusMethodNotAllowed {
		slog.Warn("mcp server close returned 405, treating as successful", "server", c.config.Name)
		return nil
	}
	return c.interpretError("close", err)
}
