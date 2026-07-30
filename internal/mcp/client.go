package mcpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"

	mcpjsonrpc "github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr/mcp"
)

// toolSession is the subset of MCP session methods used after connect.
// Tests inject fakes for pagination, structured content, and close behavior.
type toolSession interface {
	ListTools(ctx context.Context, params *mcpsdk.ListToolsParams) (*mcpsdk.ListToolsResult, error)
	CallTool(ctx context.Context, params *mcpsdk.CallToolParams) (*mcpsdk.CallToolResult, error)
	Close() error
}

// client wraps a connection to a single MCP server.
type client struct {
	config  mcp.MCPConfig
	sdk     *mcpsdk.Client
	session toolSession
}

func newClient(cfg mcp.MCPConfig) *client {
	return &client{config: cfg}
}

func (c *client) connect(ctx context.Context) error {
	transport, err := c.buildTransport()
	if err != nil {
		return err
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

func (c *client) buildTransport() (mcpsdk.Transport, error) {
	switch c.config.Type {
	case "", mcp.TransportStdio:
		if c.config.Command == "" {
			return nil, fmt.Errorf("mcp server %q: command is required for stdio transport", c.config.Name)
		}
		cmd := exec.Command(c.config.Command, c.config.Args...)
		if len(c.config.Env) > 0 {
			cmd.Env = os.Environ()
			for _, env := range c.config.Env {
				cmd.Env = append(cmd.Env, env.Name+"="+env.Value)
			}
		}
		return &mcpsdk.CommandTransport{Command: cmd}, nil
	case mcp.TransportHTTP:
		if c.config.URL == "" {
			return nil, fmt.Errorf("mcp server %q: url is required for http transport", c.config.Name)
		}
		return &mcpsdk.StreamableClientTransport{
			Endpoint:   c.config.URL,
			HTTPClient: buildHTTPClient(c.config),
		}, nil
	case mcp.TransportSSE:
		if c.config.URL == "" {
			return nil, fmt.Errorf("mcp server %q: url is required for sse transport", c.config.Name)
		}
		return &mcpsdk.SSEClientTransport{
			Endpoint:   c.config.URL,
			HTTPClient: buildHTTPClient(c.config),
		}, nil
	default:
		return nil, fmt.Errorf("mcp server %q: unsupported transport %q: %w", c.config.Name, c.config.Type, errTransportNotSupported)
	}
}

func (c *client) listTools(ctx context.Context) ([]*mcpsdk.Tool, error) {
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

func (c *client) callTool(ctx context.Context, name string, args map[string]any) (string, error) {
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

func (c *client) interpretError(operation string, err error) error {
	var rpcErr *mcpjsonrpc.Error
	if errors.As(err, &rpcErr) {
		suffix := ""
		if len(rpcErr.Data) > 0 {
			suffix = fmt.Sprintf(" (data: %s)", string(rpcErr.Data))
		}
		return fmt.Errorf("mcp server %q: %s: JSON-RPC error %d: %s%s: %w",
			c.config.Name, operation, rpcErr.Code, rpcErr.Message, suffix, err)
	}

	var httpErr *httpError
	if errors.As(err, &httpErr) {
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

func (c *client) close() error {
	if c.session == nil {
		return nil
	}
	err := c.session.Close()
	if err == nil {
		return nil
	}
	// Some MCP endpoints do not support the session DELETE request that the
	// streamable transport issues on close. A 405 here is benign; treat it as
	// a clean close.
	var httpErr *httpError
	if errors.As(err, &httpErr) && httpErr.Status == http.StatusMethodNotAllowed {
		slog.Warn("mcp server close returned 405, treating as successful", "server", c.config.Name)
		return nil
	}
	return c.interpretError("close", err)
}
