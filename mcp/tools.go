// Package mcp connects a harness to MCP servers with the official MCP Go
// SDK. It is a leaf package, so a harness that does not use MCP does not
// link the SDK.
package mcp

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/session"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Client is the session.MCPClient that connects with the official MCP Go
// SDK, over stdio, streamable HTTP or SSE. Set AgentOptions.MCP to Client{}.
// Logger reports servers that fail to connect, list or close; nil is
// slog.Default.
type Client struct {
	Logger *slog.Logger
}

// Connect implements session.MCPClient: it connects to each server and
// returns the tools of those that answered, sorted by name per server, and
// a func that closes the connections. A server that fails to connect or
// list its tools is logged and skipped.
func (m Client) Connect(ctx context.Context, servers []session.MCPServer) ([]session.MCPTool, func()) {
	log := cmp.Or(m.Logger, slog.Default())
	var clients []*client
	var tools []session.MCPTool
	for _, cfg := range servers {
		c := newClient(cfg, log)
		if err := c.connect(ctx); err != nil {
			log.Warn("failed to connect to MCP server, skipping", "server", cfg.Name, "error", err)
			continue
		}
		found, err := c.tools(ctx)
		if err != nil {
			_ = c.close()
			log.Warn("failed to list MCP tools, skipping server", "server", cfg.Name, "error", err)
			continue
		}
		tools = append(tools, found...)
		clients = append(clients, c)
	}
	return tools, func() {
		for _, c := range clients {
			if err := c.close(); err != nil {
				log.Warn("failed to close MCP client", "server", c.config.Name, "error", err)
			}
		}
	}
}

// tools lists the tools on a connected client, sorted by name.
func (c *client) tools(ctx context.Context) ([]session.MCPTool, error) {
	mcpTools, err := c.listTools(ctx)
	if err != nil {
		return nil, err
	}
	c.log.Info("discovered MCP tools", "server", c.config.Name, "count", len(mcpTools))
	slices.SortFunc(mcpTools, func(a, b *mcpsdk.Tool) int {
		return strings.Compare(a.Name, b.Name)
	})
	out := make([]session.MCPTool, 0, len(mcpTools))
	for _, t := range mcpTools {
		schema, _ := t.InputSchema.(map[string]any)
		out = append(out, session.MCPTool{
			Server:      c.config.Name,
			Name:        t.Name,
			Description: cmp.Or(t.Description, t.Title),
			Schema:      schema,
			Call: func(ctx context.Context, args map[string]any) (string, error) {
				return c.callTool(ctx, t.Name, args)
			},
		})
	}
	return out, nil
}
