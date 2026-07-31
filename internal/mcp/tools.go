package mcpruntime

import (
	"context"
	"log/slog"

	"github.com/ryanaldo34/tacklr/mcp"
)

// ToolHandler is the call signature for a discovered MCP tool.
type ToolHandler func(ctx context.Context, args map[string]any) (string, error)

// RegisterTool is called once per tool discovered on a connected MCP server.
type RegisterTool func(name, description, namespace string, schema map[string]any, handler ToolHandler)

// connectClient dials an MCP server. Overridden in tests to inject fake sessions.
var connectClient = func(ctx context.Context, c *client) error {
	return c.connect(ctx)
}

// DiscoverAllTools connects to each MCP server in configs and invokes register
// for every tool found. It returns a cleanup function that closes all open
// connections. Unreachable servers are logged and skipped.
func DiscoverAllTools(ctx context.Context, configs []mcp.MCPConfig, register RegisterTool) (cleanup func()) {
	var clients []*client

	for _, cfg := range configs {
		c := newClient(cfg)
		if err := connectClient(ctx, c); err != nil {
			slog.Warn("failed to connect to MCP server, skipping", "server", cfg.Name, "error", err)
			continue
		}

		if err := registerClientTools(ctx, c, cfg.Name, register); err != nil {
			_ = c.close()
			slog.Warn("failed to list MCP tools, skipping server", "server", cfg.Name, "error", err)
			continue
		}
		clients = append(clients, c)
	}

	return func() {
		closeClients(clients)
	}
}

func closeClients(clients []*client) {
	for _, c := range clients {
		if err := c.close(); err != nil {
			slog.Warn("failed to close MCP client", "server", c.config.Name, "error", err)
		}
	}
}

// registerClientTools lists tools on a connected client and registers each one.
func registerClientTools(ctx context.Context, c *client, namespace string, register RegisterTool) error {
	mcpTools, err := c.listTools(ctx)
	if err != nil {
		return err
	}
	slog.Info("discovered MCP tools", "server", c.config.Name, "count", len(mcpTools))
	for _, mcpTool := range mcpTools {
		name := mcpTool.Name
		description := mcpTool.Description
		if description == "" {
			description = mcpTool.Title
		}
		schema, _ := mcpTool.InputSchema.(map[string]any)
		cl := c
		register(name, description, namespace, schema, func(ctx context.Context, args map[string]any) (string, error) {
			return cl.callTool(ctx, name, args)
		})
	}
	return nil
}
