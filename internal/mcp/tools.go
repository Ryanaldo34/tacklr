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

// DiscoverAllTools connects to each MCP server in configs and invokes register
// for every tool found. It returns a cleanup function that closes all open
// connections. Unreachable servers are logged and skipped.
func DiscoverAllTools(ctx context.Context, configs []mcp.MCPConfig, register RegisterTool) (cleanup func()) {
	var clients []*client

	for _, cfg := range configs {
		c := newClient(cfg)
		if err := c.connect(ctx); err != nil {
			slog.Warn("failed to connect to MCP server, skipping", "server", cfg.Name, "error", err)
			continue
		}

		mcpTools, err := c.listTools(ctx)
		if err != nil {
			_ = c.close()
			slog.Warn("failed to list MCP tools, skipping server", "server", cfg.Name, "error", err)
			continue
		}

		slog.Info("discovered MCP tools", "server", cfg.Name, "count", len(mcpTools))
		for _, mcpTool := range mcpTools {
			name := mcpTool.Name
			description := mcpTool.Description
			if description == "" {
				description = mcpTool.Title
			}
			schema, _ := mcpTool.InputSchema.(map[string]any)
			cl := c
			register(name, description, cfg.Name, schema, func(ctx context.Context, args map[string]any) (string, error) {
				return cl.callTool(ctx, name, args)
			})
		}
		clients = append(clients, c)
	}

	return func() {
		for _, c := range clients {
			if err := c.close(); err != nil {
				slog.Warn("failed to close MCP client", "server", c.config.Name, "error", err)
			}
		}
	}
}
