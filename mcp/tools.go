package mcp

import (
	"context"
	"log/slog"
)

// DiscoveredTool is the harness-agnostic representation of a tool discovered
// on an MCP server. The harness package wraps it as a *tacklr.Tool.
type DiscoveredTool struct {
	Name        string
	Description string
	Schema      map[string]any
	Namespace   string
	CallFunc    func(ctx context.Context, args map[string]any) (string, error)
}

// DiscoverAllTools connects to each MCP server in configs, discovers their
// tools, and returns the combined DiscoveredTool list plus all clients that
// must be closed later.
//
// Servers that are unreachable or return errors are logged and skipped; their
// tools are not included but the remaining servers' tools are still returned.
func DiscoverAllTools(ctx context.Context, configs []MCPConfig) ([]DiscoveredTool, []*Client) {
	var allTools []DiscoveredTool
	var clients []*Client

	for _, cfg := range configs {
		client := NewClient(cfg)
		if err := client.Connect(ctx); err != nil {
			slog.Warn("failed to connect to MCP server, skipping", "server", cfg.Name, "error", err)
			continue
		}

		mcpTools, err := client.ListTools(ctx)
		if err != nil {
			_ = client.Close()
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
			allTools = append(allTools, DiscoveredTool{
				Name:        name,
				Description: description,
				Schema:      schema,
				Namespace:   cfg.Name,
				CallFunc: func(ctx context.Context, args map[string]any) (string, error) {
					return client.CallTool(ctx, name, args)
				},
			})
		}
		clients = append(clients, client)
	}

	return allTools, clients
}
