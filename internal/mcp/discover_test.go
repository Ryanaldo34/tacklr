package mcpruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr/mcp"
)

func TestDiscoverAllTools_listFailureSkipsServer(t *testing.T) {
	// Server that accepts TCP but is not MCP — connect or list fails and is skipped.
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("nope"))
	}))
	t.Cleanup(hs.Close)

	var names []string
	cleanup := DiscoverAllTools(context.Background(), []mcp.MCPConfig{
		{Type: mcp.TransportHTTP, Name: "broken", URL: hs.URL},
	}, func(name, description, namespace string, schema map[string]any, handler ToolHandler) {
		names = append(names, name)
	})
	t.Cleanup(cleanup)
	if len(names) != 0 {
		t.Fatalf("expected no tools from broken server, got %v", names)
	}
}

func TestClient_callToolResultShapes(t *testing.T) {
	type kv struct {
		K string `json:"k"`
	}
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "shapes", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "obj", Title: "FromTitle"},
		func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, kv, error) {
			return nil, kv{K: "v"}, nil
		})
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "fail", Description: "fails"},
		func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			return &mcpsdk.CallToolResult{
				IsError: true,
				Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: "nope"},
					&mcpsdk.TextContent{Text: "denied"},
				},
			}, struct{}{}, nil
		})
	hs := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(hs.Close)

	var byName = map[string]struct {
		desc    string
		handler ToolHandler
	}{}
	cleanup := DiscoverAllTools(t.Context(), []mcp.MCPConfig{
		{Type: mcp.TransportHTTP, Name: "svc", URL: hs.URL},
	}, func(name, description, namespace string, schema map[string]any, handler ToolHandler) {
		byName[name] = struct {
			desc    string
			handler ToolHandler
		}{desc: description, handler: handler}
	})
	t.Cleanup(cleanup)

	if byName["obj"].desc != "FromTitle" {
		t.Fatalf("title fallback desc = %q", byName["obj"].desc)
	}
	out, err := byName["obj"].handler(t.Context(), map[string]any{})
	if err != nil || !strings.Contains(out, `"k"`) || !strings.Contains(out, `"v"`) {
		t.Fatalf("structured = %q err=%v", out, err)
	}
	_, err = byName["fail"].handler(t.Context(), map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "nope; denied") {
		t.Fatalf("joined error = %v", err)
	}
}
