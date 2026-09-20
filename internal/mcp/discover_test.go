package mcpruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr/mcp"
)

// newMCPHTTPServer stands up a streamable-HTTP MCP server with the given tools.
func newMCPHTTPServer(t *testing.T, tools map[string]string) *httptest.Server {
	t.Helper()
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "discover-test", Version: "v0.0.1"}, nil)
	for name, reply := range tools {
		n, r := name, reply
		mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: n, Description: "desc for " + n},
			func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
				if strings.HasPrefix(r, "ERR:") {
					return &mcpsdk.CallToolResult{
						IsError: true,
						Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: strings.TrimPrefix(r, "ERR:")}},
					}, struct{}{}, nil
				}
				return &mcpsdk.CallToolResult{
					Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: r}},
				}, struct{}{}, nil
			})
	}
	hs := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(hs.Close)
	return hs
}

func TestDiscoverAllTools_connectsDiscoversAndInvokes(t *testing.T) {
	hs := newMCPHTTPServer(t, map[string]string{
		"echo": "pong",
		"fail": "ERR:boom",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type discovered struct {
		name, desc, ns string
		handler        ToolHandler
	}
	var found []discovered
	cleanup := DiscoverAllTools(ctx, []mcp.MCPConfig{
		{Type: mcp.TransportHTTP, Name: "svc", URL: hs.URL},
	}, func(name, description, namespace string, schema map[string]any, handler ToolHandler) {
		found = append(found, discovered{name: name, desc: description, ns: namespace, handler: handler})
	})
	t.Cleanup(cleanup)

	if len(found) != 2 {
		t.Fatalf("discovered %d tools, want 2: %+v", len(found), found)
	}
	byName := map[string]discovered{}
	for _, d := range found {
		byName[d.name] = d
		if d.ns != "svc" {
			t.Errorf("namespace = %q, want svc", d.ns)
		}
		if d.desc == "" {
			t.Errorf("tool %q missing description", d.name)
		}
	}

	// Successful call.
	out, err := byName["echo"].handler(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("echo call: %v", err)
	}
	if out != "pong" {
		t.Errorf("echo = %q, want pong", out)
	}

	// Tool-level error surfaces as error return with message.
	_, err = byName["fail"].handler(ctx, map[string]any{})
	if err == nil {
		t.Fatal("expected error from fail tool")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %q, want boom", err.Error())
	}
}

func TestDiscoverAllTools_skipsUnreachableAndStillLoadsHealthy(t *testing.T) {
	hs := newMCPHTTPServer(t, map[string]string{"ok": "yes"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var names []string
	cleanup := DiscoverAllTools(ctx, []mcp.MCPConfig{
		{Type: mcp.TransportHTTP, Name: "dead", URL: "http://127.0.0.1:1"},
		{Type: mcp.TransportHTTP, Name: "live", URL: hs.URL},
	}, func(name, description, namespace string, schema map[string]any, handler ToolHandler) {
		names = append(names, namespace+"/"+name)
	})
	t.Cleanup(cleanup)

	if len(names) != 1 || names[0] != "live/ok" {
		t.Fatalf("names = %v, want [live/ok]", names)
	}
}

func TestDiscoverAllTools_cleanupClosesClients(t *testing.T) {
	hs := newMCPHTTPServer(t, map[string]string{"a": "1"})
	ctx := context.Background()
	var handler ToolHandler
	cleanup := DiscoverAllTools(ctx, []mcp.MCPConfig{
		{Type: mcp.TransportHTTP, Name: "svc", URL: hs.URL},
	}, func(name, description, namespace string, schema map[string]any, h ToolHandler) {
		handler = h
	})
	if handler == nil {
		t.Fatal("no handler registered")
	}
	// Cleanup should not panic; post-close calls may fail.
	cleanup()
	// Second cleanup is not required; DiscoverAllTools returns a single closer.
}

func TestClient_connectListCallRoundTrip(t *testing.T) {
	// Package-level path through unexported client (connect → listTools → callTool → close).
	hs := newMCPHTTPServer(t, map[string]string{"greet": "hello"})
	c := newClient(mcp.MCPConfig{Type: mcp.TransportHTTP, Name: "roundtrip", URL: hs.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	tools, err := c.listTools(ctx)
	if err != nil {
		t.Fatalf("listTools: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "greet" {
		t.Fatalf("tools = %+v", tools)
	}
	out, err := c.callTool(ctx, "greet", map[string]any{})
	if err != nil {
		t.Fatalf("callTool: %v", err)
	}
	if out != "hello" {
		t.Errorf("out = %q", out)
	}
	if err := c.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// close with nil session is a no-op success.
	c2 := newClient(mcp.MCPConfig{Name: "empty"})
	if err := c2.close(); err != nil {
		t.Fatalf("close nil session: %v", err)
	}
}

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
