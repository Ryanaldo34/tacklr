package mcp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// A server that does not speak MCP, and one that connects but cannot list
// its tools, are skipped; the server that answers keeps its tools.
func TestConnect_skipsServersThatFail(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "ok", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "ping"},
		func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			return &mcpsdk.CallToolResult{}, struct{}{}, nil
		})
	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	good := httptest.NewServer(mcpHandler)
	t.Cleanup(good.Close)
	noList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"tools/list"`) {
			http.Error(w, "no tools", http.StatusInternalServerError)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		mcpHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(noList.Close)

	tools, cleanup := Client{}.Connect(t.Context(), []session.MCPServer{
		{Type: session.MCPHTTP, Name: "broken", URL: broken.URL},
		{Type: session.MCPHTTP, Name: "nolist", URL: noList.URL},
		{Type: session.MCPHTTP, Name: "good", URL: good.URL},
	})
	t.Cleanup(cleanup)

	if len(tools) != 1 || tools[0].Server != "good" || tools[0].Name != "ping" {
		t.Fatalf("tools = %+v, want only good's ping", tools)
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

	tools, cleanup := Client{}.Connect(t.Context(), []session.MCPServer{
		{Type: session.MCPHTTP, Name: "svc", URL: hs.URL},
	})
	t.Cleanup(cleanup)
	byName := map[string]session.MCPTool{}
	for _, tool := range tools {
		byName[tool.Name] = tool
	}

	if byName["obj"].Description != "FromTitle" || byName["obj"].Server != "svc" {
		t.Fatalf("title fallback = %+v", byName["obj"])
	}
	out, err := byName["obj"].Call(t.Context(), map[string]any{})
	if err != nil || !strings.Contains(out, `"k"`) || !strings.Contains(out, `"v"`) {
		t.Fatalf("structured = %q err=%v", out, err)
	}
	_, err = byName["fail"].Call(t.Context(), map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "nope; denied") {
		t.Fatalf("joined error = %v", err)
	}
}
