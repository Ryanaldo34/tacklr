package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// pingServer is an MCP server with one tool, ping.
func pingServer() *mcpsdk.Server {
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "ok", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "ping"},
		func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			return &mcpsdk.CallToolResult{}, struct{}{}, nil
		})
	return srv
}

// TestConnect_skipsServersThatFail: a server whose config cannot connect, a
// server that does not answer or does not speak MCP, and one that connects but cannot list its
// tools are skipped; the streamable HTTP and SSE servers that answer keep
// their tools.
func TestConnect_skipsServersThatFail(t *testing.T) {
	t.Parallel()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)
	srv := pingServer()
	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	good := httptest.NewServer(mcpHandler)
	t.Cleanup(good.Close)
	events := httptest.NewServer(mcpsdk.NewSSEHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(events.Close)
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
		{Name: "no command"},
		{Name: "missing command", Command: "/nonexistent/mcp-server"},
		{Name: "http without a url", Type: session.MCPHTTP},
		{Name: "sse without a url", Type: session.MCPSSE},
		{Name: "an unknown transport", Type: "grpc"},
		{Type: session.MCPHTTP, Name: "nothing listening", URL: "http://127.0.0.1:1"},
		{Type: session.MCPHTTP, Name: "broken", URL: broken.URL},
		{Type: session.MCPHTTP, Name: "nolist", URL: noList.URL},
		{Type: session.MCPHTTP, Name: "good", URL: good.URL},
		{Type: session.MCPSSE, Name: "events", URL: events.URL},
	})
	t.Cleanup(cleanup)

	var got []string
	for _, tool := range tools {
		got = append(got, tool.Server+"/"+tool.Name)
	}
	if strings.Join(got, ",") != "good/ping,events/ping" {
		t.Fatalf("tools = %v, want good's and events' ping", got)
	}
}

// TestConnect_closingLogsAServerThatFailsToEndItsSession: closing the
// connections ends each server's session. A server that does not allow the
// session DELETE (405) counts as closed; a server that fails it is logged
// with its status.
func TestConnect_closingLogsAServerThatFailsToEndItsSession(t *testing.T) {
	// Not parallel: it reads what the process-wide default logger writes.
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	srv := pingServer()
	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	deleting := func(status int) string {
		hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				http.Error(w, "no delete", status)
				return
			}
			mcpHandler.ServeHTTP(w, r)
		}))
		t.Cleanup(hs.Close)
		return hs.URL
	}
	tools, cleanup := Client{}.Connect(t.Context(), []session.MCPServer{
		{Type: session.MCPHTTP, Name: "refuses", URL: deleting(http.StatusMethodNotAllowed)},
		{Type: session.MCPHTTP, Name: "fails", URL: deleting(http.StatusInternalServerError)},
	})

	cleanup()

	out := logs.String()
	if len(tools) != 2 || !strings.Contains(out, `msg="mcp server close returned 405, treating as successful" server=refuses`) ||
		!strings.Contains(out, `msg="failed to close MCP client" server=fails`) || !strings.Contains(out, "close: HTTP error (status 500)") {
		t.Fatalf("tools %v, logs after close:\n%s\nwant the 405 closed and the 500 logged with its status", tools, out)
	}
}

// TestClient_callToolReturnsWhatTheServerAnswers: the client lists every page
// of tools, and a call sends the server's headers and returns the tool's
// text or its structured result, also when the result has no text. A tool
// error joins its texts, a JSON-RPC error names its code and data, and a
// call after the connections close names the closed connection.
func TestClient_callToolReturnsWhatTheServerAnswers(t *testing.T) {
	t.Parallel()
	type kv struct {
		K string `json:"k"`
	}
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "shapes", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "obj", Title: "FromTitle"},
		func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, kv, error) {
			return nil, kv{K: "v"}, nil
		})
	for _, name := range []string{"fail", "rpc"} {
		mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: name, Description: name},
			func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
				return &mcpsdk.CallToolResult{IsError: true, Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: "nope"}, &mcpsdk.TextContent{Text: "denied"},
				}}, struct{}{}, nil
			})
	}
	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	// The server needs its bearer token, and answers rpc on the wire itself.
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer shhh" {
			http.Error(w, "no token", http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var call struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				Name   string `json:"name"`
				Cursor string `json:"cursor"`
			} `json:"params"`
		}
		_ = json.Unmarshal(body, &call)
		answer := func(payload string) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(call.ID) + `,` + payload + `}`))
		}
		switch {
		case call.Method == "tools/call" && call.Params.Name == "rpc":
			answer(`"error":{"code":-32602,"message":"invalid params","data":{"detail":"missing field"}}`)
		case call.Method == "tools/call" && call.Params.Name == "raw":
			answer(`"result":{"content":[],"structuredContent":{"k":"raw"}}`)
		// The first page of tools is raw alone; the next page is the SDK server's.
		case call.Method == "tools/list" && call.Params.Cursor == "":
			answer(`"result":{"tools":[{"name":"raw","inputSchema":{"type":"object"}}],"nextCursor":"more"}`)
		case call.Method == "tools/list":
			r.Body = io.NopCloser(bytes.NewReader(bytes.Replace(body, []byte(`"cursor":"more"`), nil, 1)))
			mcpHandler.ServeHTTP(w, r)
		default:
			r.Body = io.NopCloser(bytes.NewReader(body))
			mcpHandler.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(hs.Close)

	tools, cleanup := Client{}.Connect(t.Context(), []session.MCPServer{
		{Type: session.MCPHTTP, Name: "svc", URL: hs.URL, Headers: []session.MCPHeader{{Name: "Authorization", Value: "Bearer shhh"}}},
	})
	byName := map[string]session.MCPTool{}
	for _, tool := range tools {
		byName[tool.Name] = tool
	}
	call := func(name string) (string, error) { return byName[name].Call(t.Context(), map[string]any{}) }

	structured, structuredErr := call("obj")
	raw, rawErr := call("raw")
	_, toolErr := call("fail")
	_, rpcErr := call("rpc")
	cleanup()
	_, closedErr := call("obj")

	if byName["obj"].Description != "FromTitle" || byName["obj"].Server != "svc" {
		t.Errorf("tool = %+v, want the title as its description", byName["obj"])
	}
	if structuredErr != nil || structured != `{"k":"v"}` || rawErr != nil || raw != `{"k":"raw"}` {
		t.Errorf("structured result = %q %v, with no text = %q %v, want its JSON", structured, structuredErr, raw, rawErr)
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"a tool error":        {toolErr, `tool "fail" error: nope; denied`},
		"a json-rpc error":    {rpcErr, `call tool "rpc": JSON-RPC error -32602: invalid params (data: {"detail":"missing field"})`},
		"a closed connection": {closedErr, `call tool "obj": connection closed`},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want %q", name, tc.err, tc.want)
		}
	}
	if !errors.Is(closedErr, mcpsdk.ErrConnectionClosed) {
		t.Errorf("call after close = %v, want ErrConnectionClosed", closedErr)
	}
}
