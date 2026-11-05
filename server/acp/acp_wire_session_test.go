package acp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/message"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
)

// newTestMCPServer stands up a real in-process MCP server over streamable
// HTTP exposing a single tool with the given name.
func newTestMCPServer(t *testing.T, toolName string) *httptest.Server {
	t.Helper()
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-mcp", Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: toolName, Description: "test tool"},
		func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "ok"}},
			}, struct{}{}, nil
		})
	hs := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(hs.Close)
	return hs
}

// TestHandleRPC_sessionResume_overridesMCPServers: the MCP servers a
// session/resume names replace the session's servers, so the model sees
// the new server's tools in place of the old one's, and a call to one
// returns the server's result to the client.
func TestHandleRPC_sessionResume_overridesMCPServers(t *testing.T) {
	t.Parallel()
	serverA := newTestMCPServer(t, "tool_a")
	serverB := newTestMCPServer(t, "tool_b")
	var mu sync.Mutex
	var seen [][]string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		var mcp []string
		for _, tool := range tools {
			if ns := tool.Namespace(); ns == "a" || ns == "b" {
				mcp = append(mcp, ns+"/"+tool.Name())
			}
		}
		mu.Lock()
		seen = append(seen, mcp)
		mu.Unlock()
		if last := msgs[len(msgs)-1]; last.Role != message.RoleTool && slices.Contains(mcp, "b/tool_b") {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
				{ID: "call_b", CallID: "call_b", Name: "tool_b", Namespace: "b", Arguments: `{}`},
			}}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
	})
	srv := acptest.NewServer(t, acptest.NewRuntime(t, model, tacklr.AgentOptions{}), server.NewMemoryWireStore())
	sid := acptest.SessionID(t, srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"type":"http","name":"a","url":"`+serverA.URL+`","headers":[]}]}}`))

	prompted := stopReason(t, srv.RPC(prompt("2", sid, `[{"type":"text","text":"hi"}]`)))
	resumed := srv.RPC(`{"jsonrpc":"2.0","id":3,"method":"session/resume","params":{"sessionId":"` + sid + `","cwd":"/tmp","mcpServers":[{"type":"http","name":"b","url":"` + serverB.URL + `","headers":[]}]}}`)
	var result, end any
	for _, f := range acptest.Frames(t, resumed.Body) {
		if params, ok := f["params"].(map[string]any); ok {
			if update := params["update"].(map[string]any); update["sessionUpdate"] == "tool_call_update" {
				result = update["rawOutput"]
			}
		}
		if res, ok := f["result"].(map[string]any); ok {
			end = res["stopReason"]
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if prompted != "end_turn" || end != "end_turn" || len(seen) < 2 {
		t.Fatalf("prompt %q, resume %v, model calls %v; want two turns", prompted, end, seen)
	}
	if want := map[string]any{"output": "ok"}; !reflect.DeepEqual(result, want) {
		t.Fatalf("tool_b result = %v, want %v", result, want)
	}
	if !slices.Equal(seen[0], []string{"a/tool_a"}) || !slices.Equal(seen[len(seen)-1], []string{"b/tool_b"}) {
		t.Fatalf("MCP tools per model call %v; want a/tool_a, then b/tool_b", seen)
	}
}
