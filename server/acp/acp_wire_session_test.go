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

// TestACP_mcpServersOnAResumeOrALoadReplaceTheSessions: the MCP servers a
// session/resume names replace the session's servers, so the model sees
// the new server's tools in place of the old one's, and a call to one
// returns the server's result to the client; the servers a session/load
// names replace them from the next prompt on.
func TestACP_mcpServersOnAResumeOrALoadReplaceTheSessions(t *testing.T) {
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
	c := acptest.Open(t, acptest.NewRuntime(t, model, tacklr.AgentOptions{}))
	sid := acptest.SessionID(t, c.Call("session/new", `{"cwd":"/tmp","mcpServers":[{"type":"http","name":"a","url":"`+serverA.URL+`","headers":[]}]}`))

	prompted, _ := c.Say(sid, "hi")
	resumed, frames := c.Until(c.Request("session/resume", `{"sessionId":"`+sid+`","cwd":"/tmp","mcpServers":[{"type":"http","name":"b","url":"`+serverB.URL+`","headers":[]}]}`), nil)
	mu.Lock()
	afterResume := seen[len(seen)-1]
	mu.Unlock()
	acptest.Result(t, c.Call("session/load", `{"sessionId":"`+sid+`","cwd":"/tmp","mcpServers":[{"type":"http","name":"a","url":"`+serverA.URL+`","headers":[]}]}`))
	reloaded, _ := c.Say(sid, "again")
	var result any
	for _, update := range acptest.Updates(frames) {
		if update["sessionUpdate"] == "tool_call_update" {
			result = update["rawOutput"]
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if acptest.StopReason(prompted) != "end_turn" || acptest.StopReason(resumed) != "end_turn" || acptest.StopReason(reloaded) != "end_turn" {
		t.Fatalf("prompt %v, resume %v, after the load %v; want three turns", prompted, resumed, reloaded)
	}
	if want := map[string]any{"output": "ok"}; !reflect.DeepEqual(result, want) {
		t.Fatalf("tool_b result = %v, want %v", result, want)
	}
	if !slices.Equal(seen[0], []string{"a/tool_a"}) || !slices.Equal(afterResume, []string{"b/tool_b"}) || !slices.Equal(seen[len(seen)-1], []string{"a/tool_a"}) {
		t.Fatalf("MCP tools per model call %v; want a/tool_a, then b/tool_b, then a/tool_a again after the load", seen)
	}
}
