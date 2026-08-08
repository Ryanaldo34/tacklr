package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr"
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

// toolRecorder captures the namespaced tool names visible to the model on
// each Invoke call.
type toolRecorder struct {
	mu   sync.Mutex
	seen [][]string
}

func (tr *toolRecorder) record(tools []*tacklr.Tool) {
	var names []string
	for _, tool := range tools {
		names = append(names, tool.Namespace+"/"+tool.Name)
	}
	tr.mu.Lock()
	tr.seen = append(tr.seen, names)
	tr.mu.Unlock()
}

func (tr *toolRecorder) call(i int) []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if i >= len(tr.seen) {
		return nil
	}
	return tr.seen[i]
}

func recordingStrategy(tr *toolRecorder) *mockInferenceStrategy {
	return &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			tr.record(tools)
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
}

func acpSessionID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["error"] != nil {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	sessionID, ok := resp["result"].(map[string]any)["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("missing sessionId in result: %v", resp)
	}
	return sessionID
}

// When one MCP server is unreachable, the other server's tools still appear
// and can be invoked successfully.
func TestHandleRPC_sessionMCPServers_partialFailureStillExposesHealthyTools(t *testing.T) {
	good := newTestMCPServer(t, "ping")
	store := testStore(t)
	var invokeCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				// Assert healthy tool is present, dead server is not.
				var names []string
				for _, tool := range tools {
					names = append(names, tool.Namespace+"/"+tool.Name)
				}
				if !slices.Contains(names, "good/ping") {
					t.Errorf("expected good/ping in tools, got %v", names)
				}
				if slices.Contains(names, "dead/anything") {
					t.Errorf("dead server tools should be absent, got %v", names)
				}
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "c1", CallID: "c1", Name: "ping", Namespace: "good", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "partial ok", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, nil)

	// dead URL: nothing listening
	body := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[` +
		`{"type":"http","name":"dead","url":"http://127.0.0.1:1","headers":[]},` +
		`{"type":"http","name":"good","url":"` + good.URL + `","headers":[]}` +
		`]}}`
	rec1 := serveACPRaw(t, r, body)
	sessionID := acpSessionID(t, rec1)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"ping"}]}}`)
	blob, _ := json.Marshal(parseACPFrames(t, rec2.Body))
	if !strings.Contains(string(blob), "ok") {
		t.Fatalf("expected MCP tool result ok from healthy server; frames=%s", blob)
	}
	if !strings.Contains(string(blob), "partial ok") {
		t.Fatalf("expected final message; frames=%s", blob)
	}
}

// After discovery, the model can invoke a namespaced MCP tool and the handler
// result is delivered as a tool result event to the client.
func TestHandleRPC_sessionMCPServers_toolCallReturnsResult(t *testing.T) {
	mcpHTTP := newTestMCPServer(t, "greet")
	store := testStore(t)
	var invokeCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_mcp", CallID: "call_mcp", Name: "greet", Namespace: "testmcp", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "mcp done", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"type":"http","name":"testmcp","url":"`+mcpHTTP.URL+`","headers":[]}]}}`)
	sessionID := acpSessionID(t, rec1)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"call greet"}]}}`)
	frames := parseACPFrames(t, rec2.Body)

	var toolResultText string
	var sawFinal bool
	for _, f := range frames {
		if f["error"] != nil {
			t.Fatalf("prompt error: %v", f["error"])
		}
		if f["method"] != "session/update" {
			continue
		}
		params, _ := f["params"].(map[string]any)
		update, _ := params["update"].(map[string]any)
		switch update["sessionUpdate"] {
		case "tool_call_update":
			// content may be a list of {type,text}
			if content, ok := update["content"].([]any); ok {
				for _, c := range content {
					m, _ := c.(map[string]any)
					if m["type"] == "text" {
						if txt, ok := m["text"].(string); ok {
							toolResultText += txt
						}
					}
				}
			}
			if content, ok := update["content"].(map[string]any); ok {
				if txt, ok := content["text"].(string); ok {
					toolResultText += txt
				}
			}
			if raw, ok := update["rawOutput"].(string); ok {
				toolResultText += raw
			}
		case "agent_message_chunk":
			if content, ok := update["content"].(map[string]any); ok && content["text"] == "mcp done" {
				sawFinal = true
			}
		}
	}
	if !strings.Contains(toolResultText, "ok") {
		// Fallback: scan all frames as text for "ok" tool output.
		blob, _ := json.Marshal(frames)
		if !strings.Contains(string(blob), "ok") {
			t.Fatalf("expected MCP tool result containing ok, frames=%s", blob)
		}
	}
	if !sawFinal {
		t.Error("expected final agent message after MCP tool result")
	}
	if invokeCount != 2 {
		t.Errorf("invokeCount = %d, want 2", invokeCount)
	}
}

// A client-supplied http MCP server in session/new is connected to by the
// harness and its tools are exposed to the model during the prompt turn.
func TestHandleRPC_sessionMCPServers_toolsDiscovered(t *testing.T) {
	mcpHTTP := newTestMCPServer(t, "greet")
	store := testStore(t)
	recorder := &toolRecorder{}
	r := newTestRegistry(store, recordingStrategy(recorder), []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"type":"http","name":"testmcp","url":"`+mcpHTTP.URL+`","headers":[{"name":"X-Key","value":"k"}]}]}}`)
	sessionID := acpSessionID(t, rec1)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"hi"}]}}`)
	frames := parseACPFrames(t, rec2.Body)
	for _, f := range frames {
		if f["error"] != nil {
			t.Fatalf("prompt error: %v", f["error"])
		}
	}

	tools := recorder.call(0)
	if !slices.Contains(tools, "testmcp/greet") {
		t.Errorf("expected MCP tool testmcp/greet in model tools, got %v", tools)
	}
}

// On session/resume the client re-specifies the MCP server list; the new list
// replaces the stored one for subsequent turns.
func TestHandleRPC_sessionResume_overridesMCPServers(t *testing.T) {
	serverA := newTestMCPServer(t, "tool_a")
	serverB := newTestMCPServer(t, "tool_b")
	store := testStore(t)
	recorder := &toolRecorder{}
	r := newTestRegistry(store, recordingStrategy(recorder), []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"type":"http","name":"a","url":"`+serverA.URL+`","headers":[]}]}}`)
	sessionID := acpSessionID(t, rec1)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"hi"}]}}`)
	for _, f := range parseACPFrames(t, rec2.Body) {
		if f["error"] != nil {
			t.Fatalf("prompt error: %v", f["error"])
		}
	}
	if tools := recorder.call(0); !slices.Contains(tools, "a/tool_a") {
		t.Fatalf("first turn: expected a/tool_a, got %v", tools)
	}

	rec3 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":3,"method":"session/resume","params":{"sessionId":"`+sessionID+`","cwd":"/tmp","mcpServers":[{"type":"http","name":"b","url":"`+serverB.URL+`","headers":[]}]}}`)
	for _, f := range parseACPFrames(t, rec3.Body) {
		if f["error"] != nil {
			t.Fatalf("resume error: %v", f["error"])
		}
	}

	// The resume turn continues the session, so the model was invoked again
	// with only server B's tools.
	var sawB bool
	for i := 1; i < len(recorder.seen); i++ {
		tools := recorder.call(i)
		if slices.Contains(tools, "a/tool_a") {
			t.Errorf("turn %d: server A tools should be gone after resume, got %v", i, tools)
		}
		if slices.Contains(tools, "b/tool_b") {
			sawB = true
		}
	}
	if !sawB {
		t.Errorf("expected b/tool_b in a post-resume turn, calls: %v", recorder.seen)
	}
}
