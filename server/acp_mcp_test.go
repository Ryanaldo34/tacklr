package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
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

	state, ok := r.sessions.Load(sessionID)
	if !ok {
		t.Fatal("expected session state to be stored")
	}
	s := state.(*sessionState)
	if len(s.mcpServers) != 1 || s.mcpServers[0].Name != "b" {
		t.Errorf("stored mcpServers = %v, want single server b", s.mcpServers)
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
