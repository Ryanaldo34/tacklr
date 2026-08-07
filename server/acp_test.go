package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newACPRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	return httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
}

func parseACPFrames(t *testing.T, body io.Reader) []map[string]any {
	t.Helper()
	var frames []map[string]any
	dec := json.NewDecoder(body)
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode ACP frame: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func serveACPRaw(t *testing.T, r *Registry, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := newACPRequest(t, body)
	rec := httptest.NewRecorder()
	NewServer(r, ACP).serveHTTPRPC(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// validateACPRequest
// ---------------------------------------------------------------------------

func TestValidateACPRequest_initialize(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.Method != "initialize" {
		t.Errorf("method = %q, want %q", pr.Method, "initialize")
	}
	if string(pr.ID) != "1" {
		t.Errorf("id = %s, want 1", pr.ID)
	}
}

func TestValidateACPRequest_initialize_acceptsHigherVersion(t *testing.T) {
	// Agent negotiates by responding with its supported version; request may ask higher.
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":99}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.Method != "initialize" {
		t.Errorf("method = %q, want initialize", pr.Method)
	}
}

func TestValidateACPRequest_notification_noID(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s1"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pr.Notification {
		t.Error("expected Notification=true")
	}
	if pr.ThreadID != "s1" {
		t.Errorf("threadID = %q, want s1", pr.ThreadID)
	}
}

func TestValidateACPRequest_sessionNew(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/home/user"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.Method != "session/new" {
		t.Errorf("method = %q, want %q", pr.Method, "session/new")
	}
	if pr.CWD != "/home/user" {
		t.Errorf("cwd = %q, want %q", pr.CWD, "/home/user")
	}
}

func TestValidateACPRequest_sessionNew_withMCPServers(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[
		{"name":"fs","command":"npx","args":["-y","@modelcontextprotocol/server-filesystem"],"env":[{"name":"API_KEY","value":"secret"}]},
		{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[{"name":"Authorization","value":"Bearer tok"}]},
		{"type":"sse","name":"events","url":"https://events.example.com/mcp","headers":[]}
	]}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.CWD != "/tmp" {
		t.Errorf("cwd = %q, want /tmp", pr.CWD)
	}
	if len(pr.MCPServers) != 3 {
		t.Fatalf("mcpServers len = %d, want 3", len(pr.MCPServers))
	}

	stdio := pr.MCPServers[0]
	if stdio.Name != "fs" || stdio.Command != "npx" {
		t.Errorf("stdio server = %+v, want name=fs command=npx", stdio)
	}
	if len(stdio.Args) != 2 || stdio.Args[0] != "-y" {
		t.Errorf("stdio args = %v, want [-y @modelcontextprotocol/server-filesystem]", stdio.Args)
	}
	if len(stdio.Env) != 1 || stdio.Env[0].Name != "API_KEY" || stdio.Env[0].Value != "secret" {
		t.Errorf("stdio env = %v, want [API_KEY=secret]", stdio.Env)
	}

	httpSrv := pr.MCPServers[1]
	if httpSrv.Type != "http" || httpSrv.URL != "https://api.example.com/mcp" {
		t.Errorf("http server = %+v, want type=http url=https://api.example.com/mcp", httpSrv)
	}
	if len(httpSrv.Headers) != 1 || httpSrv.Headers[0].Name != "Authorization" || httpSrv.Headers[0].Value != "Bearer tok" {
		t.Errorf("http headers = %v, want [Authorization: Bearer tok]", httpSrv.Headers)
	}

	sseSrv := pr.MCPServers[2]
	if sseSrv.Type != "sse" || sseSrv.URL != "https://events.example.com/mcp" {
		t.Errorf("sse server = %+v, want type=sse url=https://events.example.com/mcp", sseSrv)
	}
}

func TestValidateACPRequest_sessionPrompt(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-1","prompt":[{"type":"text","text":"hello"}]}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-1" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-1")
	}
	if pr.Prompt != "hello" {
		t.Errorf("prompt = %q, want %q", pr.Prompt, "hello")
	}
}

func TestValidateACPRequest_sessionPrompt_missingSessionID(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"prompt":[{"type":"text","text":"hi"}]}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for missing sessionId")
	}
	if !strings.Contains(err.Error(), "sessionId is required") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "sessionId is required")
	}
}

func TestValidateACPRequest_sessionPrompt_emptyPrompt(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for empty prompt")
	}
	if !strings.Contains(err.Error(), "prompt must not be empty") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "prompt must not be empty")
	}
}

func TestValidateACPRequest_sessionResume(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":4,"method":"session/resume","params":{"sessionId":"sess-2","cwd":"/tmp","mcpServers":[{"name":"fs","command":"npx","args":[]}]}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-2" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-2")
	}
	if len(pr.MCPServers) != 1 || pr.MCPServers[0].Command != "npx" {
		t.Errorf("mcpServers = %v, want one stdio server with command npx", pr.MCPServers)
	}
}

func TestValidateACPRequest_sessionClose(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":5,"method":"session/close","params":{"sessionId":"sess-3"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-3" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-3")
	}
}

func TestValidateACPRequest_sessionCancel(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":6,"method":"session/cancel","params":{"sessionId":"sess-4"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-4" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-4")
	}
}

func TestValidateACPRequest_invalidJSON(t *testing.T) {
	_, err := validateACPRequest([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestValidateACPRequest_wrongJSONRPCVersion(t *testing.T) {
	body := []byte(`{"jsonrpc":"1.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for wrong jsonrpc version")
	}
	if !strings.Contains(err.Error(), "jsonrpc version must be") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "jsonrpc version must be")
	}
}

func TestValidateACPRequest_missingID_isNotification(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"x"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pr.Notification {
		t.Error("expected notification when id is absent")
	}
}

func TestValidateACPRequest_missingMethod(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for missing method")
	}
}

func TestValidateACPRequest_unsupportedMethod(t *testing.T) {
	// Unknown methods are admitted so HandleInbound can return MethodNotFound.
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{"sessionId":"s1"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if pr.Method != "session/foo" {
		t.Fatalf("method = %q", pr.Method)
	}
}

func TestValidateACPRequest_sessionLoad(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"session/load","params":{"sessionId":"sess-load","cwd":"/proj","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[]}]}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-load" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-load")
	}
	if pr.CWD != "/proj" {
		t.Errorf("cwd = %q, want /proj", pr.CWD)
	}
	if len(pr.MCPServers) != 1 || pr.MCPServers[0].Type != "http" {
		t.Errorf("mcpServers = %v, want one http server", pr.MCPServers)
	}
}

func TestValidateACPRequest_sessionLoad_missingSessionID(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"session/load","params":{}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for missing sessionId")
	}
	if !strings.Contains(err.Error(), "sessionId is required") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "sessionId is required")
	}
}

func TestValidateACPRequest_configSet(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":8,"method":"session/set_config_option","params":{"sessionId":"sess-1","configId":"model","value":"custom"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-1" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-1")
	}
	if pr.ConfigID != "model" {
		t.Errorf("configId = %q, want %q", pr.ConfigID, "model")
	}
	if pr.ConfigValue != "custom" {
		t.Errorf("configValue = %q, want %q", pr.ConfigValue, "custom")
	}
}

func TestValidateACPRequest_configSet_missingConfigID(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":8,"method":"session/set_config_option","params":{"sessionId":"sess-1","value":"custom"}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for missing configId")
	}
	if !strings.Contains(err.Error(), "configId is required") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "configId is required")
	}
}

// ---------------------------------------------------------------------------
// concatenateACPPrompt
// ---------------------------------------------------------------------------

func TestConcatenateACPPrompt_textOnly(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":"hello"},{"type":"text","text":"world"}]`)
	got, err := concatenateACPPrompt(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hello\n\nworld" {
		t.Errorf("got %q, want %q", got, "hello\n\nworld")
	}
}

func TestConcatenateACPPrompt_resourceOnly(t *testing.T) {
	raw := json.RawMessage(`[{"type":"resource","resource":{"uri":"file:///a.txt","mimeType":"text/plain","text":"file content"}}]`)
	got, err := concatenateACPPrompt(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "file content" {
		t.Errorf("got %q, want %q", got, "file content")
	}
}

func TestConcatenateACPPrompt_mixed(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":"prompt"},{"type":"resource","resource":{"uri":"file:///b.txt","mimeType":"text/plain","text":"ctx"}}]`)
	got, err := concatenateACPPrompt(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "prompt\n\nctx" {
		t.Errorf("got %q, want %q", got, "prompt\n\nctx")
	}
}

func TestConcatenateACPPrompt_emptyText(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":""}]`)
	_, err := concatenateACPPrompt(raw)
	if err == nil {
		t.Fatal("expected error for empty text block")
	}
}

func TestConcatenateACPPrompt_unsupportedType(t *testing.T) {
	raw := json.RawMessage(`[{"type":"image","data":"base64..."}]`)
	_, err := concatenateACPPrompt(raw)
	if err == nil {
		t.Fatal("expected error for unsupported block type")
	}
	if !strings.Contains(err.Error(), "unsupported content block type") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "unsupported content block type")
	}
}

// ---------------------------------------------------------------------------
// eventToAcpJsonRpc
// ---------------------------------------------------------------------------

func TestEventToAcpJsonRpc_message(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:      streaming.StreamEventMessage,
		MessageID: "msg-1",
		Content:   "hello",
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames len = %d, want 1", len(frames))
	}
	var msg map[string]any
	if err := json.Unmarshal(frames[0], &msg); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if msg["method"] != "session/update" {
		t.Errorf("method = %v, want session/update", msg["method"])
	}
	params := msg["params"].(map[string]any)
	if params["sessionId"] != "thread-1" {
		t.Errorf("sessionId = %v, want thread-1", params["sessionId"])
	}
	update := params["update"].(map[string]any)
	if update["sessionUpdate"] != "agent_message_chunk" {
		t.Errorf("sessionUpdate = %v, want agent_message_chunk", update["sessionUpdate"])
	}
}

func TestEventToAcpJsonRpc_reasoning(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:      streaming.StreamEventReasoning,
		MessageID: "msg-2",
		Content:   "thinking...",
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	params := msg["params"].(map[string]any)
	update := params["update"].(map[string]any)
	if update["sessionUpdate"] != "agent_thought_chunk" {
		t.Errorf("sessionUpdate = %v, want agent_thought_chunk", update["sessionUpdate"])
	}
}

func TestEventToAcpJsonRpc_functionCall(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type: streaming.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{
			{ID: "tc-1", Name: "complete_todo", Title: "Complete Ship", Category: "think"},
		},
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames len = %d, want 1", len(frames))
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	params := msg["params"].(map[string]any)
	update := params["update"].(map[string]any)
	if update["sessionUpdate"] != "tool_call" {
		t.Errorf("sessionUpdate = %v, want tool_call", update["sessionUpdate"])
	}
	if update["status"] != "in_progress" {
		t.Errorf("status = %v, want in_progress", update["status"])
	}
	if update["title"] != "Complete Ship" {
		t.Errorf("title = %v, want Complete Ship", update["title"])
	}
	if update["name"] != "complete_todo" {
		t.Errorf("name = %v, want complete_todo", update["name"])
	}
	if update["kind"] != "think" {
		t.Errorf("kind = %v, want think", update["kind"])
	}
}

func TestEventToAcpJsonRpc_toolResult(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:    streaming.StreamEventToolResult,
		Content: "file contents here",
		ToolCalls: []tacklr.ToolCall{
			{ID: "tc-1", Name: "read_file"},
		},
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	params := msg["params"].(map[string]any)
	update := params["update"].(map[string]any)
	if update["sessionUpdate"] != "tool_call_update" {
		t.Errorf("sessionUpdate = %v, want tool_call_update", update["sessionUpdate"])
	}
	if update["status"] != "completed" {
		t.Errorf("status = %v, want completed", update["status"])
	}
	if update["toolCallId"] != "tc-1" {
		t.Errorf("toolCallId = %v, want tc-1", update["toolCallId"])
	}
	content, ok := update["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %v, want ACP ToolCallContent array of length 1", update["content"])
	}
	inner := content[0].(map[string]any)
	if inner["type"] != "content" {
		t.Errorf("inner type = %v, want content", inner["type"])
	}
	innerContent := inner["content"].(map[string]any)
	if innerContent["text"] != "file contents here" {
		t.Errorf("inner text = %v, want %q", innerContent["text"], "file contents here")
	}
}

func TestEventToAcpJsonRpc_toolResult_callIDFallback(t *testing.T) {
	// llama.cpp-style: only call_id set
	ev := &streaming.StreamEvent{
		Type:    streaming.StreamEventToolResult,
		Content: "ok",
		ToolCalls: []tacklr.ToolCall{
			{CallID: "fc_only", Name: "echo", Status: "success"},
		},
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	update := msg["params"].(map[string]any)["update"].(map[string]any)
	if update["toolCallId"] != "fc_only" {
		t.Errorf("toolCallId = %v, want fc_only (CallID fallback)", update["toolCallId"])
	}
}

func TestEventToAcpJsonRpc_functionCall_callIDFallback(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type: streaming.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{
			{CallID: "fc_only", Name: "echo"},
		},
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	update := msg["params"].(map[string]any)["update"].(map[string]any)
	if update["toolCallId"] != "fc_only" {
		t.Errorf("toolCallId = %v, want fc_only", update["toolCallId"])
	}
}

func TestEventToAcpJsonRpc_toolUpdate(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:      streaming.StreamEventToolUpdate,
		MessageID: "tc-1",
		Content:   "processing step 1...",
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames len = %d, want 1", len(frames))
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	params := msg["params"].(map[string]any)
	update := params["update"].(map[string]any)
	if update["sessionUpdate"] != "tool_call_update" {
		t.Errorf("sessionUpdate = %v, want tool_call_update", update["sessionUpdate"])
	}
	if update["status"] != "in_progress" {
		t.Errorf("status = %v, want in_progress", update["status"])
	}
	if update["toolCallId"] != "tc-1" {
		t.Errorf("toolCallId = %v, want tc-1", update["toolCallId"])
	}
	content := update["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("content len = %d, want 1", len(content))
	}
	inner := content[0].(map[string]any)
	if inner["type"] != "content" {
		t.Errorf("inner type = %v, want content", inner["type"])
	}
	innerContent := inner["content"].(map[string]any)
	if innerContent["type"] != "text" {
		t.Errorf("inner content type = %v, want text", innerContent["type"])
	}
	if innerContent["text"] != "processing step 1..." {
		t.Errorf("inner text = %v, want processing step 1...", innerContent["text"])
	}
}

func TestEventToAcpJsonRpc_complete(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:   streaming.StreamEventComplete,
		TurnID: "turn-abc",
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	if msg["id"] != "turn-abc" {
		t.Errorf("id = %v, want turn-abc", msg["id"])
	}
	result := msg["result"].(map[string]any)
	if result["stopReason"] != "end_turn" {
		t.Errorf("stopReason = %v, want end_turn", result["stopReason"])
	}
}

func TestEventToAcpJsonRpc_error(t *testing.T) {
	ev := &streaming.StreamEvent{
		Type:   streaming.StreamEventError,
		TurnID: "turn-err",
		Error:  io.ErrUnexpectedEOF,
	}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var msg map[string]any
	_ = json.Unmarshal(frames[0], &msg)
	errObj := msg["error"].(map[string]any)
	if errObj["code"] != float64(-32603) {
		t.Errorf("error code = %v, want -32603", errObj["code"])
	}
}

func TestEventToAcpJsonRpc_error_stopReasons(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{tacklr.ErrModelRefused, "refusal"},
		{tacklr.ErrMaxTokens, "max_tokens"},
		{tacklr.ErrMaxTurnRequests, "max_turn_requests"},
		{fmt.Errorf("run: context cancelled: %w", context.Canceled), "cancelled"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			frames, err := eventToAcpJsonRpc("t", &streaming.StreamEvent{
				Type:  streaming.StreamEventError,
				Error: tc.err,
			})
			if err != nil {
				t.Fatal(err)
			}
			var msg map[string]any
			_ = json.Unmarshal(frames[0], &msg)
			if msg["error"] != nil {
				t.Fatalf("want result not error: %v", msg)
			}
			res := msg["result"].(map[string]any)
			if res["stopReason"] != tc.want {
				t.Fatalf("stopReason = %v, want %s", res["stopReason"], tc.want)
			}
		})
	}
}

func TestEventToAcpJsonRpc_interrupt_skipped(t *testing.T) {
	ev := &streaming.StreamEvent{Type: streaming.StreamEventInterrupt}
	frames, err := eventToAcpJsonRpc("thread-1", ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("frames = %d, want 0 (skipped)", len(frames))
	}
}

func TestACP_OnStreamClosed_cancelledVsPark(t *testing.T) {
	p := acpProtocol{}
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Conn: &Conn{Writer: w}}
	if err := p.OnStreamClosed(context.Background(), env, "t", json.RawMessage(`1`), true); err != nil {
		t.Fatal(err)
	}
	if len(w.Results) != 1 {
		t.Fatalf("results = %d", len(w.Results))
	}
	w2 := &recordingMessageWriter{}
	env2 := ProtocolEnv{Conn: &Conn{Writer: w2}}
	_ = p.OnStreamClosed(context.Background(), env2, "t", json.RawMessage(`2`), false)
	if len(w2.Errors) != 1 {
		t.Fatalf("errors = %d", len(w2.Errors))
	}
	if err := p.OnStreamClosed(context.Background(), env, "t", nil, false); err != nil {
		t.Fatal(err)
	}
}

func TestEventToAcpJsonRpc_errorWithErrorField(t *testing.T) {
	frames, err := eventToAcpJsonRpc("s1", &streaming.StreamEvent{
		Type:  streaming.StreamEventError,
		Error: errors.New("explode"),
	})
	if err != nil || len(frames) == 0 || !strings.Contains(string(frames[0]), "explode") {
		t.Fatalf("%v %v", err, frames)
	}
}

func TestInjectReqID_nonJSONFrame(t *testing.T) {
	out := injectReqID([][]byte{[]byte("not-json"), []byte(`{"a":1}`)}, json.RawMessage(`7`), true)
	if len(out) != 2 || string(out[0]) != "not-json" {
		t.Fatalf("%v", out)
	}
}

func TestACP_handleHTTP_initialize(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`,
	)))
	acpProtocol{}.handleHTTP(ProtocolEnv{Registry: r, Conn: &Conn{}}, rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "protocolVersion") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestValidateACPRequest_moreParamEdges(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"jsonrpc":"2.0","id":1,"method":"initialize"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":0}}`, "unsupported protocol version"},
		{`{"jsonrpc":"2.0","id":1,"method":"initialize","params":"bad"}`, "invalid initialize params"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/new"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/new","params":"x"}`, "invalid session/new"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/load"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/load","params":"bad"}`, "invalid session/load"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"cwd":"/t"}}`, "sessionId is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/resume"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/resume","params":"bad"}`, "invalid session/resume"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"cwd":"/t"}}`, "sessionId is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/set_config_option"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/set_config_option","params":"bad"}`, "invalid session/set_config_option"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/set_config_option","params":{"sessionId":"s"}}`, "configId is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/close"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/close","params":"bad"}`, "invalid session/close"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/close","params":{}}`, "sessionId is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/prompt"}`, "params is required"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":"bad"}`, "invalid session/prompt"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}`, "prompt must not be empty"},
		{`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[{"type":"text","text":""}]}}`, "non-empty text"},
		{`{"jsonrpc":"2.0","id":1,"method":"authenticate"}`, ""},
		{`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s1"}}`, ""},
	}
	for _, tc := range cases {
		pr, err := validateACPRequest([]byte(tc.body))
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.body, err)
			}
			_ = pr
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err=%v want %q", tc.body, err, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// handleRPC — lifecycle methods
// ---------------------------------------------------------------------------

func TestHandleRPC_initialize(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc = %v, want 2.0", resp["jsonrpc"])
	}
	if resp["id"] != float64(1) {
		t.Errorf("id = %v, want 1", resp["id"])
	}
	result := resp["result"].(map[string]any)
	if result["protocolVersion"] != float64(1) {
		t.Errorf("protocolVersion = %v, want 1", result["protocolVersion"])
	}
	caps := result["agentCapabilities"].(map[string]any)
	if caps["loadSession"] != false {
		t.Errorf("loadSession = %v, want false", caps["loadSession"])
	}
	mcpCaps := caps["mcpCapabilities"].(map[string]any)
	if mcpCaps["http"] != true {
		t.Errorf("mcpCapabilities.http = %v, want true", mcpCaps["http"])
	}
	if mcpCaps["sse"] != true {
		t.Errorf("mcpCapabilities.sse = %v, want true", mcpCaps["sse"])
	}
}

func TestHandleRPC_sessionNew(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	result := resp["result"].(map[string]any)
	sessionID, ok := result["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("expected sessionId in result, got %v", result)
	}
	opts, ok := result["configOptions"].([]any)
	if !ok || len(opts) == 0 {
		t.Fatalf("expected configOptions in result, got %v", result)
	}
	agentOpt := opts[0].(map[string]any)
	if agentOpt["id"] != "model" {
		t.Errorf("configOptions[0].id = %v, want model", agentOpt["id"])
	}
	if agentOpt["currentValue"] != "default" {
		t.Errorf("configOptions[0].currentValue = %v, want default", agentOpt["currentValue"])
	}
}

func TestHandleRPC_sessionNew_storesSessionState(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/home/user","mcpServers":[{"name":"fs","command":"npx"}]}}`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	result := resp["result"].(map[string]any)
	sessionID := result["sessionId"].(string)

	state, ok := r.sessions.Load(sessionID)
	if !ok {
		t.Fatal("expected session state to be stored")
	}
	s := state.(*sessionState)
	if s.cwd != "/home/user" {
		t.Errorf("cwd = %q, want %q", s.cwd, "/home/user")
	}
	if len(s.mcpServers) != 1 || s.mcpServers[0].Name != "fs" || s.mcpServers[0].Command != "npx" {
		t.Errorf("mcpServers = %v, want one stdio server fs/npx", s.mcpServers)
	}
}

func TestHandleRPC_sessionClose_deletesSessionState(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	// Create session first
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	// Close it
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"`+sessionID+`"}}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("close status = %d, want 200", rec2.Code)
	}

	_, ok := r.sessions.Load(sessionID)
	if ok {
		t.Error("expected session state to be deleted after close")
	}
}

func TestHandleRPC_unknownMethod(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{"sessionId":"s1"}}`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	errObj := resp["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "method not found") {
		t.Errorf("error message = %v, want to contain %q", msg, "method not found")
	}
	if code, _ := errObj["code"].(float64); int(code) != jsonRPCCodeMethodNotFound {
		t.Errorf("error code = %v, want %d", errObj["code"], jsonRPCCodeMethodNotFound)
	}
}

func TestHandleRPC_invalidRequest(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `not json`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["error"] == nil {
		t.Error("expected error response for invalid JSON")
	}
}

func TestHandleRPC_sessionLoad(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sessionID+`","cwd":"/tmp"}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["error"] != nil {
		t.Fatalf("unexpected error: %v", resp2["error"])
	}
	result := resp2["result"].(map[string]any)
	if result["sessionId"] != sessionID {
		t.Errorf("sessionId = %v, want %s", result["sessionId"], sessionID)
	}
	opts, ok := result["configOptions"].([]any)
	if !ok || len(opts) == 0 {
		t.Fatalf("expected configOptions in result, got %v", result)
	}
}

func TestHandleRPC_sessionLoad_updatesSessionMCPServers(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sessionID+`","cwd":"/tmp","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[{"name":"Authorization","value":"Bearer tok"}]}]}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["error"] != nil {
		t.Fatalf("unexpected error: %v", resp2["error"])
	}

	state, ok := r.sessions.Load(sessionID)
	if !ok {
		t.Fatal("expected session state to be stored")
	}
	s := state.(*sessionState)
	if s.cwd != "/tmp" {
		t.Errorf("cwd = %q, want /tmp (unchanged)", s.cwd)
	}
	if len(s.mcpServers) != 1 || s.mcpServers[0].Type != "http" || s.mcpServers[0].URL != "https://api.example.com/mcp" {
		t.Errorf("mcpServers = %v, want one http server", s.mcpServers)
	}
}

func TestHandleRPC_sessionLoad_cwdMismatch(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sessionID+`","cwd":"/proj"}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["error"] == nil {
		t.Fatal("expected error for cwd mismatch")
	}
	if !strings.Contains(resp2["error"].(map[string]any)["message"].(string), "cwd") {
		t.Errorf("error = %v, want cwd mismatch message", resp2["error"])
	}
}

func TestHandleRPC_sessionLoad_notFound(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"missing"}}`)
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	errObj := resp["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "session") {
		t.Errorf("error message = %v, want to mention session", errObj["message"])
	}
}

func TestHandleRPC_noAgentConfigured_onPrompt(t *testing.T) {
	store := testStore(t)
	r := NewRegistry(store, "") // no default agent

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	if resp1["error"] != nil {
		t.Fatalf("session/new should succeed without default agent: %v", resp1["error"])
	}
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	promptBody := `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	errObj := resp2["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "no agent configured") {
		t.Errorf("error message = %v, want to contain %q", errObj["message"], "no agent configured")
	}
}

// ---------------------------------------------------------------------------
// handleRPC — session/prompt (streaming)
// ---------------------------------------------------------------------------

func TestHandleRPC_sessionPrompt_streamsEvents(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	// Create session
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	// Send prompt
	promptBody := `{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)

	frames := parseACPFrames(t, rec2.Body)
	if len(frames) == 0 {
		t.Fatal("expected at least one frame")
	}

	// Should have session/update notifications followed by a result
	var hasUpdate, hasResult bool
	for _, f := range frames {
		if f["method"] == "session/update" {
			hasUpdate = true
		}
		if f["result"] != nil {
			hasResult = true
		}
	}
	if !hasUpdate {
		t.Error("expected at least one session/update notification")
	}
	if !hasResult {
		t.Error("expected a result frame")
	}
}

func TestHandleRPC_sessionPrompt_clientTurnID(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	// Create session
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	// Send prompt with a specific numeric request ID
	promptBody := `{"jsonrpc":"2.0","id":42,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)

	frames := parseACPFrames(t, rec2.Body)

	// The final result frame must echo the client's request ID (42), not the
	// internal turn UUID.
	var resultFrame map[string]any
	for _, f := range frames {
		if f["result"] != nil {
			resultFrame = f
		}
	}
	if resultFrame == nil {
		t.Fatal("no result frame found")
	}
	if resultFrame["id"] != float64(42) {
		t.Errorf("result id = %v, want 42 (client request ID)", resultFrame["id"])
	}
}

func TestHandleRPC_sessionPrompt_stringTurnID(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	// Use a string ID to verify raw JSON passthrough
	promptBody := `{"jsonrpc":"2.0","id":"req-abc","method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)

	frames := parseACPFrames(t, rec2.Body)
	var resultFrame map[string]any
	for _, f := range frames {
		if f["result"] != nil {
			resultFrame = f
		}
	}
	if resultFrame == nil {
		t.Fatal("no result frame found")
	}
	if resultFrame["id"] != "req-abc" {
		t.Errorf("result id = %v, want %q (client request ID)", resultFrame["id"], "req-abc")
	}
}

// TestHandleRPC_sessionPrompt_toolTitleAndName: ACP tool_call carries programmatic
// name and a DisplayName template resolved from args (title), not Name-as-title only.
func TestHandleRPC_sessionPrompt_toolTitleAndName(t *testing.T) {
	store := testStore(t)
	mark := tacklr.NewTool(tacklr.ToolConfig{
		Name:        "mark_item",
		DisplayName: "Complete {title}",
		Category:    "think",
		Handler: func(ctx context.Context, args struct {
			Title string `json:"title"`
		}) (string, error) {
			return "ok:" + args.Title, nil
		},
	})
	var strategy *mockInferenceStrategy
	strategy = &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if strategy.callNum.Load() > 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "call_mark", CallID: "call_mark", Name: "mark_item",
					Arguments: `{"title":"Ship release"}`,
				}},
				IsComplete: true,
			}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{mark})
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"mark"}]}}`)
	frames := parseACPFrames(t, rec2.Body)

	var sawCall bool
	for _, f := range frames {
		if f["method"] != "session/update" {
			continue
		}
		update := f["params"].(map[string]any)["update"].(map[string]any)
		if update["sessionUpdate"] != "tool_call" {
			continue
		}
		if update["name"] != "mark_item" {
			t.Fatalf("name = %v, want mark_item", update["name"])
		}
		if update["title"] != "Complete Ship release" {
			t.Fatalf("title = %v, want Complete Ship release", update["title"])
		}
		if update["kind"] != "think" {
			t.Fatalf("kind = %v", update["kind"])
		}
		sawCall = true
	}
	if !sawCall {
		t.Fatal("expected tool_call with title+name")
	}
}

func TestHandleRPC_sessionPrompt_toolProgress(t *testing.T) {
	store := testStore(t)

	progressTool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "progress_demo",
		Handler: func(ctx context.Context, _ struct{}, runtime *tacklr.HarnessRuntime) (string, error) {
			runtime.EmitUpdate("starting work...")
			runtime.EmitUpdate("50% complete")
			runtime.EmitUpdate("almost done")
			return "task complete!", nil
		},
	})

	var strategy *mockInferenceStrategy
	strategy = &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if strategy.callNum.Load() > 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventFunctionCall,
				ToolCalls:  []tacklr.ToolCall{{ID: "call_progress", CallID: "call_progress", Name: "progress_demo", Arguments: `{}`}},
				IsComplete: true,
			}
			ch <- tacklr.LLMResponseChunk{IsComplete: true}
		},
	}

	r := newTestRegistry(store, strategy, []*tacklr.Tool{progressTool})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	promptBody := `{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"run demo"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)
	frames := parseACPFrames(t, rec2.Body)

	var toolCallInProgress bool
	var toolCallCompleted bool
	var toolCallUpdateCount int
	for _, f := range frames {
		if f["method"] != "session/update" {
			continue
		}
		params := f["params"].(map[string]any)
		update := params["update"].(map[string]any)
		switch update["sessionUpdate"].(string) {
		case "tool_call":
			switch update["status"].(string) {
			case "in_progress":
				toolCallInProgress = true
			case "completed":
				toolCallCompleted = true
			}
		case "tool_call_update":
			toolCallUpdateCount++
			if s, _ := update["status"].(string); s == "completed" {
				toolCallCompleted = true
			}
		}
	}

	if !toolCallInProgress {
		t.Error("expected tool_call with in_progress status")
	}
	if toolCallUpdateCount < 1 {
		t.Errorf("expected at least 1 tool_call_update, got %d", toolCallUpdateCount)
	}
	if !toolCallCompleted {
		t.Error("expected tool_call_update with completed status")
	}

	var hasResult bool
	for _, f := range frames {
		if f["result"] != nil {
			hasResult = true
			break
		}
	}
	if !hasResult {
		t.Error("expected a result frame")
	}
}

// ---------------------------------------------------------------------------
// handleRPC — session/set_config_option
// ---------------------------------------------------------------------------

func TestHandleRPC_configSet_agent(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := NewRegistry(store, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "default"},
		Model:  strategy,
	})
	r.Register("custom", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "custom"},
		Model:  strategy,
	})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"model","value":"custom"}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["error"] != nil {
		t.Fatalf("unexpected error: %v", resp2["error"])
	}
	result := resp2["result"].(map[string]any)
	opts := result["configOptions"].([]any)
	agentOpt := opts[0].(map[string]any)
	if agentOpt["currentValue"] != "custom" {
		t.Errorf("currentValue = %v, want custom", agentOpt["currentValue"])
	}

	state, _ := r.sessions.Load(sessionID)
	sess := state.(*sessionState)
	if sess.configValues["agent"] != "custom" {
		t.Errorf("configValues[agent] = %q, want custom", sess.configValues["agent"])
	}
}

func TestHandleRPC_configSet_unknownAgent(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"model","value":"missing"}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	errObj := resp2["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "not found") {
		t.Errorf("error message = %v, want to contain not found", errObj["message"])
	}
}

func TestHandleRPC_configSet_unknownConfigID(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"unknown","value":"x"}}`)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	errObj := resp2["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "unknown configId") {
		t.Errorf("error message = %v, want to contain unknown configId", errObj["message"])
	}
}

func TestHandleRPC_sessionPrompt_usesConfigAgent(t *testing.T) {
	store := testStore(t)
	var customInvoked bool
	r := NewRegistry(store, "default")
	r.Register("default", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "default-prompt"},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-default", IsComplete: true}
			},
		},
	})
	r.Register("custom", AgentSpec{
		Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "custom-prompt"},
		Model: &mockInferenceStrategy{
			invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				customInvoked = true
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-custom", IsComplete: true}
			},
		},
	})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"model","value":"custom"}}`)

	promptBody := `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec3 := serveACPRaw(t, r, promptBody)
	frames := parseACPFrames(t, rec3.Body)
	var hasResult bool
	var sawCustomContent bool
	for _, f := range frames {
		if f["result"] != nil {
			hasResult = true
		}
		if f["error"] != nil {
			t.Fatalf("unexpected error frame: %v", f["error"])
		}
		if f["method"] == "session/update" {
			params := f["params"].(map[string]any)
			update := params["update"].(map[string]any)
			if content, ok := update["content"].(map[string]any); ok && content["text"] == "from-custom" {
				sawCustomContent = true
			}
		}
	}
	if !hasResult {
		t.Fatal("expected result frame")
	}
	if !customInvoked {
		t.Error("expected custom agent model to be invoked")
	}
	if !sawCustomContent {
		t.Error("expected streamed content from custom agent")
	}
}

// ---------------------------------------------------------------------------
// ServeStdio (stdio transport)
// ---------------------------------------------------------------------------

func TestServeStdio_lifecycleAndPrompt(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`,
		`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"name":"fs","command":"/bin/true","args":[]}]}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := NewServer(r, ACP).ServeStdio(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}

	frames := parseACPFrames(t, &out)
	if len(frames) < 2 {
		t.Fatalf("expected at least 2 frames, got %d: %v", len(frames), frames)
	}

	var initResult, newResult map[string]any
	var sessionID string
	for _, f := range frames {
		res, ok := f["result"].(map[string]any)
		if !ok {
			continue
		}
		if f["id"] == float64(1) {
			initResult = res
		}
		if f["id"] == float64(2) {
			newResult = res
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
		}
	}
	if initResult == nil {
		t.Fatalf("no initialize result in frames: %v", frames)
	}
	if initResult["protocolVersion"] != float64(1) {
		t.Errorf("protocolVersion = %v, want 1", initResult["protocolVersion"])
	}
	if sessionID == "" {
		t.Fatalf("session/new missing sessionId: %v", newResult)
	}
	opts, ok := newResult["configOptions"].([]any)
	if !ok || len(opts) == 0 {
		t.Fatalf("session/new missing configOptions: %v", newResult)
	}

	// Second IO pass: set agent + prompt against the live session state.
	in2 := strings.Join([]string{
		`{"jsonrpc":"2.0","id":3,"method":"session/set_config_option","params":{"sessionId":"` + sessionID + `","configId":"model","value":"default"}}`,
		`{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`,
	}, "\n") + "\n"
	var out2 bytes.Buffer
	if err := NewServer(r, ACP).ServeStdio(context.Background(), strings.NewReader(in2), &out2); err != nil {
		t.Fatalf("ServeStdio prompt pass: %v", err)
	}
	frames2 := parseACPFrames(t, &out2)
	var hasUpdate, hasResult bool
	for _, f := range frames2 {
		if f["method"] == "session/update" {
			hasUpdate = true
		}
		if f["result"] != nil && f["id"] == float64(4) {
			hasResult = true
		}
		if f["error"] != nil && f["id"] == float64(4) {
			t.Fatalf("prompt error: %v", f["error"])
		}
	}
	if !hasUpdate {
		t.Error("expected session/update from prompt")
	}
	if !hasResult {
		t.Error("expected prompt result frame with id 4")
	}
}

func TestServeStdio_contextCancel(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	srv := NewServer(r, ACP)

	pr, pw := io.Pipe()
	defer pr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ServeStdio(ctx, pr, io.Discard)
	}()

	// Ensure the server is blocked on a read, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeStdio error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio did not return after cancel")
	}
	_ = pw.Close()
}

func TestHandleMessage_initialize_recordingWriter(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)
	srv := NewServer(r, ACP)
	rec := &recordingMessageWriter{}

	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	srv.HandleMessage(context.Background(), body, rec)

	if len(rec.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", rec.Errors)
	}
	if len(rec.Results) != 1 {
		t.Fatalf("results = %d, want 1", len(rec.Results))
	}
	result, ok := rec.Results[0].Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", rec.Results[0].Result)
	}
	if result["protocolVersion"] != 1 && result["protocolVersion"] != float64(1) {
		t.Errorf("protocolVersion = %v, want 1", result["protocolVersion"])
	}
}

// TestACP_sessionCancel_midPrompt is the integration coverage for session/cancel:
// mid-stream cancel ends the prompt with stopReason cancelled promptly and
// keeps the session registered for later use.
func TestACP_sessionCancel_midPrompt(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	const earlyText = "streaming-early"
	var chunksSent atomic.Int64

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			startedOnce.Do(func() { close(started) })
			// Stream until ctx is cancelled (session/cancel). If cancel is ignored
			// the prompt goroutine will not finish within the test deadline.
			for {
				select {
				case <-ctx.Done():
					return
				case ch <- tacklr.LLMResponseChunk{
					Type:       tacklr.StreamEventMessage,
					MessageId:  "m1",
					Content:    earlyText,
					IsComplete: false,
				}:
					chunksSent.Add(1)
				}
			}
		},
	}
	store := testStore(t)
	r := newTestRegistry(store, strategy, nil)
	srv := NewServer(r, ACP)

	recNew := &recordingMessageWriter{}
	srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`), recNew)
	if len(recNew.Results) != 1 {
		t.Fatalf("session/new results = %d", len(recNew.Results))
	}
	sessionID := recNew.Results[0].Result.(map[string]any)["sessionId"].(string)

	recPrompt := &recordingMessageWriter{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := []byte(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
		srv.HandleMessage(context.Background(), body, recPrompt)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not start")
	}

	// Wait until the client has received at least one agent_message_chunk, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first stream frame")
		}
		if messageChunkCount(recPrompt) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	framesBeforeCancel := messageChunkCount(recPrompt)

	srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"`+sessionID+`"}}`), &recordingMessageWriter{})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not finish after cancel")
	}

	// Desired: prompt result is stopReason cancelled.
	var cancelled bool
	for _, res := range recPrompt.Results {
		switch m := res.Result.(type) {
		case map[string]string:
			if m["stopReason"] == "cancelled" {
				cancelled = true
			}
		case map[string]any:
			if m["stopReason"] == "cancelled" {
				cancelled = true
			}
		}
	}
	if !cancelled {
		t.Fatalf("want stopReason cancelled, results=%#v frames=%v", recPrompt.Results, recPrompt.FramesAsMaps(t))
	}

	// Desired: streaming was underway and cancel stops the model (stable send count).
	if framesBeforeCancel < 1 {
		t.Fatal("expected agent_message_chunk before cancel")
	}
	sentAtDone := chunksSent.Load()
	time.Sleep(30 * time.Millisecond)
	sentLater := chunksSent.Load()
	// After the prompt finishes, the model must not keep producing forever.
	if sentLater > sentAtDone+8 {
		t.Fatalf("model still sending after cancel settled: at_done=%d later=%d frames=%d",
			sentAtDone, sentLater, messageChunkCount(recPrompt))
	}

	// Desired: session remains registered after cancel.
	if _, ok := r.sessions.Load(sessionID); !ok {
		t.Fatal("session should remain registered after cancel")
	}
	// Approach A: empty checkpoint exists from session/new so load is real, not only fallback.
	if _, err := store.LoadSession(context.Background(), sessionID); err != nil {
		t.Fatalf("empty checkpoint should exist after session/new: %v", err)
	}

	// Desired: a subsequent prompt on the same session completes normally.
	strategy.invokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventMessage,
			MessageId:  "m2",
			Content:    "after-cancel",
			IsComplete: true,
		}
	}
	recPrompt2 := &recordingMessageWriter{}
	body2 := []byte(`{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"again"}]}}`)
	srv.HandleMessage(context.Background(), body2, recPrompt2)

	if len(recPrompt2.Errors) > 0 {
		t.Fatalf("post-cancel prompt errors: %#v", recPrompt2.Errors)
	}
	var endTurn bool
	var sawAfter bool
	for _, frame := range recPrompt2.FramesAsMaps(t) {
		if res, ok := frame["result"].(map[string]any); ok && res["stopReason"] == "end_turn" {
			endTurn = true
		}
		if frame["method"] != "session/update" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		update, _ := params["update"].(map[string]any)
		if update["sessionUpdate"] != "agent_message_chunk" {
			continue
		}
		if content, ok := update["content"].(map[string]any); ok && content["text"] == "after-cancel" {
			sawAfter = true
		}
	}
	for _, res := range recPrompt2.Results {
		switch m := res.Result.(type) {
		case map[string]string:
			if m["stopReason"] == "end_turn" {
				endTurn = true
			}
		case map[string]any:
			if m["stopReason"] == "end_turn" {
				endTurn = true
			}
		}
	}
	if !endTurn {
		t.Fatalf("post-cancel prompt want stopReason end_turn, results=%#v frames=%v", recPrompt2.Results, recPrompt2.FramesAsMaps(t))
	}
	if !sawAfter {
		t.Fatal("post-cancel prompt should stream new content")
	}
}

func messageChunkCount(w *recordingMessageWriter) int {
	n := 0
	for _, f := range w.SnapshotFrames() {
		var frame map[string]any
		if json.Unmarshal(f, &frame) != nil {
			continue
		}
		if frame["method"] != "session/update" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		update, _ := params["update"].(map[string]any)
		if update["sessionUpdate"] == "agent_message_chunk" {
			n++
		}
	}
	return n
}
