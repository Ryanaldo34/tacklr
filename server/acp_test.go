package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	r.HandleRPC(rec, req, handlers[ProtocolACP], validators[ProtocolACP])
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
	// ACP stdio MCP server shapes differ from our MCPConfig; cwd must still parse.
	body := []byte(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"name":"fs","command":"npx","args":["-y","@modelcontextprotocol/server-filesystem"]}]}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.CWD != "/tmp" {
		t.Errorf("cwd = %q, want /tmp", pr.CWD)
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
	body := []byte(`{"jsonrpc":"2.0","id":4,"method":"session/resume","params":{"sessionId":"sess-2"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-2" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-2")
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
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{"sessionId":"s1"}}`)
	_, err := validateACPRequest(body)
	if err == nil {
		t.Fatal("expected error for unsupported method")
	}
	if !strings.Contains(err.Error(), "unsupported method") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "unsupported method")
	}
}

func TestValidateACPRequest_sessionLoad(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"session/load","params":{"sessionId":"sess-load"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-load" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-load")
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
	body := []byte(`{"jsonrpc":"2.0","id":8,"method":"session/set_config_option","params":{"sessionId":"sess-1","configId":"agent","value":"custom"}}`)
	pr, err := validateACPRequest(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.ThreadID != "sess-1" {
		t.Errorf("threadID = %q, want %q", pr.ThreadID, "sess-1")
	}
	if pr.ConfigID != "agent" {
		t.Errorf("configId = %q, want %q", pr.ConfigID, "agent")
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
			{ID: "tc-1", Name: "read_file", Category: "read"},
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
	if update["status"] != "completed" {
		t.Errorf("status = %v, want completed", update["status"])
	}
	if update["content"] != "file contents here" {
		t.Errorf("content = %v, want %q", update["content"], "file contents here")
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

func TestEventToAcpJsonRpc_interrupt_returnsError(t *testing.T) {
	ev := &streaming.StreamEvent{Type: streaming.StreamEventInterrupt}
	_, err := eventToAcpJsonRpc("thread-1", ev)
	if err == nil {
		t.Fatal("expected error for interrupt event")
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
	if agentOpt["id"] != "agent" {
		t.Errorf("configOptions[0].id = %v, want agent", agentOpt["id"])
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
}

func TestHandleRPC_notification_noResponse(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s1"}}`)
	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body for notification, got %q", rec.Body.String())
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

func TestHandleRPC_sessionCancel_preservesSessionState(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/cancel","params":{"sessionId":"`+sessionID+`"}}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", rec2.Code)
	}

	_, ok := r.sessions.Load(sessionID)
	if !ok {
		t.Error("expected session state to be preserved after cancel")
	}
}

func TestHandleRPC_unknownMethod(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{"sessionId":"s1"}}`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	errObj := resp["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "unsupported method") {
		t.Errorf("error message = %v, want to contain %q", errObj["message"], "unsupported method")
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

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sessionID+`"}}`)
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

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"agent","value":"custom"}}`)
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

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"agent","value":"missing"}}`)
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

	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"model","value":"x"}}`)
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

	serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sessionID+`","configId":"agent","value":"custom"}}`)

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
// ServeACPIO (stdio transport)
// ---------------------------------------------------------------------------

func TestServeACPIO_lifecycleAndPrompt(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello", IsComplete: true}
		},
	}
	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`,
		`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[{"name":"fs","command":"npx","args":[]}]}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := r.ServeACPIO(strings.NewReader(in), &out); err != nil {
		t.Fatalf("ServeACPIO: %v", err)
	}

	frames := parseACPFrames(t, &out)
	if len(frames) < 2 {
		t.Fatalf("expected at least 2 frames, got %d: %v", len(frames), frames)
	}
	if frames[0]["result"] == nil {
		t.Fatalf("initialize missing result: %v", frames[0])
	}
	initResult := frames[0]["result"].(map[string]any)
	if initResult["protocolVersion"] != float64(1) {
		t.Errorf("protocolVersion = %v, want 1", initResult["protocolVersion"])
	}

	newResult := frames[1]["result"].(map[string]any)
	sessionID, ok := newResult["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("session/new missing sessionId: %v", newResult)
	}
	opts, ok := newResult["configOptions"].([]any)
	if !ok || len(opts) == 0 {
		t.Fatalf("session/new missing configOptions: %v", newResult)
	}

	// Second IO pass: set agent + prompt against the live session state.
	in2 := strings.Join([]string{
		`{"jsonrpc":"2.0","id":3,"method":"session/set_config_option","params":{"sessionId":"` + sessionID + `","configId":"agent","value":"default"}}`,
		`{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`,
	}, "\n") + "\n"
	var out2 bytes.Buffer
	if err := r.ServeACPIO(strings.NewReader(in2), &out2); err != nil {
		t.Fatalf("ServeACPIO prompt pass: %v", err)
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
