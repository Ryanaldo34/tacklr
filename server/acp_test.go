package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/durable"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

// acpTestServer is an isolated ACP server (own ProtocolWireStore) for multi-step RPC tests.
type acpTestServer struct {
	t     *testing.T
	r     *testRuntime
	proto Protocol
	wire  ProtocolWireStore
}

func newACPTestServer(t *testing.T, r *testRuntime) *acpTestServer {
	t.Helper()
	wire := NewMemoryWireStore()
	return &acpTestServer{t: t, r: r, proto: NewACPProtocol(wire), wire: wire}
}

func newACPTestServerWithWire(t *testing.T, r *testRuntime, wire ProtocolWireStore) *acpTestServer {
	t.Helper()
	return &acpTestServer{t: t, r: r, proto: NewACPProtocol(wire), wire: wire}
}

func (s *acpTestServer) rpc(body string) *httptest.ResponseRecorder {
	s.t.Helper()
	return serveACPInbound(s.t, s.r, s.proto, body)
}

// acpByRuntime returns a stable ACP protocol per kernel so multi-step
// serveACPRaw calls share wire state.
var acpByRuntime sync.Map // *testRuntime → Protocol

func acpProtocolFor(r *testRuntime) Protocol {
	if r == nil {
		return NewACPProtocol(NewMemoryWireStore())
	}
	if v, ok := acpByRuntime.Load(r); ok {
		return v.(Protocol)
	}
	p := NewACPProtocol(NewMemoryWireStore())
	actual, _ := acpByRuntime.LoadOrStore(r, p)
	return actual.(Protocol)
}

func serveACPInbound(t *testing.T, r *testRuntime, proto Protocol, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mw := &jsonRPCMessageWriter{w: rec}
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{Writer: mw}}
	_ = proto.HandleInbound(t.Context(), env, []byte(body))
	return rec
}

// serveACPRaw runs one RPC against a per-kernel isolated ACP protocol.
func serveACPRaw(t *testing.T, r *testRuntime, body string) *httptest.ResponseRecorder {
	t.Helper()
	return serveACPInbound(t, r, acpProtocolFor(r), body)
}

func acpRPCResult(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	if errObj, ok := resp["error"]; ok && errObj != nil {
		t.Fatalf("unexpected error: %v", errObj)
	}
	res, _ := resp["result"].(map[string]any)
	return res
}

func acpRPCError(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	errObj, _ := resp["error"].(map[string]any)
	if errObj == nil {
		t.Fatalf("expected error, got %v", resp)
	}
	return errObj
}

// ---------------------------------------------------------------------------
// validateACPRequest
// ---------------------------------------------------------------------------

// TestValidateACPRequest_outcomes covers each distinct validateACPRequest return
// path once (success shapes + error strings). Integration HandleRPC tests exercise
// the same code via the wire; this keeps parse-edge coverage without N micro-tests.
func TestValidateACPRequest_outcomes(t *testing.T) {
	type wantOK struct {
		method       string
		threadID     string
		prompt       string
		cwd          string
		notification bool
		configID     string
		configValue  string
		mcpLen       int
	}
	okCases := []struct {
		name string
		body string
		want wantOK
	}{
		{"initialize", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`,
			wantOK{method: "initialize"}},
		{"initialize higher version", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":99}}`,
			wantOK{method: "initialize"}},
		{"authenticate", `{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{"methodId":"agent-login"}}`,
			wantOK{method: "authenticate"}},
		{"session/new", `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/home/user"}}`,
			wantOK{method: "session/new", cwd: "/home/user"}},
		{"session/new mcp", `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[
			{"name":"fs","command":"npx","args":["-y","pkg"],"env":[{"name":"API_KEY","value":"secret"}]},
			{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[{"name":"Authorization","value":"Bearer tok"}]},
			{"type":"sse","name":"events","url":"https://events.example.com/mcp","headers":[]}
		]}}`, wantOK{method: "session/new", cwd: "/tmp", mcpLen: 3}},
		{"session/prompt", `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess-1","prompt":[{"type":"text","text":"hello"}]}}`,
			wantOK{method: "session/prompt", threadID: "sess-1", prompt: "hello"}},
		{"session/resume", `{"jsonrpc":"2.0","id":4,"method":"session/resume","params":{"sessionId":"sess-2","cwd":"/tmp","mcpServers":[{"name":"fs","command":"npx","args":[]}]}}`,
			wantOK{method: "session/resume", threadID: "sess-2", mcpLen: 1}},
		{"session/close", `{"jsonrpc":"2.0","id":5,"method":"session/close","params":{"sessionId":"sess-3"}}`,
			wantOK{method: "session/close", threadID: "sess-3"}},
		{"session/cancel notification", `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s1"}}`,
			wantOK{method: "session/cancel", threadID: "s1", notification: true}},
		{"session/load", `{"jsonrpc":"2.0","id":7,"method":"session/load","params":{"sessionId":"sess-load","cwd":"/proj","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[]}]}}`,
			wantOK{method: "session/load", threadID: "sess-load", cwd: "/proj", mcpLen: 1}},
		{"config set", `{"jsonrpc":"2.0","id":8,"method":"session/set_config_option","params":{"sessionId":"sess-1","configId":"model","value":"custom"}}`,
			wantOK{method: "session/set_config_option", threadID: "sess-1", configID: "model", configValue: "custom"}},
		{"unknown method admitted", `{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{"sessionId":"s1"}}`,
			wantOK{method: "session/foo"}},
	}
	for _, tc := range okCases {
		t.Run("ok/"+tc.name, func(t *testing.T) {
			pr, err := validateACPRequest([]byte(tc.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pr.Method != tc.want.method {
				t.Errorf("method = %q, want %q", pr.Method, tc.want.method)
			}
			if tc.want.threadID != "" && pr.ThreadID != tc.want.threadID {
				t.Errorf("threadID = %q, want %q", pr.ThreadID, tc.want.threadID)
			}
			if tc.want.prompt != "" && pr.Prompt != tc.want.prompt {
				t.Errorf("prompt = %q, want %q", pr.Prompt, tc.want.prompt)
			}
			if tc.want.cwd != "" && pr.CWD != tc.want.cwd {
				t.Errorf("cwd = %q, want %q", pr.CWD, tc.want.cwd)
			}
			if pr.Notification != tc.want.notification {
				t.Errorf("notification = %v, want %v", pr.Notification, tc.want.notification)
			}
			if tc.want.configID != "" && pr.ConfigID != tc.want.configID {
				t.Errorf("configId = %q", pr.ConfigID)
			}
			if tc.want.configValue != "" && pr.ConfigValue != tc.want.configValue {
				t.Errorf("configValue = %q", pr.ConfigValue)
			}
			if tc.want.mcpLen > 0 && len(pr.MCPServers) != tc.want.mcpLen {
				t.Errorf("mcpServers len = %d, want %d", len(pr.MCPServers), tc.want.mcpLen)
			}
		})
	}

	errCases := []struct {
		name string
		body string
		want string
	}{
		{"invalid json", `not json`, "invalid"},
		{"wrong version", `{"jsonrpc":"1.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`, "jsonrpc version must be"},
		{"missing method", `{"jsonrpc":"2.0","id":1}`, "method"},
		{"init no params", `{"jsonrpc":"2.0","id":1,"method":"initialize"}`, "params is required"},
		{"init version 0", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":0}}`, "unsupported protocol version"},
		{"init bad params", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":"bad"}`, "invalid initialize params"},
		{"new no params", `{"jsonrpc":"2.0","id":1,"method":"session/new"}`, "params is required"},
		{"new bad params", `{"jsonrpc":"2.0","id":1,"method":"session/new","params":"x"}`, "invalid session/new"},
		{"load no params", `{"jsonrpc":"2.0","id":1,"method":"session/load"}`, "params is required"},
		{"load bad params", `{"jsonrpc":"2.0","id":1,"method":"session/load","params":"bad"}`, "invalid session/load"},
		{"load missing id", `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"cwd":"/t"}}`, "sessionId is required"},
		{"resume no params", `{"jsonrpc":"2.0","id":1,"method":"session/resume"}`, "params is required"},
		{"resume bad params", `{"jsonrpc":"2.0","id":1,"method":"session/resume","params":"bad"}`, "invalid session/resume"},
		{"resume missing id", `{"jsonrpc":"2.0","id":1,"method":"session/resume","params":{"cwd":"/t"}}`, "sessionId is required"},
		{"config no params", `{"jsonrpc":"2.0","id":1,"method":"session/set_config_option"}`, "params is required"},
		{"config bad params", `{"jsonrpc":"2.0","id":1,"method":"session/set_config_option","params":"bad"}`, "invalid session/set_config_option"},
		{"config missing id", `{"jsonrpc":"2.0","id":1,"method":"session/set_config_option","params":{"sessionId":"s"}}`, "configId is required"},
		{"close no params", `{"jsonrpc":"2.0","id":1,"method":"session/close"}`, "params is required"},
		{"close bad params", `{"jsonrpc":"2.0","id":1,"method":"session/close","params":"bad"}`, "invalid session/close"},
		{"close missing id", `{"jsonrpc":"2.0","id":1,"method":"session/close","params":{}}`, "sessionId is required"},
		{"prompt no params", `{"jsonrpc":"2.0","id":1,"method":"session/prompt"}`, "params is required"},
		{"prompt bad params", `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":"bad"}`, "invalid session/prompt"},
		{"prompt missing session", `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"prompt":[{"type":"text","text":"hi"}]}}`, "sessionId is required"},
		{"prompt empty", `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[]}}`, "prompt must not be empty"},
		{"prompt empty text", `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"sessionId":"s","prompt":[{"type":"text","text":""}]}}`, "non-empty text"},
	}
	for _, tc := range errCases {
		t.Run("err/"+tc.name, func(t *testing.T) {
			_, err := validateACPRequest([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want containing %q", err, tc.want)
			}
		})
	}
}

// TestParseACPPrompt_outcomes covers text/resource/image/pdf success and error paths once.
func TestParseACPPrompt_outcomes(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr string
	}{
		{"text only", `[{"type":"text","text":"hello"},{"type":"text","text":"world"}]`, "hello\n\nworld", ""},
		{"resource only", `[{"type":"resource","resource":{"uri":"file:///a.txt","mimeType":"text/plain","text":"file content"}}]`, "file content", ""},
		{"mixed", `[{"type":"text","text":"prompt"},{"type":"resource","resource":{"uri":"file:///b.txt","mimeType":"text/plain","text":"ctx"}}]`, "prompt\n\nctx", ""},
		{"empty text", `[{"type":"text","text":""}]`, "", "empty"},
		{"image needs mime", `[{"type":"image","data":"AAAA"}]`, "", "mimeType"},
		{"resource_link needs name", `[{"type":"resource_link","uri":"file:///x"}]`, "", "name is required"},
		{"resource_link ok", `[{"type":"resource_link","uri":"file:///x.pdf","name":"x.pdf","mimeType":"application/pdf","title":"T","description":"D","size":12}]`, "[Resource link] name=x.pdf uri=file:///x.pdf mimeType=application/pdf size=12 title=T\nD", ""},
		{"image not image mime", `[{"type":"image","mimeType":"application/pdf","data":"AAAA"}]`, "", "must be image/"},
		{"image bad uri", `[{"type":"image","mimeType":"image/png","uri":"file://x"}]`, "", "uri must be"},
		{"resource missing field", `[{"type":"resource"}]`, "", "resource field"},
		{"resource empty", `[{"type":"resource","resource":{"uri":"file:///a"}}]`, "", "text or blob"},
		{"audio block", `[{"type":"audio"}]`, "", "audio"},
		{"unknown block", `[{"type":"foo"}]`, "", "unsupported"},
		{"invalid array", `{}`, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := parseACPPrompt(json.RawMessage(tc.raw))
			if tc.wantErr != "" || tc.name == "invalid array" {
				if err == nil {
					t.Fatal("expected error")
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if msg.Content != tc.want {
				t.Fatalf("got %q want %q", msg.Content, tc.want)
			}
		})
	}
	t.Run("image+text", func(t *testing.T) {
		msg, err := parseACPPrompt(json.RawMessage(`[{"type":"text","text":"see"},{"type":"image","mimeType":"image/png","data":"AAAA"}]`))
		if err != nil {
			t.Fatal(err)
		}
		// Text stays on Content; only binary parts on ContentParts.
		if msg.Content != "see" || len(msg.ContentParts) != 1 {
			t.Fatalf("content=%q parts=%d", msg.Content, len(msg.ContentParts))
		}
		mimes := msg.MIMETypes()
		if len(mimes) != 1 || mimes[0] != "image/png" {
			t.Fatalf("mimes=%v", mimes)
		}
	})
	t.Run("pdf blob", func(t *testing.T) {
		msg, err := parseACPPrompt(json.RawMessage(`[{"type":"resource","resource":{"uri":"file:///a.pdf","mimeType":"application/pdf","blob":"JVBERg=="}}]`))
		if err != nil {
			t.Fatal(err)
		}
		if len(msg.ContentParts) != 1 || msg.ContentParts[0].Type != tacklr.ContentTypeInputFile {
			t.Fatalf("parts=%+v", msg.ContentParts)
		}
	})
}

// TestPresentationToACP_outcomes maps each stream event kind once (including skip paths).
func TestPresentationToACP_outcomes(t *testing.T) {
	mustUpdate := func(t *testing.T, frames [][]byte, sessionUpdate string) map[string]any {
		t.Helper()
		if len(frames) == 0 {
			t.Fatal("no frames")
		}
		var msg map[string]any
		if err := json.Unmarshal(frames[0], &msg); err != nil {
			t.Fatal(err)
		}
		update := msg["params"].(map[string]any)["update"].(map[string]any)
		if sessionUpdate != "" && update["sessionUpdate"] != sessionUpdate {
			t.Fatalf("sessionUpdate=%v want %s", update["sessionUpdate"], sessionUpdate)
		}
		return update
	}

	// message
	frames, err := presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventMessage, MessageID: "msg-1", Content: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, frames, "agent_message_chunk")

	// reasoning with content
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventReasoning, Content: "thinking...",
	})
	if err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, frames, "agent_thought_chunk")
	// empty reasoning skipped
	frames, err = presentationToACP("s", streaming.StreamEvent{Type: streaming.StreamEventReasoning})
	if err != nil || frames != nil {
		t.Fatalf("empty reasoning: %v %v", err, frames)
	}

	// function call + title/name/kind + CallID fallback
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type:      streaming.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{{ID: "tc-1", Name: "complete_todo", Title: "Complete Ship", Category: "think"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	u := mustUpdate(t, frames, "tool_call")
	if u["title"] != "Complete Ship" || u["name"] != "complete_todo" || u["kind"] != "think" {
		t.Fatalf("tool_call fields: %#v", u)
	}
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type:      streaming.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{{CallID: "fc_only", Name: "echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mustUpdate(t, frames, "tool_call")["toolCallId"] != "fc_only" {
		t.Fatal("call id fallback")
	}
	// function call with assistant content
	frames, err = presentationToACP("s", streaming.StreamEvent{
		Type: streaming.StreamEventFunctionCall, Content: "thinking aloud",
		ToolCalls: []tacklr.ToolCall{{ID: "c1", CallID: "c1", Name: "echo", Category: "other"}},
	})
	if err != nil || len(frames) < 2 {
		t.Fatalf("function call multi: %v n=%d", err, len(frames))
	}

	// tool result success / failed / empty / CallID
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventToolResult, Content: "file contents here",
		ToolCalls: []tacklr.ToolCall{{ID: "tc-1", Name: "read_file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	u = mustUpdate(t, frames, "tool_call_update")
	if u["status"] != "completed" {
		t.Fatalf("status=%v", u["status"])
	}
	frames, err = presentationToACP("s", streaming.StreamEvent{
		Type: streaming.StreamEventToolResult, Content: "boom",
		ToolCalls: []tacklr.ToolCall{{ID: "c1", CallID: "c1", Name: "echo", Status: "error"}},
	})
	if err != nil || !strings.Contains(string(frames[0]), "failed") {
		t.Fatalf("tool failed: %v %s", err, frames)
	}
	frames, err = presentationToACP("s", streaming.StreamEvent{Type: streaming.StreamEventToolResult})
	if err != nil || frames != nil {
		t.Fatalf("empty tool result: %v %v", err, frames)
	}
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventToolResult, Content: "ok",
		ToolCalls: []tacklr.ToolCall{{CallID: "fc_only", Name: "echo", Status: "success"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mustUpdate(t, frames, "tool_call_update")["toolCallId"] != "fc_only" {
		t.Fatal("tool result call id")
	}

	// tool update progress
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventToolUpdate, MessageID: "tc-1", Content: "processing step 1...",
	})
	if err != nil {
		t.Fatal(err)
	}
	u = mustUpdate(t, frames, "tool_call_update")
	if u["status"] != "in_progress" || u["toolCallId"] != "tc-1" {
		t.Fatalf("%#v", u)
	}

	// complete
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventComplete, TurnID: "turn-abc",
	})
	if err != nil {
		t.Fatal(err)
	}
	var complete map[string]any
	_ = json.Unmarshal(frames[0], &complete)
	if complete["result"].(map[string]any)["stopReason"] != "end_turn" {
		t.Fatalf("%v", complete)
	}

	// error internal + stop-reason outcomes + plain error field
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{
		Type: streaming.StreamEventError, TurnID: "turn-err", Error: io.ErrUnexpectedEOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	var errMsg map[string]any
	_ = json.Unmarshal(frames[0], &errMsg)
	if errMsg["error"].(map[string]any)["code"] != float64(-32603) {
		t.Fatalf("%v", errMsg)
	}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{tacklr.ErrModelRefused, "refusal"},
		{tacklr.ErrMaxTokens, "max_tokens"},
		{tacklr.ErrMaxTurnRequests, "max_turn_requests"},
		{fmt.Errorf("run: context cancelled: %w", context.Canceled), "cancelled"},
	} {
		frames, err = presentationToACP("t", streaming.StreamEvent{Type: streaming.StreamEventError, Error: tc.err})
		if err != nil {
			t.Fatal(err)
		}
		var msg map[string]any
		_ = json.Unmarshal(frames[0], &msg)
		if msg["result"].(map[string]any)["stopReason"] != tc.want {
			t.Fatalf("stopReason want %s got %v", tc.want, msg)
		}
	}
	frames, err = presentationToACP("s1", streaming.StreamEvent{
		Type: streaming.StreamEventError, Error: errors.New("explode"),
	})
	if err != nil || !strings.Contains(string(frames[0]), "explode") {
		t.Fatalf("%v %v", err, frames)
	}

	// plan update
	frames, err = presentationToACP("s", streaming.StreamEvent{
		Type: streaming.StreamEventPlanUpdate,
		Data: []byte(`[{"title":"A","status":"pending","description":""}]`),
	})
	if err != nil || !strings.Contains(string(frames[0]), `"plan"`) {
		t.Fatalf("plan: %v %s", err, frames)
	}

	// interrupt skipped
	frames, err = presentationToACP("thread-1", streaming.StreamEvent{Type: streaming.StreamEventInterrupt})
	if err != nil || len(frames) != 0 {
		t.Fatalf("interrupt skip: %v n=%d", err, len(frames))
	}
}

func TestACP_OnStreamClosed_cancelledVsPark(t *testing.T) {
	p := NewACPProtocol(nil).(*acpProtocol)
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
	if err := p.OnStreamClosed(context.Background(), env2, "t", json.RawMessage(`2`), false); err != nil {
		t.Fatal(err)
	}
	if len(w2.Errors) != 0 || len(w2.Results) != 0 {
		t.Fatalf("complete close must not write, errors=%d results=%d", len(w2.Errors), len(w2.Results))
	}
	if err := p.OnStreamClosed(context.Background(), env, "t", nil, false); err != nil {
		t.Fatal(err)
	}
}

func TestInjectReqID_nonJSONFrame(t *testing.T) {
	out := injectReqID([][]byte{[]byte("not-json"), []byte(`{"a":1}`)}, json.RawMessage(`7`), true)
	if len(out) != 2 || string(out[0]) != "not-json" {
		t.Fatalf("%v", out)
	}
}

func TestACP_handleInbound_initialize(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "protocolVersion") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// handleRPC — lifecycle methods
// ---------------------------------------------------------------------------

func TestHandleRPC_initialize(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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
	if caps["loadSession"] != true {
		t.Errorf("loadSession = %v, want true", caps["loadSession"])
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
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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

func TestHandleRPC_sessionNew_persistsWireEnvelope(t *testing.T) {
	// Outcome: durable wire store holds cwd/mcp after session/new (not private map peeks).
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := newACPTestServer(t, r)

	rec := srv.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/home/user","mcpServers":[{"name":"fs","command":"npx","env":[{"name":"API_KEY","value":"never-store"}]}]}}`)
	sessionID, _ := acpRPCResult(t, rec)["sessionId"].(string)
	if sessionID == "" {
		t.Fatal("missing sessionId")
	}

	raw, err := srv.wire.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("wire store: %v", err)
	}
	var env acpWireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.CWD != "/home/user" {
		t.Errorf("cwd = %q, want /home/user", env.CWD)
	}
	if env.Owner != "local" {
		t.Errorf("owner = %q, want local", env.Owner)
	}
	if len(env.MCPServers) != 1 || env.MCPServers[0].Name != "fs" {
		t.Errorf("mcpServers = %+v", env.MCPServers)
	}
	if len(env.MCPServers[0].Env) != 0 || strings.Contains(string(raw), "never-store") {
		t.Fatalf("wire envelope stored stdio MCP secret: %s", raw)
	}
}

func TestHandleRPC_sessionClose_thenLoadNotFound(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := newACPTestServer(t, r)

	rec1 := srv.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID, _ := acpRPCResult(t, rec1)["sessionId"].(string)

	rec2 := srv.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"` + sessionID + `"}}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("close status = %d, want 200", rec2.Code)
	}

	rec3 := srv.rpc(`{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/tmp"}}`)
	errObj := acpRPCError(t, rec3)
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "session") {
		t.Errorf("error = %v, want session not found", errObj)
	}
}

func TestHandleRPC_unknownMethod(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

	rec := serveACPRaw(t, r, `not json`)

	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["error"] == nil {
		t.Error("expected error response for invalid JSON")
	}
}

func TestHandleRPC_sessionLoad(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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

func TestHandleRPC_sessionLoad_persistsOnlyMCPTopology(t *testing.T) {
	// Outcome: durable wire state keeps topology/reference but not inline credentials.
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := newACPTestServer(t, r)

	rec1 := srv.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID, _ := acpRPCResult(t, rec1)["sessionId"].(string)

	rec2 := srv.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/tmp","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","credentialRef":"vault://api","headers":[{"name":"Authorization","value":"Bearer tok"}]}]}}`)
	_ = acpRPCResult(t, rec2)

	raw, err := srv.wire.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var env acpWireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.CWD != "/tmp" {
		t.Errorf("cwd = %q, want /tmp", env.CWD)
	}
	if len(env.MCPServers) != 1 || env.MCPServers[0].Type != "http" || env.MCPServers[0].URL != "https://api.example.com/mcp" {
		t.Errorf("mcpServers = %+v", env.MCPServers)
	}
	if len(env.MCPServers[0].Headers) != 0 || env.MCPServers[0].CredentialRef != "vault://api" {
		t.Errorf("durable MCP credentials = %+v", env.MCPServers[0])
	}
	if strings.Contains(string(raw), "Bearer tok") {
		t.Fatalf("wire envelope stored MCP secret: %s", raw)
	}
}

func TestHandleRPC_sessionLoad_cwdMismatch(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"missing"}}`)
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	errObj := resp["error"].(map[string]any)
	if !strings.Contains(errObj["message"].(string), "session") {
		t.Errorf("error message = %v, want to mention session", errObj["message"])
	}
}

// TestHandleRPC_sessionLoad_fromStoreAfterRestart: a new process (new kernel +
// protocol, same wire store) can session/load and complete a prompt.
func TestHandleRPC_sessionLoad_fromStoreAfterRestart(t *testing.T) {
	wire := NewMemoryWireStore()

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "after-restart", IsComplete: true}
		},
	}

	// Process 1: create session
	r1 := newTestRuntime(t, strategy, durable.AgentSpec{})
	s1 := newACPTestServerWithWire(t, r1, wire)
	rec1 := s1.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[]}]}}`)
	sessionID, _ := acpRPCResult(t, rec1)["sessionId"].(string)

	// Process 2: load + prompt
	r2 := newTestRuntime(t, strategy, durable.AgentSpec{})
	s2 := newACPTestServerWithWire(t, r2, wire)
	rec2 := s2.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/proj"}}`)
	if acpRPCResult(t, rec2)["sessionId"] != sessionID {
		t.Fatalf("load result: %v", rec2.Body.String())
	}

	raw, err := wire.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var env acpWireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.CWD != "/proj" || len(env.MCPServers) != 1 {
		t.Fatalf("wire envelope: %+v", env)
	}

	rec3 := s2.rpc(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	var endTurn bool
	for _, f := range parseACPFrames(t, rec3.Body) {
		if res, ok := f["result"].(map[string]any); ok && res["stopReason"] == "end_turn" {
			endTurn = true
		}
		if f["error"] != nil {
			t.Fatalf("prompt after restart: %v", f["error"])
		}
	}
	if !endTurn {
		t.Fatalf("expected end_turn, body=%s", rec3.Body.String())
	}
}

func TestHandleRPC_noAgentConfigured_onPrompt(t *testing.T) {
	r := newEmptyRuntime() // no default agent

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
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello", IsComplete: true}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{})

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

	// Text-only model rejects an image part before the turn starts.
	imgBody := `{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"see"},{"type":"image","mimeType":"image/png","data":"AAAA"}]}}`
	rec3 := serveACPRaw(t, r, imgBody)
	errObj := acpRPCError(t, rec3)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "unsupported content type") {
		t.Fatalf("image reject: %#v", errObj)
	}
}

func TestHandleRPC_sessionPrompt_clientTurnID(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{})

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
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{})

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
	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{mark}}})
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
	progressTool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "progress_demo",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
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

	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{progressTool}}})

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
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	r.Catalog.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "default"},
			Model:  strategy,
		},
	})
	r.Catalog.Register("custom", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "custom"},
			Model:  strategy,
		},
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
	// Outcome already asserted via configOptions.currentValue on the wire response.
}

func TestHandleRPC_configSet_unknownAgent(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})

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
	var customInvoked bool
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	r.Catalog.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "default-prompt"},
			Model: &mockInferenceStrategy{
				invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-default", IsComplete: true}
				},
			},
		},
	})
	r.Catalog.Register("custom", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "custom-prompt"},
			Model: &mockInferenceStrategy{
				invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
					customInvoked = true
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "from-custom", IsComplete: true}
				},
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

func TestHandleInbound_initialize_recordingWriter(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil))
	rec := &recordingMessageWriter{}

	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	srv.inbound(context.Background(), body, rec)

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
