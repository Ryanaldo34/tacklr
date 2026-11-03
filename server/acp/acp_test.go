package acp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

func newEmptyRuntime(t *testing.T) *acptest.Runtime {
	t.Helper()
	return &acptest.Runtime{Client: livesess.Runtime(t, tacklr.AgentOptions{})}
}

func inbound(ctx context.Context, s *server.Server, body []byte, w server.MessageWriter) {
	_ = s.Protocols[0].HandleInbound(ctx, server.ProtocolEnv{
		Client: s.Client, Agent: s.Agent, Security: s.Security,
		Connections: s.Connections, Conn: &server.Conn{Writer: w},
	}, body)
}

type recordingMessageWriter = testkit.RecordingWriter

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// acpByRuntime returns a stable ACP protocol per kernel so multi-step
// serveACPRaw calls share wire state.
var acpByRuntime sync.Map // *acptest.Runtime → server.Protocol

func acpProtocolFor(r *acptest.Runtime) server.Protocol {
	if r == nil {
		return acp.New(server.NewMemoryWireStore())
	}
	if v, ok := acpByRuntime.Load(r); ok {
		return v.(server.Protocol)
	}
	p := acp.New(server.NewMemoryWireStore())
	actual, _ := acpByRuntime.LoadOrStore(r, p)
	return actual.(server.Protocol)
}

// serveACPRaw runs one RPC against a per-kernel isolated ACP protocol.
func serveACPRaw(t *testing.T, r *acptest.Runtime, body string) *httptest.ResponseRecorder {
	t.Helper()
	return acptest.Serve(t, r, acpProtocolFor(r), body)
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

func TestHandleRPC_sessionNew_stripsMCPSecretsFromWire(t *testing.T) {
	r := acptest.NewRuntime(t, testkit.HTTPModel(t, nil), tacklr.AgentOptions{})
	srv := acptest.NewServer(t, r, server.NewMemoryWireStore())
	rec := srv.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/home/user","mcpServers":[{"name":"fs","command":"npx","env":[{"name":"API_KEY","value":"never-store"}]}]}}`)
	sessionID, _ := acptest.Result(t, rec)["sessionId"].(string)
	if sessionID == "" {
		t.Fatal("missing sessionId")
	}
	raw, err := srv.Wire.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("wire store: %v", err)
	}
	if strings.Contains(string(raw), "never-store") {
		t.Fatalf("wire envelope stored stdio MCP secret: %s", raw)
	}
}

func TestHandleRPC_sessionClose_thenLoadNotFound(t *testing.T) {
	r := acptest.NewRuntime(t, testkit.HTTPModel(t, nil), tacklr.AgentOptions{})
	srv := acptest.NewServer(t, r, server.NewMemoryWireStore())

	rec1 := srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID, _ := acptest.Result(t, rec1)["sessionId"].(string)

	rec2 := srv.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"` + sessionID + `"}}`)
	if rec2.Code != http.StatusOK {
		t.Fatalf("close status = %d, want 200", rec2.Code)
	}

	rec3 := srv.RPC(`{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/tmp"}}`)
	errObj := acpRPCError(t, rec3)
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "session") {
		t.Errorf("error = %v, want session not found", errObj)
	}
}

func TestHandleRPC_sessionLoad_fromStoreAfterRestart(t *testing.T) {
	wire := server.NewMemoryWireStore()

	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "after-restart", IsComplete: true}
	})

	// Process 1: create session
	r1 := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{})
	s1 := acptest.NewServer(t, r1, wire)
	rec1 := s1.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj","mcpServers":[{"type":"http","name":"api","url":"https://api.example.com/mcp","headers":[]}]}}`)
	sessionID, _ := acptest.Result(t, rec1)["sessionId"].(string)

	// New protocol process, same Temporal worker and wire store.
	s2 := acptest.NewServer(t, r1, wire)
	rec2 := s2.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"` + sessionID + `","cwd":"/proj"}}`)
	if acptest.Result(t, rec2)["sessionId"] != sessionID {
		t.Fatalf("load result: %v", rec2.Body.String())
	}

	raw, err := wire.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		CWD        string              `json:"cwd"`
		MCPServers []session.MCPServer `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.CWD != "/proj" || len(env.MCPServers) != 1 {
		t.Fatalf("wire envelope: %+v", env)
	}

	rec3 := s2.RPC(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	var endTurn bool
	for _, f := range acptest.Frames(t, rec3.Body) {
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
	r := newEmptyRuntime(t)

	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	if resp1["error"] != nil {
		t.Fatalf("session/new should succeed without default agent: %v", resp1["error"])
	}
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	if acptest.Result(t, serveACPRaw(t, r, `{"jsonrpc":"2.0","id":15,"method":"session/load","params":{"sessionId":"`+sessionID+`","cwd":"/tmp"}}`))["sessionId"] != sessionID {
		t.Fatal("load empty agent should keep session")
	}
	bindRec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":16,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sessionID+`","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok"}}]}}`)
	bindRes := acptest.Result(t, bindRec)
	errs, _ := bindRes["errors"].([]any)
	if len(errs) == 0 {
		t.Fatal("want unknown vfs profile when agent has no OpenVFS")
	}

	promptBody := `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)
	var resp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["error"] == nil && resp2["result"] == nil {
		t.Fatalf("prompt with no model: %v", resp2)
	}
}

// ---------------------------------------------------------------------------
// handleRPC — session/prompt (streaming)
// ---------------------------------------------------------------------------

func TestHandleRPC_sessionPrompt_streamsEvents(t *testing.T) {
	strategy := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "thinking", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hello", IsComplete: true}
	})
	r := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{})

	// Create session
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)

	// Send prompt
	promptBody := `{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`
	rec2 := serveACPRaw(t, r, promptBody)

	frames := acptest.Frames(t, rec2.Body)
	if len(frames) == 0 {
		t.Fatal("expected at least one frame")
	}

	// Should have session/update notifications followed by a result
	var hasUpdate, hasThought, hasResult bool
	for _, f := range frames {
		if f["method"] == "session/update" {
			hasUpdate = true
			params, _ := f["params"].(map[string]any)
			update, _ := params["update"].(map[string]any)
			if update["sessionUpdate"] == "agent_thought_chunk" {
				hasThought = true
			}
		}
		if f["result"] != nil {
			hasResult = true
		}
	}
	if !hasUpdate {
		t.Error("expected at least one session/update notification")
	}
	if !hasThought {
		t.Error("expected agent_thought_chunk for reasoning")
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
	var calls atomic.Int32
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if calls.Add(1) > 1 {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{
			Type:    session.EventFunctionCall,
			Content: "calling",
			ToolCalls: []message.ToolCall{
				{
					ID: "call_mark", CallID: "call_mark", Name: "mark_item",
					Arguments: `{"title":"Ship release"}`,
				},
				{ID: "ghost", CallID: "ghost", Name: "ghost_tool", Arguments: `{}`},
			},
			IsComplete: true,
		}
	})
	r := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{Tools: []*tacklr.Tool{mark}})
	rec1 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var resp1 map[string]any
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	sessionID := resp1["result"].(map[string]any)["sessionId"].(string)
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"mark"}]}}`)
	frames := acptest.Frames(t, rec2.Body)

	var sawCall, sawGhost bool
	for _, f := range frames {
		if f["method"] != "session/update" {
			continue
		}
		update := f["params"].(map[string]any)["update"].(map[string]any)
		if update["sessionUpdate"] != "tool_call" {
			continue
		}
		switch update["name"] {
		case "mark_item":
			if update["title"] != "Complete Ship release" {
				t.Fatalf("title = %v, want Complete Ship release", update["title"])
			}
			if update["kind"] != "think" {
				t.Fatalf("kind = %v", update["kind"])
			}
			sawCall = true
		case "ghost_tool":
			if update["title"] != "ghost_tool" {
				t.Fatalf("unknown tool title = %v, want ghost_tool", update["title"])
			}
			sawGhost = true
		}
	}
	if !sawCall {
		t.Fatal("expected tool_call with title+name")
	}
	if !sawGhost {
		t.Fatal("expected unknown tool title fallback")
	}
}

func TestHandleRPC_sessionPrompt_acpContentBlocks(t *testing.T) {
	var sawLink, sawImage, sawPDF bool
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if n := len(msgs); n > 0 {
			last := msgs[n-1]
			if last != nil && strings.Contains(last.Content, "[Resource link] name=spec") {
				sawLink = true
			}
			for _, part := range last.ContentParts {
				if part.Type == message.ContentTypeInputImage {
					sawImage = true
				}
				if part.FileData != nil && part.FileData.MIMEType == "application/pdf" {
					sawPDF = true
				}
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok-blocks", IsComplete: true}
	})
	r := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{})
	initRec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	initBody := initRec.Body.String()
	if !strings.Contains(initBody, `"image":true`) {
		t.Fatalf("vision model should advertise image=true: %s", initBody)
	}

	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/proj"}}`)
	sessionID := acptest.Result(t, recNew)["sessionId"].(string)
	prompt := `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[` +
		`{"type":"text","text":"review"},` +
		`{"type":"image","mimeType":"image/png","data":"AAAA"},` +
		`{"type":"image","mimeType":"image/jpeg","uri":"https://example.com/cat.jpg"},` +
		`{"type":"resource","resource":{"uri":"mem://note","text":"embedded notes"}},` +
		`{"type":"resource","resource":{"uri":"mem://doc.pdf","mimeType":"application/pdf","blob":"JVBERg=="}},` +
		`{"type":"resource_link","name":"spec","uri":"https://example.com/spec.md","mimeType":"text/markdown","title":"Spec","description":"the spec","size":12}` +
		`]}}`
	rec := serveACPRaw(t, r, prompt)
	var endTurn bool
	for _, f := range acptest.Frames(t, rec.Body) {
		if f["error"] != nil {
			t.Fatalf("prompt error: %v", f["error"])
		}
		if res, ok := f["result"].(map[string]any); ok && res["stopReason"] == "end_turn" {
			endTurn = true
		}
	}
	if !endTurn {
		t.Fatalf("expected end_turn, body=%s", rec.Body.String())
	}
	if !sawLink || !sawImage || !sawPDF {
		t.Fatalf("model window missing content blocks link=%v image=%v pdf=%v", sawLink, sawImage, sawPDF)
	}

	audio := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"audio","mimeType":"audio/wav","data":"AAAA"}]}}`)
	if msg, _ := acpRPCError(t, audio)["message"].(string); !strings.Contains(msg, "audio") {
		t.Fatalf("audio reject: %s", audio.Body.String())
	}
	for i, body := range []string{
		`{"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[]}}`,
		`{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"  "}]}}`,
		`{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"image","mimeType":"text/plain","data":"x"}]}}`,
		`{"jsonrpc":"2.0","id":8,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"resource"}]}}`,
		`{"jsonrpc":"2.0","id":9,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"resource","resource":{}}]}}`,
		`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"resource","resource":{"blob":"xxxx","mimeType":"image/png"}}]}}`,
		`{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"resource_link","name":"x"}]}}`,
		`{"jsonrpc":"2.0","id":12,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"video"}]}}`,
		`{"jsonrpc":"2.0","id":13,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":"not-array"}}`,
		`{"jsonrpc":"2.0","id":17,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"image","mimeType":"image/png"}]}}`,
	} {
		if acpRPCError(t, serveACPRaw(t, r, body)) == nil {
			t.Fatalf("case %d: want invalid prompt", i)
		}
	}
	mismatch := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":14,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","cwd":"/other","prompt":[{"type":"text","text":"x"}]}}`)
	if msg, _ := acpRPCError(t, mismatch)["message"].(string); !strings.Contains(msg, "cwd") {
		t.Fatalf("prompt cwd mismatch: %v", acpRPCError(t, mismatch))
	}
}

func TestHandleRPC_sessionLoad_cwdMismatch(t *testing.T) {
	r := acptest.NewRuntime(t, testkit.HTTPModel(t, nil), tacklr.AgentOptions{})
	srv := acptest.NewServer(t, r, server.NewMemoryWireStore())
	rec := srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj"}}`)
	sessionID := acptest.Result(t, rec)["sessionId"].(string)
	errObj := acpRPCError(t, srv.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sessionID+`","cwd":"/other"}}`))
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "cwd") {
		t.Fatalf("cwd mismatch: %v", errObj)
	}
}
