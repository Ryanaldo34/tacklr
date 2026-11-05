package acp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry/export"
	"github.com/ryanaldo34/tacklr/workspace"
)

func TestMain(m *testing.M) {
	shutdown, err := export.Init(context.Background(), export.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

// errHungUp is what a write to a client that hung up gets.
var errHungUp = errors.New("connection reset by peer")

// hungUp is an HTTP response whose client hung up: each write fails.
type hungUp struct{ *httptest.ResponseRecorder }

func (hungUp) Write([]byte) (int, error) { return 0, errHungUp }

// rpcError is the error object of a JSON-RPC response; a response with
// none fails the test.
func rpcError(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
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

// stopReason is the stopReason a prompt's frames end with; "" when they
// end with an error.
func stopReason(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	for _, f := range acptest.Frames(t, rec.Body) {
		if res, ok := f["result"].(map[string]any); ok {
			if sr, _ := res["stopReason"].(string); sr != "" {
				return sr
			}
		}
	}
	return ""
}

func prompt(id, sessionID, blocks string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":` + blocks + `}}`
}

// TestHandleRPC_sessionOutcomes: on a model that sees images, the agent
// advertises images; each content block reaches the model; a prompt it
// cannot read, or in another cwd, is refused; a tool call shows its title,
// name and kind; a refusal or the token limit ends the prompt with its
// stop reason; a load in another cwd is refused; and a closed session is
// not found.
func TestHandleRPC_sessionOutcomes(t *testing.T) {
	var sawLink, sawImage, sawPDF atomic.Bool
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
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		switch {
		case last.Role == message.RoleTool:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
		case last.Content == "mark":
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, Content: "calling", IsComplete: true, ToolCalls: []message.ToolCall{
				{ID: "call_mark", CallID: "call_mark", Name: "mark_item", Arguments: `{"title":"Ship release"}`},
				{ID: "ghost", CallID: "ghost", Name: "ghost_tool", Arguments: `{}`},
			}}
		case last.Content == "refuse":
			ch <- tacklr.LLMResponseChunk{Type: session.EventError, Error: session.ErrModelRefused, Content: "model refused", IsComplete: true}
		case last.Content == "long":
			ch <- tacklr.LLMResponseChunk{Type: session.EventError, Error: session.ErrMaxTokens, IsComplete: true}
		default:
			sawLink.Store(strings.Contains(last.Content, "[Resource link] name=spec"))
			for _, part := range last.ContentParts {
				sawImage.Store(sawImage.Load() || part.Type == message.ContentTypeInputImage)
				sawPDF.Store(sawPDF.Load() || part.FileData != nil && part.FileData.MIMEType == "application/pdf")
			}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok-blocks", IsComplete: true}
		}
	})
	r := acptest.NewRuntime(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{mark}})
	srv := acptest.NewServer(t, r, server.NewMemoryWireStore())
	newSession := func(t *testing.T) string {
		t.Helper()
		return acptest.SessionID(t, srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/proj"}}`))
	}

	t.Run("contentBlocksReachTheModel", func(t *testing.T) {
		initialize := srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
		rec := srv.RPC(prompt("3", newSession(t), `[`+
			`{"type":"text","text":"review"},`+
			`{"type":"image","mimeType":"image/png","data":"AAAA"},`+
			`{"type":"image","mimeType":"image/jpeg","uri":"https://example.com/cat.jpg"},`+
			`{"type":"resource","resource":{"uri":"mem://note","text":"embedded notes"}},`+
			`{"type":"resource","resource":{"uri":"mem://doc.pdf","mimeType":"application/pdf","blob":"JVBERg=="}},`+
			`{"type":"resource_link","name":"spec","uri":"https://example.com/spec.md","mimeType":"text/markdown","title":"Spec","description":"the spec","size":12}`+
			`]`))

		if !strings.Contains(initialize.Body.String(), `"image":true`) {
			t.Fatalf("a model that sees images should advertise image=true: %s", initialize.Body.String())
		}
		if stopReason(t, rec) != "end_turn" || !sawLink.Load() || !sawImage.Load() || !sawPDF.Load() {
			t.Fatalf("link=%v image=%v pdf=%v, body=%s; want each block in the window and end_turn", sawLink.Load(), sawImage.Load(), sawPDF.Load(), rec.Body.String())
		}
	})

	t.Run("unreadablePromptsAreRefused", func(t *testing.T) {
		sid := newSession(t)
		for _, blocks := range []string{
			`[{"type":"audio","mimeType":"audio/wav","data":"AAAA"}]`,
			`[]`,
			`[{"type":"text","text":"  "}]`,
			`[{"type":"image","mimeType":"text/plain","data":"x"}]`,
			`[{"type":"image","mimeType":"image/png"}]`,
			`[{"type":"resource"}]`,
			`[{"type":"resource","resource":{}}]`,
			`[{"type":"resource","resource":{"blob":"xxxx","mimeType":"image/png"}}]`,
			`[{"type":"resource_link","name":"x"}]`,
			`[{"type":"video"}]`,
			`"not-array"`,
		} {
			if msg, _ := rpcError(t, srv.RPC(prompt("4", sid, blocks)))["message"].(string); !strings.Contains(msg, "invalid") {
				t.Errorf("prompt %s: %q, want it refused as invalid", blocks, msg)
			}
		}
	})

	t.Run("anotherCWDIsRefused", func(t *testing.T) {
		sid := newSession(t)
		inPrompt := srv.RPC(`{"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{"sessionId":"` + sid + `","cwd":"/other","prompt":[{"type":"text","text":"x"}]}}`)
		inLoad := srv.RPC(`{"jsonrpc":"2.0","id":6,"method":"session/load","params":{"sessionId":"` + sid + `","cwd":"/other"}}`)
		for _, rec := range []*httptest.ResponseRecorder{inPrompt, inLoad} {
			if msg, _ := rpcError(t, rec)["message"].(string); !strings.Contains(msg, "cwd") {
				t.Errorf("another cwd = %q, want a cwd mismatch", msg)
			}
		}
	})

	t.Run("toolCallsShowTitleNameAndKind", func(t *testing.T) {
		rec := srv.RPC(prompt("7", newSession(t), `[{"type":"text","text":"mark"}]`))
		calls := map[any]map[string]any{}
		for _, f := range acptest.Frames(t, rec.Body) {
			if params, ok := f["params"].(map[string]any); ok {
				if update := params["update"].(map[string]any); update["sessionUpdate"] == "tool_call" {
					calls[update["name"]] = update
				}
			}
		}
		if mark := calls["mark_item"]; mark["title"] != "Complete Ship release" || mark["kind"] != "think" {
			t.Errorf("mark_item call = %v, want its display title and kind", mark)
		}
		if ghost := calls["ghost_tool"]; ghost["title"] != "ghost_tool" {
			t.Errorf("unknown tool call = %v, want its name as title", ghost)
		}
	})

	t.Run("stopReasons", func(t *testing.T) {
		refusal := stopReason(t, srv.RPC(prompt("8", newSession(t), `[{"type":"text","text":"refuse"}]`)))
		maxTokens := stopReason(t, srv.RPC(prompt("9", newSession(t), `[{"type":"text","text":"long"}]`)))
		if refusal != "refusal" || maxTokens != "max_tokens" {
			t.Fatalf("stop reasons %q, %q; want refusal, max_tokens", refusal, maxTokens)
		}
	})

	t.Run("closedSessionIsNotFound", func(t *testing.T) {
		sid := newSession(t)
		closed := acptest.Result(t, srv.RPC(`{"jsonrpc":"2.0","id":10,"method":"session/close","params":{"sessionId":"`+sid+`"}}`))
		load := rpcError(t, srv.RPC(`{"jsonrpc":"2.0","id":11,"method":"session/load","params":{"sessionId":"`+sid+`","cwd":"/proj"}}`))
		if msg, _ := load["message"].(string); closed == nil || !strings.Contains(msg, "not found") {
			t.Fatalf("close = %v, then load = %v; want the session gone", closed, load)
		}
	})
}

// TestHandleRPC_sessionPrompt_streamsEvents: a prompt streams the turn's
// reasoning and message as session/update chunks, each text once, and ends
// with a result; a model that reads text only refuses an image.
func TestHandleRPC_sessionPrompt_streamsEvents(t *testing.T) {
	model := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "thinking", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hello", IsComplete: true}
	})
	srv := acptest.NewServer(t, acptest.NewRuntime(t, model, tacklr.AgentOptions{}), server.NewMemoryWireStore())
	sid := acptest.SessionID(t, srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))

	rec := srv.RPC(prompt("10", sid, `[{"type":"text","text":"hi"}]`))
	image := rpcError(t, srv.RPC(prompt("11", sid, `[{"type":"text","text":"see"},{"type":"image","mimeType":"image/png","data":"AAAA"}]`)))

	texts := map[any]string{}
	var end any
	for _, f := range acptest.Frames(t, rec.Body) {
		if params, ok := f["params"].(map[string]any); ok && f["method"] == "session/update" {
			update := params["update"].(map[string]any)
			if content, ok := update["content"].(map[string]any); ok {
				texts[update["sessionUpdate"]] += content["text"].(string)
			}
		}
		if res, ok := f["result"].(map[string]any); ok {
			end = res["stopReason"]
		}
	}
	if texts["agent_thought_chunk"] != "thinking" || texts["agent_message_chunk"] != "hello" || end != "end_turn" {
		t.Errorf("texts %v, end %v; want the reasoning and the message once each, then end_turn", texts, end)
	}
	if msg, _ := image["message"].(string); !strings.Contains(msg, "unsupported content type") {
		t.Fatalf("image on a text model = %v, want unsupported content", image)
	}
}

// TestHandleRPC_noAgentConfigured_onPrompt: a runtime with no model and no
// workspace makes and loads sessions, refuses a vfs bind with an unknown
// profile error, and fails a prompt.
func TestHandleRPC_noAgentConfigured_onPrompt(t *testing.T) {
	srv := acptest.NewServer(t, &acptest.Runtime{Client: livesess.Runtime(t, tacklr.AgentOptions{}, workspace.Config{})}, server.NewMemoryWireStore())
	sid := acptest.SessionID(t, srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))

	loaded := acptest.Result(t, srv.RPC(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`","cwd":"/tmp"}}`))
	bind := acptest.Result(t, srv.RPC(`{"jsonrpc":"2.0","id":3,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sid+`","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok"}}]}}`))
	rec := srv.RPC(prompt("4", sid, `[{"type":"text","text":"hi"}]`))

	if errs, _ := bind["errors"].([]any); loaded["sessionId"] != sid || len(errs) == 0 {
		t.Fatalf("load = %v, bind = %v; want the session kept and the bind refused", loaded, bind)
	}
	var failed bool
	for _, f := range acptest.Frames(t, rec.Body) {
		failed = failed || f["error"] != nil
	}
	if !failed {
		t.Fatalf("prompt with no model = %s, want an error", rec.Body.String())
	}
}

// TestClientBridge_callFailsWhenTheClientIsGone: a call to a client that
// hung up fails with the write's error, and a call whose context ends
// returns the context's error.
func TestClientBridge_callFailsWhenTheClientIsGone(t *testing.T) {
	ended, cancel := context.WithCancel(t.Context())
	cancel()

	_, hungUpErr := acp.NewClientBridge(acp.HTTPWriter(hungUp{httptest.NewRecorder()})).Call(t.Context(), "x", nil)
	_, endedErr := acp.NewClientBridge(acp.HTTPWriter(httptest.NewRecorder())).Call(ended, "x", nil)

	if !errors.Is(hungUpErr, errHungUp) || !errors.Is(endedErr, context.Canceled) {
		t.Fatalf("hung up = %v, ended = %v; want the write's and the context's errors", hungUpErr, endedErr)
	}
}
