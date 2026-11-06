package acp_test

import (
	"context"
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

// TestACP_sessionOutcomes: on a model that sees images, the agent
// advertises images; each content block reaches the model; a prompt it
// cannot read, or in another cwd, is refused; a tool call shows its title,
// name and kind; a refusal or the token
// limit ends the prompt with its stop reason; a load in another cwd is
// refused; and a closed session is not found.
func TestACP_sessionOutcomes(t *testing.T) {
	t.Parallel()
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
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
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
	url := acptest.Listen(t, acptest.Server(acptest.NewRuntime(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{mark}}), nil))
	open := func(t *testing.T) *acptest.Client { return acptest.Connect(t, url, `{"protocolVersion":1}`) }

	t.Run("contentBlocksReachTheModel", func(t *testing.T) {
		t.Parallel()
		c := acptest.Dial(t, url, nil)
		initialize := c.Call("initialize", `{"protocolVersion":1}`)
		end, _ := c.Until(c.Prompt(c.NewSession("/proj"), `[`+
			`{"type":"text","text":"review"},`+
			`{"type":"image","mimeType":"image/png","data":"AAAA"},`+
			`{"type":"image","mimeType":"image/jpeg","uri":"https://example.com/cat.jpg"},`+
			`{"type":"resource","resource":{"uri":"mem://note","text":"embedded notes"}},`+
			`{"type":"resource","resource":{"uri":"mem://doc.pdf","mimeType":"application/pdf","blob":"JVBERg=="}},`+
			`{"type":"resource_link","name":"spec","uri":"https://example.com/spec.md","mimeType":"text/markdown","title":"Spec","description":"the spec","size":12}`+
			`]`), nil)

		prompts := acptest.Result(t, initialize)["agentCapabilities"].(map[string]any)["promptCapabilities"].(map[string]any)
		if prompts["image"] != true {
			t.Fatalf("a model that sees images should advertise image=true: %v", initialize)
		}
		if acptest.StopReason(end) != "end_turn" || !sawLink.Load() || !sawImage.Load() || !sawPDF.Load() {
			t.Fatalf("link=%v image=%v pdf=%v, end=%v; want each block in the window and end_turn", sawLink.Load(), sawImage.Load(), sawPDF.Load(), end)
		}
	})

	t.Run("unreadablePromptsAreRefused", func(t *testing.T) {
		t.Parallel()
		c := open(t)
		sid := c.NewSession("/proj")
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
			if resp, _ := c.Until(c.Prompt(sid, blocks), nil); !strings.Contains(acptest.Message(t, resp), "invalid") {
				t.Errorf("prompt %s: %v, want it refused as invalid", blocks, resp)
			}
		}
	})

	t.Run("anotherCWDIsRefused", func(t *testing.T) {
		t.Parallel()
		c := open(t)
		sid := c.NewSession("/proj")
		inPrompt := c.Call("session/prompt", `{"sessionId":"`+sid+`","cwd":"/other","prompt":[{"type":"text","text":"x"}]}`)
		inLoad := c.Call("session/load", `{"sessionId":"`+sid+`","cwd":"/other"}`)
		for _, resp := range []map[string]any{inPrompt, inLoad} {
			if msg := acptest.Message(t, resp); !strings.Contains(msg, "cwd") {
				t.Errorf("another cwd = %q, want a cwd mismatch", msg)
			}
		}
	})

	t.Run("toolCallsShowTitleNameAndKind", func(t *testing.T) {
		t.Parallel()
		c := open(t)
		_, frames := c.Say(c.NewSession("/proj"), "mark")
		calls := map[any]map[string]any{}
		for _, update := range acptest.Updates(frames) {
			if update["sessionUpdate"] == "tool_call" {
				calls[update["name"]] = update
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
		t.Parallel()
		c := open(t)
		refused, _ := c.Say(c.NewSession("/proj"), "refuse")
		long, _ := c.Say(c.NewSession("/proj"), "long")
		refusal, maxTokens := acptest.StopReason(refused), acptest.StopReason(long)
		if refusal != "refusal" || maxTokens != "max_tokens" {
			t.Fatalf("stop reasons %q, %q; want refusal, max_tokens", refusal, maxTokens)
		}
	})

	t.Run("closedSessionIsNotFound", func(t *testing.T) {
		t.Parallel()
		c := open(t)
		sid := c.NewSession("/proj")
		closed := acptest.Result(t, c.Call("session/close", `{"sessionId":"`+sid+`"}`))
		load := c.Call("session/load", `{"sessionId":"`+sid+`","cwd":"/proj"}`)
		if msg := acptest.Message(t, load); closed == nil || !strings.Contains(msg, "not found") {
			t.Fatalf("close = %v, then load = %v; want the session gone", closed, load)
		}
	})
}

// TestACP_promptStreamsEvents: a prompt streams the turn's
// reasoning and message as session/update chunks, each text once (a
// complete reasoning or message whose deltas streamed sends no text again),
// and ends with a result; a model that reads text only refuses an image.
func TestACP_promptStreamsEvents(t *testing.T) {
	t.Parallel()
	model := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "thinking", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventReasoning, IsComplete: true}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "", IsComplete: false}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hello", IsComplete: true}
	})
	c := acptest.Open(t, acptest.NewRuntime(t, model, tacklr.AgentOptions{}))
	sid := c.NewSession("/tmp")

	resp, frames := c.Say(sid, "hi")
	image, _ := c.Until(c.Prompt(sid, `[{"type":"text","text":"see"},{"type":"image","mimeType":"image/png","data":"AAAA"}]`), nil)

	texts := map[any]string{}
	for _, update := range acptest.Updates(frames) {
		if content, ok := update["content"].(map[string]any); ok {
			texts[update["sessionUpdate"]] += content["text"].(string)
		}
	}
	end := acptest.StopReason(resp)
	if texts["agent_thought_chunk"] != "thinking" || texts["agent_message_chunk"] != "hello" || end != "end_turn" {
		t.Errorf("texts %v, end %v; want the reasoning and the message once each, then end_turn", texts, end)
	}
	if msg := acptest.Message(t, image); !strings.Contains(msg, "unsupported content type") {
		t.Fatalf("image on a text model = %v, want unsupported content", image)
	}
}

// TestACP_noAgentConfigured: a runtime with no model and no
// workspace makes and loads sessions, refuses a vfs bind with an unknown
// profile error, and fails a prompt.
func TestACP_noAgentConfigured(t *testing.T) {
	t.Parallel()
	c := acptest.Open(t, &acptest.Runtime{Client: livesess.Runtime(t, tacklr.AgentOptions{}, workspace.Config{})})
	sid := c.NewSession("/tmp")

	loaded := acptest.Result(t, c.Call("session/load", `{"sessionId":"`+sid+`","cwd":"/tmp"}`))
	bind := acptest.Result(t, c.Call("_tacklr/vfs/bind", `{"sessionId":"`+sid+`","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok"}}]}`))
	failed, _ := c.Say(sid, "hi")

	if errs, _ := bind["errors"].([]any); loaded["sessionId"] != sid || len(errs) == 0 {
		t.Fatalf("load = %v, bind = %v; want the session kept and the bind refused", loaded, bind)
	}
	if failed["error"] == nil {
		t.Fatalf("prompt with no model = %v, want an error", failed)
	}
}
