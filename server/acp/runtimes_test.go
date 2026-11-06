package acp_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
)

// TestACP_servesTheSameConversationOnEachRuntime: one ACP server serves
// the same conversation on Temporal and on Durable Objects (each call to an
// object over the object protocol, with each session's wire state in its
// object): a prompt asks permission for its tool and ends once allowed,
// and the next ends cancelled when the permission is cancelled; a second
// connection loads the session; and after a close the session is not
// found.
func TestACP_servesTheSameConversationOnEachRuntime(t *testing.T) {
	t.Parallel()
	deploy := tacklr.NewTool(tacklr.ToolConfig{
		Name:    "deploy",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "deployed", nil },
	})
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := msgs[len(msgs)-1]; last.Role == message.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done: " + last.Content, IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
			{ID: "c-deploy", CallID: "c-deploy", Name: "deploy", Arguments: `{}`},
		}}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, SystemPrompt: "test prompt", Tools: []*tacklr.Tool{deploy}}
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	objects := durableobject.NewClient(dotest.NewWire(durableobject.Config{Agent: agent, Sealer: sealer}))
	answer := func(outcome map[string]any) func(map[string]any) any {
		return func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"outcome": outcome}}
		}
	}
	allow, cancel := answer(map[string]any{"outcome": "selected", "optionId": "allow-once"}), answer(map[string]any{"outcome": "cancelled"})

	for name, rt := range map[string]struct {
		r    *acptest.Runtime
		wire server.ProtocolWireStore
	}{
		"temporal":        {acptest.NewRuntime(t, model, agent), nil},
		"durable objects": {&acptest.Runtime{Client: objects, Agent: agent}, objects.WireStore()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			url := acptest.Listen(t, acptest.Server(rt.r, rt.wire))
			c := acptest.Connect(t, url, `{"protocolVersion":1}`)
			sid := c.NewSession("/srv")

			end, frames := c.Until(c.Prompt(sid, `[{"type":"text","text":"ship it"}]`), allow)
			cancelled, _ := c.Until(c.Prompt(sid, `[{"type":"text","text":"ship it again"}]`), cancel)
			loaded := acptest.Connect(t, url, `{"protocolVersion":1}`).Call("session/load", `{"sessionId":"`+sid+`","cwd":"/srv"}`)
			closed := c.Call("session/close", `{"sessionId":"`+sid+`"}`)
			gone := c.Call("session/load", `{"sessionId":"`+sid+`","cwd":"/srv"}`)

			blob, _ := json.Marshal(frames)
			if acptest.StopReason(end) != "end_turn" || !strings.Contains(string(blob), "done: deployed") {
				t.Fatalf("prompt = %v after %s; want end_turn after the allowed tool ran", end, blob)
			}
			if acptest.StopReason(cancelled) != "cancelled" {
				t.Fatalf("a cancelled permission ended the prompt with %v, want stopReason cancelled", cancelled)
			}
			if acptest.Result(t, loaded)["sessionId"] != sid || acptest.Result(t, closed) == nil {
				t.Fatalf("load = %v, close = %v; want the session loaded, then closed", loaded, closed)
			}
			if msg := acptest.Message(t, gone); !strings.Contains(msg, "not found") {
				t.Fatalf("load after close = %q, want not found", msg)
			}
		})
	}
}
