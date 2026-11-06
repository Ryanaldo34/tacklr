package acp_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// plainInterrupt is a host's own interrupt kind, which ACP has no form
// for.
type plainInterrupt struct{}

func (*plainInterrupt) TypeName() string           { return "test_plain_interrupt" }
func (*plainInterrupt) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (*plainInterrupt) Return([]byte) error        { return nil }
func (*plainInterrupt) Error() string              { return "waiting" }

func init() { tacklr.RegisterInterrupt(func() tacklr.Interrupt { return &plainInterrupt{} }) }

// TestACP_interruptOutcomes: when a turn parks and the client can answer,
// the agent asks it mid-turn: an allowed permission runs the tool (a tool
// whose title is empty is shown by its name), and an accepted choice
// resumes the turn with it, to end_turn. A cancelled permission or choice
// cancels the turn, also when the client cancelled the turn first. An
// answer the agent cannot use, or an error from the client, ends the
// prompt with an error. A park the
// client cannot answer (no form support, or an interrupt with no ACP form)
// ends the prompt with an error that the turn needs user input. A client
// that hangs up while asked is asked again when it loads the session on a
// new connection, and its answer finishes the turn.
func TestACP_interruptOutcomes(t *testing.T) {
	t.Parallel()
	askUser := tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, cc *tacklr.CallContext) (string, error) {
			answer, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Pick a path", Options: []tacklr.UserChoice{
				{Title: "A", Description: "First", IsRecommended: true},
				{Title: "B", Description: "Second"},
			}})
			if err != nil {
				return "", err
			}
			return "selected: " + answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title, nil
		},
	})
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:    "sensitive",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { return "approved", nil },
	})
	// The title of untitled names an argument the model does not send.
	untitled := tacklr.NewTool(tacklr.ToolConfig{
		Name:        "untitled",
		DisplayName: "{target}",
		OnCall:      []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler:     func(ctx context.Context) (string, error) { return "approved untitled", nil },
	})
	waitChild := tacklr.NewTool(tacklr.ToolConfig{
		Name: "wait_child",
		Handler: func(ctx context.Context, _ struct{}, cc *tacklr.CallContext) (string, error) {
			_, err := cc.Park(&plainInterrupt{})
			return "", err
		},
	})
	// The model calls the tool the prompt names, then ends with its result.
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		if last.Role == message.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{
			{ID: "c-" + last.Content, CallID: "c-" + last.Content, Name: last.Content, Arguments: `{}`},
		}}
	})
	r := acptest.NewRuntime(t, model, tacklr.AgentOptions{Tools: []*tacklr.Tool{askUser, sensitive, untitled, waitChild}})
	url := acptest.Listen(t, acptest.Server(r, nil))
	form := acptest.Connect(t, url, `{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{},"url":null}}}`)
	plain := acptest.Connect(t, url, `{"protocolVersion":1,"clientCapabilities":{"_meta":{"tacklr":{"vfs":{"tokenRefresh":true}}}}}`)
	reply := func(result any) func(map[string]any) any {
		return func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": result}
		}
	}
	permission := func(outcome map[string]any) func(map[string]any) any {
		return reply(map[string]any{"outcome": outcome})
	}

	for _, c := range []struct {
		name   string
		client *acptest.Client
		tool   string
		answer func(map[string]any) any
		// stop is how the prompt ends, "" for an error. want is a part of
		// what came before an end_turn, or of the error ("" for any).
		stop string
		want string
	}{
		{"allowedPermission", plain, "sensitive", permission(map[string]any{"outcome": "selected", "optionId": "allow-once"}), "end_turn", "approved"},
		{"untitledPermissionShowsTheToolName", plain, "untitled", func(req map[string]any) any {
			if req["params"].(map[string]any)["toolCall"].(map[string]any)["title"] != "untitled" {
				return permission(map[string]any{"outcome": "cancelled"})(req)
			}
			return permission(map[string]any{"outcome": "selected", "optionId": "allow-once"})(req)
		}, "end_turn", "approved untitled"},
		{"acceptedChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{"choice": "A"}}), "end_turn", "selected: A"},
		{"cancelledPermission", plain, "sensitive", permission(map[string]any{"outcome": "cancelled"}), "cancelled", ""},
		{"cancelledChoice", form, "ask_user", reply(map[string]any{"action": "cancel"}), "cancelled", ""},
		// The client cancels the turn, then answers what it was asked
		// with cancelled, as ACP asks.
		{"cancelWhileAsked", plain, "sensitive", func(req map[string]any) any {
			plain.Send(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + req["params"].(map[string]any)["sessionId"].(string) + `"}}`)
			return permission(map[string]any{"outcome": "cancelled"})(req)
		}, "cancelled", ""},
		{"unknownPermissionOutcome", plain, "sensitive", permission(map[string]any{"outcome": "nope"}), "", ""},
		{"permissionWithNoOption", plain, "sensitive", permission(map[string]any{"outcome": "selected"}), "", ""},
		{"permissionNotAnObject", plain, "sensitive", reply("not-an-object"), "", ""},
		{"clientError", plain, "sensitive", func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "error": map[string]any{"code": -32000, "message": "nope"}}
		}, "", ""},
		{"clientErrorOnChoice", form, "ask_user", func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "error": map[string]any{"code": -32000, "message": "nope"}}
		}, "", ""},
		{"declinedChoice", form, "ask_user", reply(map[string]any{"action": "decline"}), "", ""},
		{"unknownChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{"choice": "nope"}}), "", ""},
		{"noChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{}}), "", ""},
		{"unknownElicitationAction", form, "ask_user", reply(map[string]any{"action": "nope"}), "", ""},
		{"elicitationNotAnObject", form, "ask_user", reply("not-an-object"), "", ""},
		{"noFormSupport", plain, "ask_user", nil, "", "user input"},
		{"interruptWithNoForm", form, "wait_child", nil, "", "user input"},
	} {
		t.Run(c.name, func(t *testing.T) {
			client := c.client.Use(t)
			end, frames := client.Until(client.Prompt(client.NewSession("/tmp"), `[{"type":"text","text":"`+c.tool+`"}]`), c.answer)

			if c.stop != "" {
				blob, _ := json.Marshal(frames)
				if acptest.StopReason(end) != c.stop || !strings.Contains(string(blob), c.want) {
					t.Fatalf("prompt ended with %v after %s; want %s after %q", end, blob, c.stop, c.want)
				}
				return
			}
			if msg, _ := end["error"].(map[string]any)["message"].(string); end["error"] == nil || !strings.Contains(msg, c.want) {
				t.Fatalf("prompt ended with %v; want an error with %q", end, c.want)
			}
		})
	}

	t.Run("hangUpWhileAsked", func(t *testing.T) {
		gone := acptest.Connect(t, url, `{"protocolVersion":1}`)
		sid := gone.NewSession("/tmp")
		gone.Prompt(sid, `[{"type":"text","text":"sensitive"}]`)
		for gone.Frame()["method"] != "session/request_permission" {
			continue
		}
		gone.Close()
		back := acptest.Connect(t, url, `{"protocolVersion":1}`)
		loaded := back.Request("session/load", `{"sessionId":"`+sid+`","cwd":"/tmp"}`)
		var asked, result any
		var rest []map[string]any
		for result == nil || asked == nil {
			frame := back.Frame()
			switch {
			case frame["method"] == "session/request_permission":
				asked = frame["params"].(map[string]any)["sessionId"]
				back.Send(`{"jsonrpc":"2.0","id":` + string(mustJSON(frame["id"])) + `,"result":{"outcome":{"outcome":"selected","optionId":"allow-once"}}}`)
			case frame["id"] == float64(loaded):
				result = frame["result"]
			}
		}
		for done := false; !done; {
			frame := back.Frame()
			rest = append(rest, frame)
			for _, u := range acptest.Updates([]map[string]any{frame}) {
				done = done || u["sessionUpdate"] == "agent_message_chunk"
			}
		}

		blob, _ := json.Marshal(rest)
		if asked != sid || !strings.Contains(string(blob), "approved") {
			t.Fatalf("asked for %v, load = %v, then %s; want the permission asked again and the tool run once allowed", asked, result, blob)
		}
	})
}

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

// TestACP_sessionCheckpoint_secondPromptContinuesPlan: create_plan + complete_todo
// checkpoints, then a second session/prompt loads the store and list_plan shows
// restored statuses.
func TestACP_sessionCheckpoint_secondPromptContinuesPlan(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	phase := "turn1"
	var turn1Steps, turn2Steps int
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		mu.Lock()
		defer mu.Unlock()
		call := func(id, name, args string) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: []message.ToolCall{{ID: id, CallID: id, Name: name, Arguments: args}}, IsComplete: true}
			ch <- tacklr.LLMResponseChunk{IsComplete: true}
		}
		if phase == "turn1" {
			turn1Steps++
			switch turn1Steps {
			case 1:
				call("cp", "create_plan", `{"plan":"P","todos":[{"title":"Alpha","status":"pending","description":"a"},{"title":"Beta","status":"pending","description":"b"}]}`)
			case 2:
				call("ct", "complete_todo", `{"title":"Alpha"}`)
			case 3:
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "handoff alpha", IsComplete: true}
			default:
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "turn1 done", IsComplete: true}
			}
			return
		}
		turn2Steps++
		if turn2Steps == 1 {
			call("lp", "list_plan", `{}`)
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "turn2 continued", IsComplete: true}
	})
	c := acptest.Open(t, acptest.NewRuntime(t, strategy, tacklr.AgentOptions{}))
	sid := c.NewSession("/tmp")

	turn1, _ := c.Say(sid, "do the plan")
	mu.Lock()
	phase = "turn2"
	mu.Unlock()
	end2, turn2 := c.Say(sid, "continue")

	if acptest.StopReason(turn1) != "end_turn" {
		t.Fatalf("turn1 = %v", turn1)
	}
	blob, _ := json.Marshal(append(turn2, end2))
	out := string(blob)
	for _, want := range []string{"Alpha", "completed", "Beta", "turn2 continued"} {
		if !strings.Contains(out, want) {
			t.Fatalf("second turn should list the restored plan (Alpha completed, Beta left) and end; missing %q in %s", want, out)
		}
	}
}
