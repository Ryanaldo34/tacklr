package acp_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
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
// resumes the turn with it, to end_turn. An answer the
// agent cannot use, or an error from the client, ends the prompt with an
// error, and a client that hangs up while asked cancels the turn. A park
// the client cannot answer (no form support, an interrupt with no ACP
// form, or a client on HTTP) ends the prompt with an error that the turn
// needs user input.
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
	hs, srv := startACPWSServer(t, r)
	form := dial(t, hs, `{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{},"url":null}}}`)
	plain := dial(t, hs, `{"protocolVersion":1,"clientCapabilities":{"_meta":{"tacklr":{"vfs":{"tokenRefresh":true}}}}}`)
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
		client *wsClient
		tool   string
		answer func(map[string]any) any
		// want is the tool's result for a turn that ends; else a part of
		// the error the prompt ends with ("" for any error).
		want string
		ends bool
	}{
		{"allowedPermission", plain, "sensitive", permission(map[string]any{"outcome": "selected", "optionId": "allow-once"}), "approved", true},
		{"untitledPermissionShowsTheToolName", plain, "untitled", func(req map[string]any) any {
			if req["params"].(map[string]any)["toolCall"].(map[string]any)["title"] != "untitled" {
				return permission(map[string]any{"outcome": "cancelled"})(req)
			}
			return permission(map[string]any{"outcome": "selected", "optionId": "allow-once"})(req)
		}, "approved untitled", true},
		{"acceptedChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{"choice": "A"}}), "selected: A", true},
		{"cancelledPermission", plain, "sensitive", permission(map[string]any{"outcome": "cancelled"}), "cancelled", false},
		{"unknownPermissionOutcome", plain, "sensitive", permission(map[string]any{"outcome": "nope"}), "", false},
		{"permissionWithNoOption", plain, "sensitive", permission(map[string]any{"outcome": "selected"}), "", false},
		{"permissionNotAnObject", plain, "sensitive", reply("not-an-object"), "", false},
		{"clientError", plain, "sensitive", func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "error": map[string]any{"code": -32000, "message": "nope"}}
		}, "", false},
		{"clientErrorOnChoice", form, "ask_user", func(req map[string]any) any {
			return map[string]any{"jsonrpc": "2.0", "id": req["id"], "error": map[string]any{"code": -32000, "message": "nope"}}
		}, "", false},
		{"declinedChoice", form, "ask_user", reply(map[string]any{"action": "decline"}), "", false},
		{"unknownChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{"choice": "nope"}}), "", false},
		{"noChoice", form, "ask_user", reply(map[string]any{"action": "accept", "content": map[string]any{}}), "", false},
		{"unknownElicitationAction", form, "ask_user", reply(map[string]any{"action": "nope"}), "", false},
		{"elicitationNotAnObject", form, "ask_user", reply("not-an-object"), "", false},
		{"noFormSupport", plain, "ask_user", nil, "user input", false},
		{"interruptWithNoForm", form, "wait_child", nil, "user input", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.client.t = t
			end, frames := c.client.until(c.client.prompt(c.client.newSession(), c.tool), c.answer)

			if c.ends {
				blob, _ := json.Marshal(frames)
				if res, _ := end["result"].(map[string]any); res["stopReason"] != "end_turn" || !strings.Contains(string(blob), c.want) {
					t.Fatalf("prompt ended with %v after %s; want end_turn after %q", end, blob, c.want)
				}
				return
			}
			if msg, _ := end["error"].(map[string]any)["message"].(string); end["error"] == nil || !strings.Contains(msg, c.want) {
				t.Fatalf("prompt ended with %v; want an error with %q", end, c.want)
			}
		})
	}

	t.Run("hangUpWhileAsked", func(t *testing.T) {
		h := dial(t, hs, `{"protocolVersion":1}`)
		sid := h.newSession()
		h.prompt(sid, "sensitive")
		for h.frame()["method"] != "session/request_permission" {
			continue
		}
		_ = h.conn.Close(websocket.StatusNormalClosure, "")
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		sub, err := r.Client.Subscribe(ctx, session.ID(sid), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Close() }()

		var end error
		for ev := range sub.Events() {
			if ev.Type == session.EventError {
				end = ev.Error
				break
			}
		}

		if !errors.Is(end, context.Canceled) {
			t.Fatalf("turn of a client that hung up while asked ended with %v, want it cancelled", end)
		}
	})

	t.Run("httpClient", func(t *testing.T) {
		proto := srv.Protocols[0]
		sid := acptest.SessionID(t, acptest.Serve(t, r, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		rec := acptest.Serve(t, r, proto, prompt("2", sid, `[{"type":"text","text":"ask_user"}]`))
		if !strings.Contains(rec.Body.String(), "user input") {
			t.Fatalf("park on HTTP = %s, want an error that the turn needs user input", rec.Body.String())
		}
	})
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
	srv := acptest.NewServer(t, acptest.NewRuntime(t, strategy, tacklr.AgentOptions{}), server.NewMemoryWireStore())
	sid := acptest.SessionID(t, srv.RPC(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))

	turn1 := srv.RPC(prompt("2", sid, `[{"type":"text","text":"do the plan"}]`))
	mu.Lock()
	phase = "turn2"
	mu.Unlock()
	turn2 := srv.RPC(prompt("3", sid, `[{"type":"text","text":"continue"}]`))

	if stopReason(t, turn1) != "end_turn" {
		t.Fatalf("turn1 = %s", turn1.Body.String())
	}
	out := turn2.Body.String()
	for _, want := range []string{"Alpha", "completed", "Beta", "turn2 continued"} {
		if !strings.Contains(out, want) {
			t.Fatalf("second turn should list the restored plan (Alpha completed, Beta left) and end; missing %q in %s", want, out)
		}
	}
}
