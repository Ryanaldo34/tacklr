package acp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/session"
)

// TestACP_aTurnOutlivesItsConnection: a message that is not a request is
// refused, and a response to nothing the agent asked gets no answer;
// session/cancel ends a running prompt with stopReason
// cancelled, and the next prompt runs; and a turn whose client hangs up
// goes on, so a new connection that loads the session gets the rest of
// it.
func TestACP_aTurnOutlivesItsConnection(t *testing.T) {
	t.Parallel()
	started := map[string]chan struct{}{"cancel": make(chan struct{}, 1), "hang up": make(chan struct{}, 1)}
	release := make(chan struct{})
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		text := msgs[len(msgs)-1].Content
		select {
		case started[text] <- struct{}{}:
		default:
		}
		switch text {
		case "cancel":
			<-ctx.Done()
		case "hang up":
			<-release
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "finished without you", IsComplete: true}
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "after the cancel", IsComplete: true}
		}
	})
	url := acptest.Listen(t, acptest.Server(acptest.NewRuntime(t, model, tacklr.AgentOptions{}), nil))
	wait := func(ch chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for the model call")
		}
	}
	c := acptest.Connect(t, url, `{"protocolVersion":1}`)

	c.Send(`{`)
	c.Send(`{"jsonrpc":"2.0","id":99}`)
	notRequests := []any{c.Frame()["error"], c.Frame()["error"]}
	c.Send(`{"jsonrpc":"2.0","id":99,"result":{}}`)
	created := c.Request("session/new", `{"cwd":"/tmp"}`)
	afterStray := c.Frame()
	sid := acptest.SessionID(t, afterStray)
	id := c.Prompt(sid, `[{"type":"text","text":"cancel"}]`)
	wait(started["cancel"])
	c.Send(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sid + `"}}`)
	cancelled, _ := c.Until(id, nil)
	next, _ := c.Say(sid, "again")
	gone := acptest.Connect(t, url, `{"protocolVersion":1}`)
	left := gone.NewSession("/tmp")
	gone.Prompt(left, `[{"type":"text","text":"hang up"}]`)
	wait(started["hang up"])
	gone.Close()
	back := acptest.Connect(t, url, `{"protocolVersion":1}`)
	loaded := back.Call("session/load", `{"sessionId":"`+left+`","cwd":"/tmp"}`)
	close(release)
	var rest string
	for !strings.Contains(rest, "finished") {
		for _, u := range acptest.Updates([]map[string]any{back.Frame()}) {
			if content, ok := u["content"].(map[string]any); ok {
				rest += content["text"].(string)
			}
		}
	}

	for _, e := range notRequests {
		if code, _ := e.(map[string]any)["code"].(float64); int(code) != acp.CodeInvalidRequest {
			t.Errorf("message that is not a request = %v, want invalid request", e)
		}
	}
	if afterStray["id"] != float64(created) {
		t.Errorf("after a response to nothing it asked, the agent sent %v; want the next request's answer", afterStray)
	}
	if acptest.StopReason(cancelled) != "cancelled" || acptest.StopReason(next) != "end_turn" {
		t.Errorf("cancelled prompt = %v, next = %v; want cancelled, then end_turn", cancelled, next)
	}
	if acptest.Result(t, loaded)["sessionId"] != left || rest != "finished without you" {
		t.Errorf("load = %v, then %q; want the rest of the turn the first client left", loaded, rest)
	}
}
