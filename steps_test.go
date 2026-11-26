package tacklr_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
)

// lookup is a read tool of a minute's timeout that finds "found".
func lookup() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name: "lookup", DisplayName: "Look up {q}", Category: message.ToolCategoryRead, Timeout: time.Minute,
		Handler: func(context.Context) (string, error) { return "found", nil },
	})
}

// TestInfer_streamsAReplyAsDeltasThenItsCompleteMessages: Infer streams
// the reply's text as deltas at their offsets, then each complete message
// with its full text and each call as its tool presents it, and returns
// its messages, its calls and each call's tool timeout.
func TestInfer_streamsAReplyAsDeltasThenItsCompleteMessages(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t, []tacklr.LLMResponseChunk{
		{Type: session.EventReasoning, Content: "think", IsComplete: true},
		{Type: session.EventMessage, Content: "hel"},
		{Type: session.EventMessage, Content: "lo", IsComplete: true},
		calls(message.ToolCall{CallID: "c1", Name: "lookup", Arguments: `{"q":"x"}`}),
	})
	h := newHarness(t, "infer", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{lookup()}})
	prompt(t, h, "go")
	s := newStream(t)

	got, err := h.Infer(t.Context(), s.out)

	var roles []message.Role
	for _, m := range got.Messages {
		roles = append(roles, m.Role)
	}
	if r := tacklr.NewRound(got); err != nil || !slices.Equal(roles, []message.Role{message.RoleReasoning, message.RoleAssistant, message.RoleAssistant}) ||
		!slices.Equal(contents(got.Messages), []string{"think", "hello", ""}) || got.Calls[0].Name != "lookup" || r.Calls[0].Timeout != time.Minute {
		t.Fatalf("reply = %+v %v, want the reasoning, the text, then the call with its tool's timeout", got, err)
	}
	var texts []string
	var presented message.ToolCall
	for _, ev := range s.events() {
		switch ev.Type {
		case session.EventMessage:
			texts = append(texts, fmt.Sprintf("%v@%d:%s", ev.Delta, ev.Offset, ev.Content))
		case session.EventFunctionCall:
			presented = ev.ToolCalls[0]
		}
	}
	if !slices.Equal(texts, []string{"true@0:hel", "true@3:lo", "false@0:hello"}) || presented.Title != "Look up x" || presented.Category != message.ToolCategoryRead {
		t.Fatalf("events %q, call %+v; want the deltas at their offsets, the full text, and the call as its tool presents it", texts, presented)
	}
}

// TestInfer_theRequestAfterARoundCarriesItsResults: once a round commits,
// the next request carries its call's result, and a reply without calls
// has its text as its result.
func TestInfer_theRequestAfterARoundCarriesItsResults(t *testing.T) {
	t.Parallel()
	model, provider := scripted(t, []tacklr.LLMResponseChunk{calls(call("c1", "lookup", `{"q":"x"}`))}, []tacklr.LLMResponseChunk{text("all done")})
	h := newHarness(t, "after-round", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{lookup()}})
	prompt(t, h, "go")
	out := newStream(t).out
	first, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runRound(t, h, first.Calls...); err != nil {
		t.Fatal(err)
	}

	last, err := h.Infer(t.Context(), out)

	answered := testkit.LastMessage(provider.sent()[1].msgs)
	if err != nil || last.Result != "all done" || answered.Role != message.RoleTool || answered.Content != "found" {
		t.Fatalf("result %q %v after a request that ended with %+v, want the call's result sent and the text as the result", last.Result, err, answered)
	}
}

// TestInfer_aRequestAfterToolsTheModelCannotSendFailsWithErrModelAfterTools:
// a request after a tool round that the model cannot send, as any that
// fails, fails with ErrModelAfterTools.
func TestInfer_aRequestAfterToolsTheModelCannotSendFailsWithErrModelAfterTools(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{lookup()}}
	h := newHarness(t, "keyless", opts)
	prompt(t, h, "go")
	if _, err := runRound(t, h, call("c1", "lookup", `{"q":"x"}`)); err != nil {
		t.Fatal(err)
	}
	rows, _ := h.Checkpoint()
	opts.Model = openai.New(http.DefaultClient).WithModel("gpt-4o")
	keyless := newHarness(t, "keyless", opts)
	if err := keyless.RestoreCheckpoint(rows); err != nil {
		t.Fatal(err)
	}

	_, err := keyless.Infer(t.Context(), newStream(t).out)

	if !errors.Is(err, tacklr.ErrModelAfterTools) || !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("infer = %v, want ErrModelAfterTools for the missing key", err)
	}
}

// TestInfer_eachRequestPairsEveryCallWithItsResult: a provider refuses a
// request with a call and no result; a round a prompt moved past gives
// its open call a result before the next request.
func TestInfer_eachRequestPairsEveryCallWithItsResult(t *testing.T) {
	t.Parallel()
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }})
	model, provider := scripted(t)
	h := newHarness(t, "paired", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{gate, lookup()}})
	prompt(t, h, "go")
	out := newStream(t).out
	r := tacklr.NewRound(reply(call("g1", "gate", "{}"), call("l1", "lookup", "{}")))
	for i := range r.Calls {
		r.Calls[i], _ = h.RunCall(t.Context(), r.Calls[i], out)
	}
	if err := h.Commit(t.Context(), tacklr.CommitIn{Round: r, Users: []*message.Message{userMsg("instead")}}, out); err != nil {
		t.Fatal(err)
	}

	_, err := h.Infer(t.Context(), out)

	open := map[string]bool{}
	for _, m := range provider.sent()[0].msgs {
		for _, c := range m.ToolCalls {
			open[c.CallID] = true
		}
		if m.Role == message.RoleTool {
			delete(open, m.ToolCallID)
		}
	}
	if err != nil || len(open) != 0 {
		t.Fatalf("infer = %v with calls %v open, want each call paired with its result", err, open)
	}
}

// TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls: a round committed
// with a call that has no result (a prompt moved past it) gives that call a
// cancelled result, then commits the prompt. A commit with an ended
// context, a call with a result and no body, or content the model cannot
// read fails and changes nothing.
func TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls(t *testing.T) {
	t.Parallel()
	gate := tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }})
	free := tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "free-ok", nil }})
	h := newHarness(t, "cancel-round", tacklr.AgentOptions{Model: testkit.TextHTTPModel(t, nil), MaxWindowSize: 8192, Tools: []*tacklr.Tool{gate, free}})
	prompt(t, h, "go")
	out := newStream(t).out
	r := tacklr.NewRound(reply(call("g1", "gate", "{}"), call("f1", "free", "{}")))
	for i := range r.Calls {
		r.Calls[i], _ = h.RunCall(t.Context(), r.Calls[i], out)
	}
	parked, complete := r.Parked(), r.Complete()
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	noBody := *r
	noBody.Calls = slices.Clone(r.Calls)
	noBody.Calls[0].Done, noBody.Calls[0].Body = true, nil
	image := &message.Message{Role: message.RoleUser, ContentParts: []message.ContentPart{{
		Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"},
	}}}
	in := tacklr.CommitIn{Round: r, Users: []*message.Message{userMsg("instead")}}

	failures := []error{
		h.Commit(ended, in, out),
		h.Commit(t.Context(), tacklr.CommitIn{Round: &noBody}, out),
		h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{image}}, out),
	}
	unchanged := contents(checkpoint(t, h).window)
	err := h.Commit(t.Context(), in, out)

	if parked != 0 || complete || !errors.Is(failures[0], context.Canceled) || !errors.Is(failures[1], session.ErrNotFound) || failures[2] == nil || !strings.Contains(failures[2].Error(), "image/png") {
		t.Fatalf("parked call %d, failed commits = %v; want the gate parked, then the cancel, the missing body and the unsupported content", parked, failures)
	}
	if got := contents(checkpoint(t, h).window); err != nil || !slices.Equal(unchanged, []string{"go"}) ||
		!slices.Equal(got, []string{"go", "", tacklr.CancelledToolResultContent, "free-ok", "instead"}) {
		t.Fatalf("window %q after failed commits, then %q %v; want the cancelled gate, the free result, then the new prompt", unchanged, got, err)
	}
}

// TestCommit_failsARoundTooLargeWithNoResultToCut: a prompt too large for
// the window has no result to cut, so its commit fails and keeps the
// window.
func TestCommit_failsARoundTooLargeWithNoResultToCut(t *testing.T) {
	t.Parallel()
	model, _ := counted(t, windowTokens)
	h := newHarness(t, "too-large", tacklr.AgentOptions{Model: model, MaxWindowSize: 500})
	prompt(t, h, "go")

	err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{userMsg(strings.Repeat("x", 2000))}}, newStream(t).out)

	if window := contents(checkpoint(t, h).window); err == nil || !strings.Contains(err.Error(), "the round needs") || !slices.Equal(window, []string{"go"}) {
		t.Fatalf("commit = %v with window %q, want it refused and the window kept", err, window)
	}
}
