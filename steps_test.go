package tacklr_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/workspace"
)

// TestInfer_streamsAReplyWhoseRoundTheNextRequestAnswers: Infer streams
// the reply's text as deltas, then each complete message with its full
// text, and returns its messages, its calls and each call's tool timeout.
// Once the round commits, the next request carries the call's result; a
// request after tools that fails, or that a model cannot send, fails with
// ErrModelAfterTools. A reply without calls has its text as its result.
func TestInfer_streamsAReplyWhoseRoundTheNextRequestAnswers(t *testing.T) {
	lookup := tacklr.NewTool(tacklr.ToolConfig{
		Name: "lookup", DisplayName: "Look up {q}", Category: message.ToolCategoryRead, Timeout: time.Minute,
		Handler: func(context.Context) (string, error) { return "found", nil },
	})
	model, provider := scripted(t,
		[]tacklr.LLMResponseChunk{
			{Type: session.EventReasoning, Content: "think", IsComplete: true},
			{Type: session.EventMessage, Content: "hel"},
			{Type: session.EventMessage, Content: "lo", IsComplete: true},
			// A provider may name a call by its call id alone.
			calls(message.ToolCall{CallID: "c1", Name: "lookup", Arguments: `{"q":"x"}`}),
		},
		[]tacklr.LLMResponseChunk{failure(fmt.Errorf("%w: upstream", tacklr.ErrNetwork))},
		[]tacklr.LLMResponseChunk{text("all done")},
	)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{lookup}}
	h := newHarness(t, "infer", opts)
	prompt(t, h, "go")
	s := newStream(t)

	first, firstErr := h.Infer(t.Context(), s.out)
	r := tacklr.NewRound(first)
	ran, runErr := h.RunCall(t.Context(), r.Calls[0], s.out)
	r.Calls[0] = ran
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, s.out)
	rows, _ := h.Checkpoint()
	opts.Model = openai.New(http.DefaultClient).WithModel("gpt-4o")
	keyless := newHarness(t, "infer", opts)
	restoreErr := keyless.RestoreCheckpoint(rows)
	_, unsent := keyless.Infer(t.Context(), newStream(t).out)
	_, failed := h.Infer(t.Context(), s.out)
	last, lastErr := h.Infer(t.Context(), s.out)

	if err := errors.Join(firstErr, runErr, commitErr, restoreErr, lastErr); err != nil {
		t.Fatal(err)
	}
	roles := []message.Role{}
	for _, m := range first.Messages {
		roles = append(roles, m.Role)
	}
	if !slices.Equal(roles, []message.Role{message.RoleReasoning, message.RoleAssistant, message.RoleAssistant}) ||
		!slices.Equal(contents(first.Messages), []string{"think", "hello", ""}) || first.Calls[0].Name != "lookup" || r.Calls[0].Timeout != time.Minute {
		t.Fatalf("reply = %+v, want the reasoning, the text, then the call with its tool's timeout", first)
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
	if !slices.Equal(texts[:3], []string{"true@0:hel", "true@3:lo", "false@0:hello"}) || presented.Title != "Look up x" || presented.Category != message.ToolCategoryRead {
		t.Fatalf("events %q, call %+v; want the deltas at their offsets, the full text, and the call as its tool presents it", texts, presented)
	}
	if !errors.Is(unsent, tacklr.ErrModelAfterTools) || !errors.Is(failed, tacklr.ErrModelAfterTools) {
		t.Fatalf("after tools: unsent %v, failed %v, want ErrModelAfterTools", unsent, failed)
	}
	answered := provider.sent()[2].msgs
	if got := answered[len(answered)-1]; last.Result != "all done" || got.Role != message.RoleTool || got.Content != "found" {
		t.Fatalf("result %q after a request that ended with %+v, want the call's result sent and the text as the result", last.Result, got)
	}
}

// TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls: a round committed
// with a call that has no result (a prompt moved past it) gives that call a
// cancelled result, then commits the prompt. A commit with an ended
// context, a call with a result and no body, or content the model cannot
// read fails and changes nothing.
func TestCommit_aPromptPastAnOpenRoundCancelsItsOpenCalls(t *testing.T) {
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

// TestCommit_cutsAResultTooLargeForTheWindow: a result too large for the
// window even with nothing left to summarize is cut to fit, on a rune
// boundary. With an Overflow its full text is first kept and the cut
// result says where; without, the note says it was not kept. A Keep or a
// count that fails, or a round with no result to cut, fails the commit.
func TestCommit_cutsAResultTooLargeForTheWindow(t *testing.T) {
	ascii, runes := strings.Repeat("x", 2000)+"END", strings.Repeat("─", 700)
	cases := []struct {
		name     string
		full     string
		overflow string // "", "writable" or "read-only"
		count    func([]*message.Message) (int, error)
		prompt   bool
		note     string // the cut result's note
		fail     string // the commit's error
	}{
		{name: "kept", full: ascii, overflow: "writable", count: windowTokens, note: "The full output is at /workspace/work/overflow/trim/b1.txt; read it with start and end."},
		{name: "not kept", full: runes, count: windowTokens, note: "The full output was not kept."},
		{name: "a keep that fails", full: ascii, overflow: "read-only", count: windowTokens, fail: "stage full tool result"},
		// The prompt's commit counts once, the round's once, then the cut.
		{name: "a count that fails after a cut", full: ascii, count: failAfter(2), fail: "count tokens"},
		{name: "a prompt with no result to cut", full: ascii, count: windowTokens, prompt: true, fail: "the round needs 2005 tokens"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			big := tacklr.NewTool(tacklr.ToolConfig{Name: "big", Handler: func(context.Context) (string, error) { return tc.full, nil }})
			dir := t.TempDir()
			ms, err := vfs.Tree(vfs.At("work", vfs.Local(dir)))(t.Context(), "trim", vfs.Request{})
			if err != nil {
				t.Fatal(err)
			}
			model, _ := counted(t, tc.count)
			opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 500, Tools: []*tacklr.Tool{big}}
			if tc.overflow != "" {
				if opts.Overflow, err = workspace.Overflow(ms, "/workspace/work/overflow"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.overflow == "read-only" {
				if err := os.Chmod(dir, 0o555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			}
			h := newHarness(t, "trim", opts)
			if err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{userMsg("go")}}, newStream(t).out); err != nil {
				t.Fatal(err)
			}

			if tc.prompt {
				err = h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{userMsg(tc.full)}}, newStream(t).out)
			} else {
				_, err = runRound(t, h, call("b1", "big", "{}"))
			}

			window := checkpoint(t, h).window
			cut := window[len(window)-1].Content
			if tc.fail != "" {
				if err == nil || !strings.Contains(err.Error(), tc.fail) || len(window) != 1 {
					t.Fatalf("commit = %v with window %q, want %q and the window kept", err, contents(window), tc.fail)
				}
				return
			}
			if err != nil || !utf8.ValidString(cut) || cut[:3] != tc.full[:3] || !strings.HasSuffix(cut, tc.note+"]") || len(cut) >= len(tc.full) {
				t.Fatalf("result = %q %v, want it cut on a rune with %q", cut, err, tc.note)
			}
			if kept, _ := ms.Route(t.Context(), "/workspace/work/overflow/trim/b1.txt").ReadFile(t.Context()); tc.overflow != "" && string(kept) != tc.full {
				t.Fatalf("kept = %q, want the full result", kept)
			}
		})
	}
}

// TestCommit_countsTokensOnlyNearTheWindow: with the provider's count of
// the last request, a round far below the window commits without a token
// count request, and one near it is counted.
func TestCommit_countsTokensOnlyNearTheWindow(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inputTokens int
		wantCounts  int
	}{
		{"far below the window", 100, 0},
		{"near the window", 7000, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var counts atomic.Int32
			model, _ := counted(t, func([]*message.Message) (int, error) { counts.Add(1); return tc.inputTokens, nil },
				[]tacklr.LLMResponseChunk{calls(call("f1", "free", "{}"))})
			free := tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { return "free-ok", nil }})
			h := newHarness(t, "counts", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{free}})
			out := newStream(t).out
			reply, err := h.Infer(t.Context(), out)
			if err != nil {
				t.Fatal(err)
			}
			r := tacklr.NewRound(reply)
			if r.Calls[0], err = h.RunCall(t.Context(), r.Calls[0], out); err != nil {
				t.Fatal(err)
			}
			counts.Store(0)

			err = h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)

			if n := int(counts.Load()); err != nil || n != tc.wantCounts {
				t.Fatalf("commit = %v with %d token count requests, want %d", err, n, tc.wantCounts)
			}
		})
	}
}
