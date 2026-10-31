package tacklr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// roundsHarness is a turn whose model answers each request with the calls
// of calls, then "done"; gate parks for permission and free does not. runs
// counts each tool's runs.
func roundsHarness(t *testing.T, opts AgentOptions, calls ...ToolCall) (*TurnManager, AgentOptions, map[string]int) {
	t.Helper()
	runs := map[string]int{}
	gate := NewTool(ToolConfig{Name: "gate", OnCall: []OnCallFunc{ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { runs["gate"]++; return "gate-ok", nil }})
	free := NewTool(ToolConfig{Name: "free", Handler: func(context.Context) (string, error) { runs["free"]++; return "free-ok", nil }})
	asked := false
	opts.Model = &scriptedModel{InvokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		if asked {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			return
		}
		asked = true
		ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: calls, IsComplete: true}
	}}
	opts.Tools = append(opts.Tools, gate, free)
	h := mustNewTurnManager(t, opts)
	t.Cleanup(h.Close)
	return h, opts, runs
}

// replyRound absorbs a prompt, gets the model's reply with calls, and runs
// them.
func replyRound(t *testing.T, h *TurnManager) ([]ToolStep, error) {
	t.Helper()
	out := drainEventCh()
	if err := h.absorbUser(t.Context(), &Message{Role: RoleUser, Content: "go"}, out); err != nil {
		t.Fatal(err)
	}
	if _, err := h.runInference(t.Context(), out); err != nil {
		t.Fatal(err)
	}
	return h.runTools(t.Context(), 1, nil, nil, out)
}

func contents(msgs []*Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Content)
	}
	return out
}

// TestOpenRound_aNewPromptCancelsItsOpenCalls: a prompt while a round is
// open gives each call without a result a cancelled result, commits the
// round, then commits the prompt. A commit that fails keeps the round open.
func TestOpenRound_aNewPromptCancelsItsOpenCalls(t *testing.T) {
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "cancel-round", MaxWindowSize: 8192},
		toolCall("g1", "gate", "{}"), toolCall("f1", "free", "{}"))
	if _, err := replyRound(t, h); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	failedErr := h.absorbUser(cancelled, &Message{Role: RoleUser, Content: "instead"}, drainEventCh())
	keptOpen := h.session.round != nil
	err := h.absorbUser(t.Context(), &Message{Role: RoleUser, Content: "instead"}, drainEventCh())

	got := contents(h.context.Messages())
	if !errors.Is(failedErr, context.Canceled) || !keptOpen {
		t.Fatalf("absorb with a cancelled context = %v, round open %v, want it to fail and keep the round", failedErr, keptOpen)
	}
	if err != nil || len(got) != 5 || got[2] != CancelledToolResultContent || got[3] != "free-ok" || got[4] != "instead" || h.session.round != nil {
		t.Fatalf("window = %q %v, want the cancelled gate, the free result, then the new prompt", got, err)
	}
}

// TestOpenRound_rejectsAnUnknownResume: answers for an interrupt the round
// does not hold fail the step; a child result for a call the round no
// longer holds (a retry after it committed) changes nothing.
func TestOpenRound_rejectsAnUnknownResume(t *testing.T) {
	h, _, _ := roundsHarness(t, AgentOptions{sessionID: "outside", MaxWindowSize: 8192}, toolCall("g1", "gate", "{}"))
	if _, err := replyRound(t, h); err != nil {
		t.Fatal(err)
	}

	_, runErr := h.runTools(t.Context(), 2, map[string][]byte{"x1": []byte(`{}`)}, nil, drainEventCh())
	recordErr := h.recordToolResult(t.Context(), toolCall("x1", "spawn_specialist", "{}"), "out", false, drainEventCh())

	if !errors.Is(runErr, ErrInterruptNotFound) || recordErr != nil || h.session.round.Calls[0].Park == nil {
		t.Fatalf("run = %v, record = %v, want the unknown resume named and the round unchanged", runErr, recordErr)
	}
}

// TestCommitRound_trimsAResultTooLargeForTheWindow: a result too large for
// the window even with nothing left to summarize is cut to fit. With
// OverflowPath its full text is first saved where read finds it; without, the
// note says it was not kept. A save that fails fails the step.
func TestCommitRound_trimsAResultTooLargeForTheWindow(t *testing.T) {
	full := strings.Repeat("x", 2000) + "END"
	cases := []struct {
		name     string
		overflow string
		block    string // a path made a file or directory first, so the save fails
		want     string
	}{
		{name: "kept on a mount", overflow: "/workspace/work/overflow", want: "The full output is at /workspace/work/overflow/trim/b1.txt; read it with start and end."},
		{name: "not kept", want: "The full output was not kept."},
		{name: "a directory that cannot be made", overflow: "/workspace/work/blocked", block: "file", want: "stage full tool result"},
		{name: "a file that cannot be written", overflow: "/workspace/work/overflow", block: "dir", want: "stage full tool result"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := mustMountTree(t, "trim", vfs.At("work", vfs.Local(t.TempDir())))
			switch tc.block {
			case "file":
				if err := ms.Route(t.Context(), tc.overflow).WriteFile(t.Context(), []byte("x")); err != nil {
					t.Fatal(err)
				}
			case "dir":
				if err := ms.Route(t.Context(), tc.overflow+"/trim/b1.txt").MkdirAll(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			big := NewTool(ToolConfig{Name: "big", Handler: func(context.Context) (string, error) { return full, nil }})
			h, _, _ := roundsHarness(t, AgentOptions{sessionID: "trim", MaxWindowSize: 500, mountSession: ms, OverflowPath: tc.overflow, Tools: []*Tool{big}},
				toolCall("b1", "big", "{}"))

			_, err := replyRound(t, h)

			if tc.block != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || h.session.round == nil {
					t.Fatalf("run = %v, want %q and the round kept open", err, tc.want)
				}
				return
			}
			window := h.context.Messages()
			result := window[len(window)-1]
			if err != nil || !strings.HasPrefix(result.Content, "xxx") || !strings.HasSuffix(result.Content, tc.want+"]") || len(result.Content) >= len(full) {
				t.Fatalf("result = %q %v, want it cut with %q", result.Content, err, tc.want)
			}
			if tc.overflow != "" {
				read, err := h.findTool("read", "").invoke(t.Context(), `{"path":"/workspace/work/overflow/trim/b1.txt"}`, turnRuntime(h))
				if err != nil || !strings.Contains(read.Output, "END") {
					t.Fatalf("read the full output = %q %v", read.Output, err)
				}
			}
		})
	}
}

// TestOpenRound_anAwaitedResultCommitsWhenItsEpisodeCannotBeKept: a
// specialist's result is the call's result even when the Brain cannot keep
// it as an Episode (the catalog has no Episode kind).
func TestOpenRound_anAwaitedResultCommitsWhenItsEpisodeCannotBeKept(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{Kind: "Fact", IsParent: true}))
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{sessionID: "episode-fails", MaxWindowSize: 8192, Model: &scriptedModel{}, Brain: eng, SearchNamespace: mustNS(t, "org", "acme")})
	t.Cleanup(h.Close)
	call := toolCall("sp1", "spawn_specialist", "{}")
	h.context.Restore([]*Message{userMsg("go")})
	h.session.round = newOpenRound([]*Message{{Role: RoleAssistant, ToolCalls: []ToolCall{call}}}, []ToolCall{call})
	h.session.round.Calls[0].AwaitID = "child"

	err = h.recordToolResult(t.Context(), call, "found it", false, drainEventCh())

	if got := contents(h.context.Messages()); err != nil || len(got) != 3 || got[2] != "found it" || h.session.round != nil {
		t.Fatalf("window = %q %v, want the specialist's result committed", got, err)
	}
}
