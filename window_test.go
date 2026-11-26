package tacklr_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// vectors is the host's embedding provider: it embeds every text as one
// fixed vector. With cancel it ends the step's context as it embeds, as a
// worker that shuts down does.
type vectors struct{ cancel context.CancelFunc }

func (v vectors) Embed(_ context.Context, texts []string) ([][]float32, error) {
	if v.cancel != nil {
		v.cancel()
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0}
	}
	return out, nil
}

func (vectors) Model() string { return "vector-model" }

// embedded is a Brain whose store embeds with v, one text at a time.
func embedded(t *testing.T, v vectors) *brain.Engine {
	t.Helper()
	eng, err := brain.NewEngine(brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: v, BatchSize: 1})))
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

// planned is a session whose window is a goal and its plan of todos,
// committed as the model's create_plan.
func planned(t *testing.T, id string, opts tacklr.AgentOptions, todos ...tacklr.Todo) *tacklr.Harness {
	t.Helper()
	h := newHarness(t, id, opts)
	prompt(t, h, "goal")
	args, _ := json.Marshal(map[string]any{"plan": "ship", "todos": todos})
	if _, err := runRound(t, h, call("plan", "create_plan", string(args))); err != nil {
		t.Fatal(err)
	}
	return h
}

// overPressure is a planned session of a Brain whose store embeds with v,
// with two large prompts that a 100-character follow-up takes past the
// window's pressure. The model counts with count and answers reply.
func overPressure(t *testing.T, count func([]*message.Message) (int, error), v vectors, reply ...[]tacklr.LLMResponseChunk) *tacklr.Harness {
	t.Helper()
	model, _ := counted(t, count, reply...)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 1000, Brain: embedded(t, v), SearchNamespace: storetest.Namespace(t, "org", "acme")}
	h := planned(t, "collapse", opts, tacklr.Todo{Title: "t"})
	prompt(t, h, strings.Repeat("a", 350))
	prompt(t, h, strings.Repeat("b", 350)+" "+collapsedToken)
	return h
}

const collapsedToken = "unique-collapsed-token"

// TestCommit_collapsesAWindowOverPressureIntoASavedSummary: a round that
// takes the window past its pressure collapses everything after the goal
// and the plan into one summary. With a Brain the collapsed messages are
// this session's saved messages: search finds them, open reads one
// exactly, and the summary says so.
func TestCommit_collapsesAWindowOverPressureIntoASavedSummary(t *testing.T) {
	t.Parallel()
	h := overPressure(t, windowTokens, vectors{}, []tacklr.LLMResponseChunk{text("summary")})

	err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{userMsg(strings.Repeat("f", 100))}}, newStream(t).out)

	window := checkpoint(t, h).window
	if err != nil || len(window) != 4 || !tacklr.IsPlanDocument(window[1]) || !strings.Contains(window[2].Content, "Use the search tool") || window[3].Content != strings.Repeat("f", 100) {
		t.Fatalf("window = %q %v, want the goal, the plan, a summary that points at search, and the round", contents(window), err)
	}
	var found brain.Page
	if err := json.Unmarshal([]byte(result(t, h, call("s1", "search", `{"query":"`+collapsedToken+`"}`))), &found); err != nil {
		t.Fatal(err)
	}
	hits := slices.Collect(found.Results.Objects())
	if len(hits) == 0 || hits[0].Kind != brain.KindSessionMessage {
		t.Fatalf("search the collapsed history = %+v, want its saved message first", hits)
	}
	if opened := result(t, h, call("o1", "open", `{"ref":"`+hits[0].ID.String()+`"}`)); !strings.Contains(opened, collapsedToken) {
		t.Fatalf("open the saved message = %s, want it exactly", opened)
	}
}

// TestCommit_aFailureDuringWindowUpkeepFailsTheCommitAndKeepsTheWindow: a
// count, a save of the collapsed messages (its context ends as it embeds)
// or a summary request that fails stops the collapse: the commit fails
// and the window is as it was.
func TestCommit_aFailureDuringWindowUpkeepFailsTheCommitAndKeepsTheWindow(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		count    func([]*message.Message) (int, error)
		reply    tacklr.LLMResponseChunk
		ends     bool
		collapse bool
		fail     string
	}{
		{name: "a count", count: failAfter(3), reply: text("summary"), fail: "count tokens"},
		{name: "a save", count: windowTokens, reply: text("summary"), ends: true, fail: "save session messages"},
		{name: "a summary request", count: windowTokens, reply: failure(fmt.Errorf("%w: stream failed", tacklr.ErrNetwork)), collapse: true, fail: "context compress stream failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			v := vectors{}
			if tc.ends {
				v.cancel = cancel
			}
			h := overPressure(t, tc.count, v, []tacklr.LLMResponseChunk{tc.reply})
			before := contents(checkpoint(t, h).window)
			in := tacklr.CommitIn{Users: []*message.Message{userMsg(strings.Repeat("f", 100))}}
			if tc.collapse {
				in = tacklr.CommitIn{Collapse: true}
			}

			err := h.Commit(ctx, in, newStream(t).out)

			if window := contents(checkpoint(t, h).window); err == nil || !strings.Contains(err.Error(), tc.fail) || !slices.Equal(window, before) {
				t.Fatalf("commit = %v with window %d messages, want %q and the window kept", err, len(window), tc.fail)
			}
		})
	}
}

// TestCompleteTodo_handsOffTheRestOfThePlan: completing a todo while
// others are open rebuilds the window as the goal, the plan, the model's
// handoff and a nudge to continue; the messages it replaces are this
// session's saved messages, which search finds. Completing the last open
// todo keeps the window.
func TestCompleteTodo_handsOffTheRestOfThePlan(t *testing.T) {
	t.Parallel()
	const token = "unique-retain-handoff-token"
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "acme")
	model, _ := scripted(t, []tacklr.LLMResponseChunk{text("the handoff")})
	lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Handler: func(context.Context) (string, error) { return token, nil }})
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Brain: eng, SearchNamespace: ns, Tools: []*tacklr.Tool{lookup}}
	h := newHarness(t, "handoff", opts)
	prompt(t, h, "goal")
	args, _ := json.Marshal(map[string]any{"plan": "ship", "todos": []tacklr.Todo{
		{Title: "A", Description: "first"}, {Title: "B", Status: tacklr.TodoStatusCompleted}, {Title: "C", Description: "next"},
	}})
	if _, err := runRound(t, h, call("plan", "create_plan", string(args))); err != nil {
		t.Fatal(err)
	}
	parts := &message.Message{Role: message.RoleUser, ContentParts: []message.ContentPart{
		{Type: message.ContentTypeInputText, Text: "look it up"},
		{Type: message.ContentTypeInputText},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "data:image/png;base64,AAAA"}},
	}}
	if err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{parts}}, newStream(t).out); err != nil {
		t.Fatal(err)
	}
	if _, err := runRound(t, h, call("call-session-1", "lookup", `{"q":"x"}`)); err != nil {
		t.Fatal(err)
	}

	first, firstErr := runRound(t, h, call("c3", "complete_todo", `{"title":"A"}`))
	handedOff := checkpoint(t, h).window
	last, lastErr := runRound(t, h, call("c4", "complete_todo", `{"title":"C"}`))

	if firstErr != nil || !strings.Contains(first.Calls[0].Body.Text(), `now starting "C"`) || len(handedOff) != 4 ||
		handedOff[0].Content != "goal" || !tacklr.IsPlanDocument(handedOff[1]) || handedOff[2].Content != "the handoff" || !strings.Contains(handedOff[3].Content, "Continue executing") {
		t.Fatalf("handoff = %v %q, window %q; want the goal, the plan, the handoff and the nudge", firstErr, first.Calls[0].Body.Text(), contents(handedOff))
	}
	if window := checkpoint(t, h).window; lastErr != nil || !strings.Contains(last.Calls[0].Body.Text(), "All todos completed") || len(window) != 6 {
		t.Fatalf("complete the last todo = %v %q, window %q; want the window kept with the round", lastErr, last.Calls[0].Body.Text(), contents(window))
	}
	scope := brain.Scope{Namespace: ns, SessionID: "handoff"}
	page, err := eng.Search(t.Context(), scope, brain.SearchRequest{Query: token, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	hits := slices.Collect(page.Results.Objects())
	if len(hits) != 1 || hits[0].Kind != brain.KindSessionMessage {
		t.Fatalf("search the handed-off history = %+v, want its saved result", hits)
	}
	saved, err := eng.Get(t.Context(), scope, hits[0].ID)
	if err != nil || !containsAll(saved.Content, "call-session-1", token) {
		t.Fatalf("saved message = %s %v, want the call's result", saved.Content, err)
	}
	page, err = eng.Search(t.Context(), scope, brain.SearchRequest{Query: "look it up"})
	if err != nil || len(slices.Collect(page.Results.Objects())) == 0 {
		t.Fatalf("search the prompt's text = %v, want it saved", err)
	}
}

// TestHandoff_fallsBackToThePlanWhenTheModelGivesNoHandoff: a handoff
// request that fails installs a handoff written from the plan.
func TestHandoff_fallsBackToThePlanWhenTheModelGivesNoHandoff(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t, []tacklr.LLMResponseChunk{failure(fmt.Errorf("%w: handoff stream failed", tacklr.ErrNetwork))})
	h := planned(t, "fallback", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, tacklr.Todo{Title: "First"}, tacklr.Todo{Title: "Ship", Description: "finish"})

	_, err := runRound(t, h, call("c1", "complete_todo", `{"title":"First"}`))

	if window := checkpoint(t, h).window; err != nil || len(window) != 4 || !strings.Contains(window[2].Content, "[in_progress] Ship: finish") {
		t.Fatalf("handoff = %v, window %q, want the goal, the plan, the fallback handoff and the nudge", err, contents(window))
	}
}

// TestHandoff_failsAndKeepsTheWindowWhenItsContextEnds: a handoff whose
// context ends during its save of the window, or after it, fails the
// commit; the window keeps its messages and the round.
func TestHandoff_failsAndKeepsTheWindowWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	audited, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithAudit(func(_ context.Context, ev brain.AuditEvent) error {
		if ev.Op == brain.AuditSessionSave {
			cancel() // the worker shuts down as the save lands
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	embedding, stop := context.WithCancel(t.Context())
	for name, tc := range map[string]struct {
		brain *brain.Engine
		ctx   context.Context
		want  string
	}{
		"a context that ends during the save": {embedded(t, vectors{cancel: stop}), embedding, "save session messages"},
		"a context that ends after the save":  {audited, ctx, "context canceled"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			model, _ := scripted(t)
			h := planned(t, "save-fail", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Brain: tc.brain, SearchNamespace: storetest.Namespace(t, "org", "acme")},
				tacklr.Todo{Title: "A"}, tacklr.Todo{Title: "B"})
			prompt(t, h, "kept-detail")
			out := newStream(t).out
			tcs := call("c1", "complete_todo", `{"title":"A"}`)
			r := tacklr.NewRound(reply(tcs))
			ran, err := h.RunCall(t.Context(), r.Calls[0], out)
			if err != nil {
				t.Fatal(err)
			}
			r.Calls[0] = ran

			err = h.Commit(tc.ctx, tacklr.CommitIn{Round: r}, out)

			if window := contents(checkpoint(t, h).window); !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), tc.want) || !slices.Equal(window[2:], []string{"kept-detail", "", r.Calls[0].Body.Text()}) {
				t.Fatalf("handoff = %v with window %q, want %v and the window with the round", err, window, tc.want)
			}
		})
	}
}

// TestCommit_sizesTheWindowFromUsageWhenTheProviderCannotCount: with a
// provider that has no token-count endpoint, each commit is sized from
// the input tokens the last response reported, plus the size of what was
// added since. A round that takes the window over pressure collapses it
// into a summary, and a result too large for the window is cut to fit.
// The round's reasoning was about the window the summary replaced, so the
// round keeps only its call and result.
func TestCommit_sizesTheWindowFromUsageWhenTheProviderCannotCount(t *testing.T) {
	t.Parallel()
	// Arrange
	const window = 100_000
	big := tacklr.NewTool(tacklr.ToolConfig{Name: "big", Handler: func(context.Context) (string, error) {
		return strings.Repeat("x", 2*window), nil
	}})
	thought := tacklr.LLMResponseChunk{Type: session.EventReasoning, Content: "read it all", IsComplete: true}
	p := &provider{replies: [][]tacklr.LLMResponseChunk{{text("reading")}, {thought, calls(call("b1", "big", "{}"))}, {text("summary")}}}
	model := testkit.CountedHTTPModel(t, p.respond, nil)
	h := newHarness(t, "uncounted", tacklr.AgentOptions{Model: model, MaxWindowSize: window, Tools: []*tacklr.Tool{big}})
	prompt(t, h, "go")
	out := newStream(t).out
	reading, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(t.Context(), tacklr.CommitIn{Round: &tacklr.Round{Reply: reading}}, out); err != nil {
		t.Fatal(err)
	}

	// Act
	big1, err := h.Infer(t.Context(), out)
	if err != nil {
		t.Fatal(err)
	}
	r := tacklr.NewRound(big1)
	if r.Calls[0], err = h.RunCall(t.Context(), tacklr.NewCall(big1.Calls[0], 0), out); err != nil {
		t.Fatal(err)
	}
	err = h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)

	// Assert
	got := checkpoint(t, h).window
	cut := got[len(got)-1].Content
	if err != nil || len(got) != 4 || !strings.HasPrefix(got[1].Content, "CONVERSATION SUMMARY") || len(got[2].ToolCalls) != 1 || !strings.Contains(cut, "[Output truncated to fit the context window") || len(cut) >= window {
		t.Fatalf("window = %d messages (%v), last %d bytes; want the goal, a summary, the call and its result cut under the window", len(got), err, len(cut))
	}
}

// TestCommit_aCollapseAfterARefusedRequestSummarizesTheWindow: after the
// provider refused a request as over the context window, a collapse with
// no counted request after the goal summarizes everything after it. A
// window of only the goal has nothing to collapse.
func TestCommit_aCollapseAfterARefusedRequestSummarizesTheWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		summary []tacklr.LLMResponseChunk
		answer  bool
		want    []string
		fail    error
	}{
		{name: "summarized", summary: []tacklr.LLMResponseChunk{text("summary")}, answer: true, want: []string{"go", "CONVERSATION SUMMARY\n────────────\nsummary"}},
		{name: "only the goal", want: []string{"go"}, fail: session.ErrContextLength},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Arrange
			model, _ := scripted(t, []tacklr.LLMResponseChunk{text("done")}, tc.summary)
			h := newHarness(t, "refit", tacklr.AgentOptions{Model: model, MaxWindowSize: 100_000})
			prompt(t, h, "go")
			out := newStream(t).out
			if tc.answer {
				done, err := h.Infer(t.Context(), out)
				if err != nil {
					t.Fatal(err)
				}
				if err := h.Commit(t.Context(), tacklr.CommitIn{Round: &tacklr.Round{Reply: done}}, out); err != nil {
					t.Fatal(err)
				}
				prompt(t, h, "next")
			}

			// Act
			err := h.Commit(t.Context(), tacklr.CommitIn{Collapse: true}, out)

			// Assert
			if got := contents(checkpoint(t, h).window); !errors.Is(err, tc.fail) || !slices.Equal(got, tc.want) {
				t.Fatalf("collapse = %v with window %q, want %v and %q", err, got, tc.fail, tc.want)
			}
		})
	}
}
