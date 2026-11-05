package tacklr_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/openai"
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

// TestCommit_collapsesAWindowOverPressureIntoASavedSummary: a round that
// takes the window past its pressure collapses everything after the goal
// and the plan into one summary. With a Brain the collapsed messages are
// this session's saved messages: search finds them, open reads one
// exactly, and the summary says so. A count, the summary request or the
// save (its context ends as it embeds) that fails stops the collapse and
// keeps the window.
func TestCommit_collapsesAWindowOverPressureIntoASavedSummary(t *testing.T) {
	t.Parallel()
	const token = "unique-collapsed-token"
	summary := []tacklr.LLMResponseChunk{text("summary")}
	// Each session counts its goal, its two large messages, then the
	// follow-up; the summary's response is counted next.
	cases := []struct {
		name        string
		count       func([]*message.Message) (int, error)
		reply       []tacklr.LLMResponseChunk
		brain, ends bool
		fail        string
	}{
		{name: "saved for search", count: windowTokens, reply: summary, brain: true},
		{name: "the first count fails", count: failAfter(3), reply: summary, fail: "count tokens"},
		{name: "the count after the summary fails", count: failAfter(5), reply: summary, fail: "count tokens"},
		{name: "the summary request fails", count: windowTokens, reply: []tacklr.LLMResponseChunk{failure(fmt.Errorf("%w: stream failed", tacklr.ErrNetwork))}, fail: "context compress stream failed"},
		{name: "the save fails", count: windowTokens, reply: summary, brain: true, ends: true, fail: "save session messages"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			model, _ := counted(t, tc.count, tc.reply)
			opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 1000}
			if tc.brain {
				v := vectors{}
				if tc.ends {
					v.cancel = cancel
				}
				opts.Brain, opts.SearchNamespace = embedded(t, v), storetest.Namespace(t, "org", "acme")
			}
			h := planned(t, "collapse", opts, tacklr.Todo{Title: "t"})
			prompt(t, h, strings.Repeat("a", 350))
			prompt(t, h, strings.Repeat("b", 350)+" "+token)
			before := contents(checkpoint(t, h).window)

			err := h.Commit(ctx, tacklr.CommitIn{Users: []*message.Message{userMsg(strings.Repeat("f", 100))}}, newStream(t).out)

			window := checkpoint(t, h).window
			if tc.fail != "" {
				if err == nil || !strings.Contains(err.Error(), tc.fail) || !slices.Equal(contents(window), before) {
					t.Fatalf("commit = %v with window %d messages, want %q and the window kept", err, len(window), tc.fail)
				}
				return
			}
			if err != nil || len(window) != 4 || !tacklr.IsPlanDocument(window[1]) || !strings.Contains(window[2].Content, "Use the search tool") || window[3].Content != strings.Repeat("f", 100) {
				t.Fatalf("window = %q %v, want the goal, the plan, a summary that points at search, and the round", contents(window), err)
			}
			var found brain.Page
			if err := json.Unmarshal([]byte(result(t, h, call("s1", "search", `{"query":"`+token+`"}`))), &found); err != nil {
				t.Fatal(err)
			}
			hits := slices.Collect(found.Results.Objects())
			if len(hits) == 0 || hits[0].Kind != brain.KindSessionMessage {
				t.Fatalf("search the collapsed history = %+v, want its saved message first", hits)
			}
			if opened := result(t, h, call("o1", "open", `{"ref":"`+hits[0].ID.String()+`"}`)); !strings.Contains(opened, token) {
				t.Fatalf("open the saved message = %s, want it exactly", opened)
			}
		})
	}
}

// TestCompleteTodo_handsOffTheRestOfThePlan: completing a todo while
// others are open rebuilds the window as the goal, the plan, the model's
// handoff and a nudge to continue; the messages it replaces are this
// session's saved messages, which search finds. Completing the last open
// todo keeps the window. A wrong title is the model's to correct.
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
	noPlan := result(t, h, call("c0", "complete_todo", `{"title":"A"}`))
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
	wrong := []string{
		result(t, h, call("c1", "complete_todo", `{"title":"Z"}`)),
		result(t, h, call("c2", "complete_todo", `{"title":"B"}`)),
	}

	first, firstErr := runRound(t, h, call("c3", "complete_todo", `{"title":"A"}`))
	handedOff := checkpoint(t, h).window
	last, lastErr := runRound(t, h, call("c4", "complete_todo", `{"title":"C"}`))

	if !strings.Contains(noPlan, "no plan exists") || !strings.Contains(wrong[0], `todo "Z" not found`) || !strings.Contains(wrong[1], `todo "B" is already completed`) {
		t.Fatalf("complete with no plan %q, then %q; want each named for the model", noPlan, wrong)
	}
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

// TestHandoff_fallsBackToThePlanWhenTheModelGivesNoHandoff: a request the
// model cannot send, an empty stream and a stream error each install a
// handoff written from the plan.
func TestHandoff_fallsBackToThePlanWhenTheModelGivesNoHandoff(t *testing.T) {
	t.Parallel()
	for name, reply := range map[string][]tacklr.LLMResponseChunk{
		"a request that cannot be sent": nil,
		"an empty stream":               {},
		"a stream error":                {failure(fmt.Errorf("%w: handoff stream failed", tacklr.ErrNetwork))},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			model, _ := scripted(t, reply)
			opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
			h := planned(t, "fallback", opts, tacklr.Todo{Title: "First"}, tacklr.Todo{Title: "Ship", Description: "finish"})
			if reply == nil {
				rows, _ := h.Checkpoint()
				opts.Model = openai.New(http.DefaultClient).WithModel("gpt-4o")
				h = newHarness(t, "fallback", opts)
				if err := h.RestoreCheckpoint(rows); err != nil {
					t.Fatal(err)
				}
			}

			_, err := runRound(t, h, call("c1", "complete_todo", `{"title":"First"}`))

			window := checkpoint(t, h).window
			if err != nil || len(window) != 4 || !strings.Contains(window[2].Content, "[in_progress] Ship: finish") {
				t.Fatalf("handoff = %v, window %q, want the goal, the plan, the fallback handoff and the nudge", err, contents(window))
			}
		})
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
