package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestHandoff_savesSessionMessagesForSearch(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "acme")
	const (
		sessionID = "sess-handoff"
		token     = "unique-retain-handoff-token"
		callID    = "call-session-1"
	)
	h := mustNewHarness(t, AgentOptions{
		sessionID:       sessionID,
		MaxWindowSize:   8192,
		SearchNamespace: ns,
		Brain:           eng,
		Model:           &scriptedModel{},
	})
	text := messageSearchText(&message.Message{
		Role:    message.RoleAssistant,
		Content: "body",
		ContentParts: []message.ContentPart{
			{Type: message.ContentTypeOutputText, Text: "out"},
			{Type: message.ContentTypeInputText, Text: ""},
			{Type: message.ContentTypeInputImage},
		},
		ToolCalls: []message.ToolCall{
			{Name: "search", Arguments: `{"q":"1"}`},
			{},
		},
	})
	for _, want := range []string{"assistant", "body", "out", "search", `{"q":"1"}`} {
		if !strings.Contains(text, want) {
			t.Fatalf("search text %q missing %q", text, want)
		}
	}
	if err := h.applyEffect(t.Context(), EffectHandoff); err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("empty window = %v", err)
	}
	h.context.Restore([]*message.Message{
		{Role: message.RoleUser, Content: "do the work"},
		{Role: message.RoleTool, ToolCallID: callID, Content: token},
	})
	h.session.Plan.setDocument("plan body")
	h.session.Plan.set([]Todo{
		{Title: "A", Status: TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: TodoStatusPending, Description: "next"},
	})

	if err := h.applyEffect(t.Context(), EffectHandoff); err != nil {
		t.Fatal(err)
	}

	page := mustSearch(t, eng, brain.Scope{Namespace: ns, SessionID: sessionID}, token)
	if len(slices.Collect(page.Results.Objects())) != 1 || slices.Collect(page.Results.Objects())[0].Kind != brain.KindSessionMessage {
		t.Fatalf("search = %+v", slices.Collect(page.Results.Objects()))
	}
	got, err := eng.Get(t.Context(), brain.Scope{Namespace: ns, SessionID: sessionID}, slices.Collect(page.Results.Objects())[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Content, callID) || !strings.Contains(got.Content, token) {
		t.Fatalf("session message = %s", got.Content)
	}
}

// TestCommitRound_aCollapseSavesTheWindowForSearch: the messages a collapse
// takes out of the window are this session's saved messages: the search
// tool finds them and open reads one exactly. The summary says so. A save
// that fails stops the collapse.
func TestCommitRound_aCollapseSavesTheWindowForSearch(t *testing.T) {
	const token = "unique-collapsed-token"
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: vectorEmbedder{}}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	// The first commit's context is cancelled once it counts tokens, so the
	// save (the store embeds each message) fails.
	saving, cancel := context.WithCancel(t.Context())
	h := mustNewHarness(t, AgentOptions{
		sessionID:       "sess-collapse",
		MaxWindowSize:   270,
		SearchNamespace: storetest.Namespace(t, "org", "acme"),
		Brain:           eng,
		Model: &scriptedModel{
			CountTokensFn: func(ctx context.Context, msgs []*message.Message, tools []*Tool) (int, error) {
				cancel()
				return contentTokens(ctx, msgs, tools)
			},
			InvokeFn: func(_ context.Context, _ []*message.Message, _ []*Tool, ch chan<- LLMResponseChunk) {
				ch <- LLMResponseChunk{Type: session.EventMessage, Content: "summary", IsComplete: true}
			},
		},
	})
	collapsed := &message.Message{Role: message.RoleAssistant, Content: strings.Repeat("a", 300) + " " + token}
	h.context.Restore([]*message.Message{{Role: message.RoleUser, Content: "goal"}, buildPlanDocumentMessage("ship"), collapsed})
	call := func(name, args string) string {
		res, err := h.findTool(name, "").invoke(t.Context(), args, turnRuntime(h))
		if err != nil {
			t.Fatal(err)
		}
		return res.Output
	}

	saveErr := h.commitRound(saving, []*message.Message{{Role: message.RoleUser, Content: "follow-up"}}, EffectNone)
	kept := len(h.context.Messages())
	commitErr := h.commitRound(t.Context(), []*message.Message{{Role: message.RoleUser, Content: "follow-up"}}, EffectNone)
	var found brain.Page
	if err := json.Unmarshal([]byte(call("search", `{"query":"`+token+`"}`)), &found); err != nil {
		t.Fatal(err)
	}
	hits := slices.Collect(found.Results.Objects())

	if !errors.Is(saveErr, context.Canceled) || kept != 3 {
		t.Fatalf("collapse with a failing save = %v, window %d, want the save's error and the window kept", saveErr, kept)
	}
	window := h.context.Messages()
	if commitErr != nil || len(window) != 4 || !IsPlanDocument(window[1]) || !strings.Contains(window[2].Content, searchableHistoryNote) || window[3].Content != "follow-up" {
		t.Fatalf("window = %+v %v, want the goal, the plan, a summary that points at search, and the round", window, commitErr)
	}
	if len(hits) != 1 || hits[0].Kind != brain.KindSessionMessage {
		t.Fatalf("search the collapsed history = %+v, want its saved message", hits)
	}
	if opened := call("open", `{"ref":"`+hits[0].ID.String()+`"}`); !strings.Contains(opened, token) {
		t.Fatalf("open the saved message = %s, want it exactly", opened)
	}
}

func TestHandoff_sessionMessageSaveErrorKeepsWindow(t *testing.T) {
	// The store embeds the window, so a cancelled handoff fails its save.
	store := brain.NewMemoryStore(brain.WithMemoryEmbedding(brain.Embedding{Embedder: vectorEmbedder{}}))
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewHarness(t, AgentOptions{
		sessionID:       "sess-save-fail",
		MaxWindowSize:   8192,
		SearchNamespace: storetest.Namespace(t, "org", "acme"),
		Brain:           eng,
		Model:           &scriptedModel{},
	})
	h.context.Restore([]*message.Message{
		{Role: message.RoleUser, Content: "do the work"},
		{Role: message.RoleAssistant, Content: "kept-detail"},
	})
	h.session.Plan.setDocument("plan body")
	h.session.Plan.set([]Todo{
		{Title: "A", Status: TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: TodoStatusPending, Description: "next"},
	})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	err = h.applyEffect(cancelled, EffectHandoff)

	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "save session messages") {
		t.Fatalf("handoff with a failing save = %v, want the save's error", err)
	}
	window := h.context.Messages()
	if len(window) != 2 || window[1].Content != "kept-detail" {
		t.Fatalf("window = %+v", window)
	}
}

// vectorEmbedder embeds every text as one fixed vector.
type vectorEmbedder struct{}

func (vectorEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0}
	}
	return out, nil
}

func (vectorEmbedder) Model() string { return "vector-model" }

func mustSearch(t *testing.T, eng *brain.Engine, scope brain.Scope, query string) brain.Page {
	t.Helper()
	page, err := eng.Search(t.Context(), scope, brain.SearchRequest{Query: query})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func toolCall(id, name, args string) message.ToolCall {
	return message.ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}
