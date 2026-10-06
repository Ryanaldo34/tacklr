package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestSystemPrompt_knowledgeGuidanceWhenBrainSet(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &scriptedModel{},
		Brain:  eng,
	})
	t.Cleanup(h.Close)
	prompt := h.constructSystemPrompt()
	if !strings.Contains(prompt, "knowledge store") || !strings.Contains(prompt, "earlier messages from this session") {
		t.Fatalf("want knowledge-store guidance when Brain is set:\n%s", prompt)
	}
}

func TestHandoff_savesSessionMessagesForSearch(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "acme")
	const (
		sessionID = "sess-handoff"
		token     = "unique-retain-handoff-token"
		callID    = "call-session-1"
	)
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:       sessionID,
		Config:          Config{MaxWindowSize: 8192},
		SearchNamespace: ns,
		Brain:           eng,
		Model:           &scriptedModel{},
	})
	t.Cleanup(h.Close)
	if messageSearchText(nil) != "" {
		t.Fatal("nil message")
	}
	text := messageSearchText(&Message{
		Role:    RoleAssistant,
		Content: "body",
		ContentParts: []ContentPart{
			{Type: ContentTypeOutputText, Text: "out"},
			{Type: ContentTypeInputText, Text: ""},
			{Type: ContentTypeInputImage},
		},
		ToolCalls: []ToolCall{
			{Name: "search", Arguments: `{"q":"1"}`},
			{},
		},
	})
	for _, want := range []string{"assistant", "body", "out", "search", `{"q":"1"}`} {
		if !strings.Contains(text, want) {
			t.Fatalf("search text %q missing %q", text, want)
		}
	}
	if err := h.applyBatchToolResultEffect(t.Context(), EffectHandoff); err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("empty window = %v", err)
	}
	h.context.Restore([]*Message{
		{Role: RoleUser, Content: "do the work"},
		{Role: RoleTool, ToolCallID: callID, Content: token},
	})
	h.session.Plan.SetDocument("plan body")
	h.session.Plan.Set([]Todo{
		{Title: "A", Status: TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: TodoStatusPending, Description: "next"},
	})

	if err := h.applyBatchToolResultEffect(t.Context(), EffectHandoff); err != nil {
		t.Fatal(err)
	}

	page := mustSearch(t, eng, brain.Scope{Namespace: ns, SessionID: sessionID}, token)
	if len(page.Objects) != 1 || page.Objects[0].Kind != brain.KindSessionMessage {
		t.Fatalf("search = %+v", page.Objects)
	}
	got, err := eng.Read(t.Context(), brain.Scope{Namespace: ns, SessionID: sessionID}, page.Objects[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Content, callID) || !strings.Contains(got.Content, token) {
		t.Fatalf("session message = %s", got.Content)
	}
}

func TestAbsorb_retainsCompressedEpisode(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(t.Context(), EpisodeKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "acme")
	const token = "unique-retain-compress-token"
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:       "sess-compress",
		Config:          Config{MaxWindowSize: 40},
		ContextPolicy:   ContextPolicy{PressureRatio: 0.5, CompressFraction: 0.5},
		SearchNamespace: ns,
		Brain:           eng,
		Model: &scriptedModel{
			InvokeFn: func(_ context.Context, _ []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				if tools == nil {
					ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "summary of " + token, IsComplete: true}
					return
				}
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "turn", IsComplete: true}
			},
		},
	})
	t.Cleanup(h.Close)
	h.context.Restore([]*Message{
		{Role: RoleUser, Content: "goal"},
		{Role: RoleAssistant, Content: strings.Repeat("a", 30) + " " + token},
	})
	if err := h.addToContext(t.Context(), &Message{Role: RoleUser, Content: "follow-up"}, nil); err != nil {
		t.Fatal(err)
	}
	mustSearchKind(t, eng, ns, token, KindEpisode)
}

func TestHandoff_sessionMessageSaveErrorKeepsWindow(t *testing.T) {
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(failSessionSave{MemoryStore: store}, brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:       "sess-save-fail",
		Config:          Config{MaxWindowSize: 8192},
		SearchNamespace: mustNS(t, "org", "acme"),
		Brain:           eng,
		Model:           &scriptedModel{},
	})
	t.Cleanup(h.Close)
	h.context.Restore([]*Message{
		{Role: RoleUser, Content: "do the work"},
		{Role: RoleAssistant, Content: "kept-detail"},
	})
	h.session.Plan.SetDocument("plan body")
	h.session.Plan.Set([]Todo{
		{Title: "A", Status: TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: TodoStatusPending, Description: "next"},
	})
	err = h.applyBatchToolResultEffect(t.Context(), EffectHandoff)
	if err == nil || !strings.Contains(err.Error(), "session messages down") {
		t.Fatalf("err = %v", err)
	}
	window := h.context.Messages()
	if len(window) != 2 || window[1].Content != "kept-detail" {
		t.Fatalf("window = %+v", window)
	}
}

func TestHandoff_nilBrainReplacesWindow(t *testing.T) {
	h := mustNewTurnManager(t, AgentOptions{
		SessionID: "sess-no-brain",
		Config:    Config{MaxWindowSize: 8192},
		Model:     &scriptedModel{},
	})
	t.Cleanup(h.Close)
	h.context.Restore([]*Message{{Role: RoleUser, Content: "do the work"}})
	h.session.Plan.SetDocument("plan body")
	h.session.Plan.Set([]Todo{
		{Title: "A", Status: TodoStatusCompleted, Description: "done"},
		{Title: "B", Status: TodoStatusPending, Description: "next"},
	})
	if err := h.applyBatchToolResultEffect(t.Context(), EffectHandoff); err != nil {
		t.Fatal(err)
	}
	window := h.context.Messages()
	if len(window) < 3 || window[0].Content != "do the work" {
		t.Fatalf("window = %+v", window)
	}
}

type failSessionSave struct {
	*brain.MemoryStore
}

func (failSessionSave) SaveSessionMessages(context.Context, brain.Namespace, string, string, []brain.SessionMessage) (int, error) {
	return 0, errors.New("session messages down")
}

func TestSpecialist_retainsResultEpisode(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(t.Context(), EpisodeKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := mustNS(t, "org", "acme")
	const token = "unique-retain-specialist-token"
	h := mustNewTurnManager(t, AgentOptions{
		SessionID:       "sess-spec",
		Config:          Config{MaxWindowSize: 8192},
		SearchNamespace: ns,
		Brain:           eng,
		Model:           &scriptedModel{},
		Specialists: []*Specialist{
			{Name: "researcher", Model: &scriptedModel{
				InvokeFn: func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
					ch <- LLMResponseChunk{Type: StreamEventMessage, Content: token, IsComplete: true}
				},
			}},
		},
	})
	t.Cleanup(h.Close)
	h.BindJobHost(stubJobHost{result: token})
	activatePlan(t, h)
	tool := h.findTool("spawn_specialist", "")
	if tool == nil {
		t.Fatal("spawn_specialist missing")
	}
	out, err := runWriteTool(t, h, tool, `{"specialist":"researcher","task_description_and_context":"look it up"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, token) {
		t.Fatalf("spawn output = %q", out)
	}
	mustSearchKind(t, eng, ns, token, KindEpisode)
}

func mustSearch(t *testing.T, eng *brain.Engine, scope brain.Scope, query string) brain.SearchPage {
	t.Helper()
	page, err := eng.Search(t.Context(), scope, brain.SearchRequest{Query: query}, brain.NewSearchContext())
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func mustSearchKind(t *testing.T, eng *brain.Engine, ns brain.Namespace, query, kind string) brain.RichObject {
	t.Helper()
	page := mustSearch(t, eng, brain.Scope{Namespace: ns}, query)
	if len(page.Objects) == 0 {
		t.Fatalf("search %q: no hits", query)
	}
	if page.Objects[0].Kind != kind {
		t.Fatalf("kind = %q, want %s", page.Objects[0].Kind, kind)
	}
	return page.Objects[0]
}

type stubJobHost struct {
	result string
}

func (s stubJobHost) RunSpecialist(context.Context, string, string, string) (string, error) {
	return s.result, nil
}
func (stubJobHost) Schedule(context.Context, JobRequest, string) (Job, error) {
	return Job{}, nil
}
func (stubJobHost) Jobs() []Job                             { return nil }
func (stubJobHost) CancelJob(context.Context, string) error { return nil }

func toolCall(id, name, args string) ToolCall {
	return ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

func TestTagModelAfterToolsError_wrapsProviderFailures(t *testing.T) {
	base := errors.New("upstream failed")
	wrapped := fmt.Errorf("%w: already", ErrModelAfterTools)

	fromError := tagModelAfterToolsError(LLMResponseChunk{Error: base})
	fromWrapped := tagModelAfterToolsError(LLMResponseChunk{Error: wrapped})
	fromContent := tagModelAfterToolsError(LLMResponseChunk{Content: "provider said no"})

	if !errors.Is(fromError.Error, ErrModelAfterTools) || fromError.Content == "" {
		t.Fatalf("from error = %+v", fromError)
	}
	if !errors.Is(fromWrapped.Error, ErrModelAfterTools) || strings.Count(fromWrapped.Error.Error(), "model request failed") != 1 {
		t.Fatalf("double wrap = %v", fromWrapped.Error)
	}
	if !errors.Is(fromContent.Error, ErrModelAfterTools) || !strings.Contains(fromContent.Content, "provider said no") {
		t.Fatalf("from content = %+v", fromContent)
	}
}
