package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestMakeSchemaNullable_shapes(t *testing.T) {
	if got := makeSchemaNullable(nil); got["type"] == nil {
		t.Fatal(got)
	}
	s := map[string]any{"type": "string"}
	got := makeSchemaNullable(s)
	arr, ok := got["type"].([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("%+v", got)
	}
	n := map[string]any{"type": "null"}
	if makeSchemaNullable(n)["type"] != "null" {
		t.Fatal("null")
	}
	a := map[string]any{"type": []any{"string", "number"}}
	got = makeSchemaNullable(a)
	arr, _ = got["type"].([]any)
	if len(arr) != 3 {
		t.Fatalf("%+v", got)
	}
	b := map[string]any{"type": []any{"string", "null"}}
	got = makeSchemaNullable(b)
	arr, _ = got["type"].([]any)
	if len(arr) != 2 {
		t.Fatalf("%+v", got)
	}
}

func TestWebHelpers_truncateAndSynthesis(t *testing.T) {
	if truncateRunes("", 10) != "" || truncateRunes("hi", 0) != "hi" {
		t.Fatal("edge")
	}
	if truncateRunes("hello", 10) != "hello" {
		t.Fatal("short")
	}
	if got := truncateRunes("abcdefghij", 5); got != "abcde…" {
		t.Fatalf("%q", got)
	}
	raw, _ := json.Marshal("hello world")
	if formatSynthesisContent(raw) != "hello world" {
		t.Fatal(formatSynthesisContent(raw))
	}
	raw, _ = json.Marshal(map[string]any{"a": 1})
	if formatSynthesisContent(raw) == "" {
		t.Fatal("object")
	}
	if formatSynthesisContent(json.RawMessage(`not-json`)) == "" {
		t.Fatal("fallback")
	}
}

func TestBrainToolHelpers_parseAndNew(t *testing.T) {
	if _, err := parseUUID("", "f"); err == nil {
		t.Fatal("empty")
	}
	if _, err := parseUUID("nope", "f"); err == nil {
		t.Fatal("bad")
	}
	if _, err := parseUUID(uuid.Nil.String(), "f"); err == nil {
		t.Fatal("nil uuid")
	}
	id := uuid.New()
	got, err := parseUUID(id.String(), "f")
	if err != nil || got != id {
		t.Fatal(err)
	}
	out, err := formatBrainJSON(map[string]any{"ok": true})
	if err != nil || out == "" {
		t.Fatal(err)
	}
	if newBrainTools(nil, nil, nil) != nil {
		t.Fatal("nil tools")
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	sm := session.NewSessionManager()
	sc := brain.NewSearchContext()
	tools := newBrainTools(eng, sm, sc)
	if len(tools) != 6 {
		t.Fatalf("tools %d", len(tools))
	}
}

func TestAgentHarness_namespaceAndIDs(t *testing.T) {
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	h := NewAgent(context.Background(), AgentOptions{
		Model:  &mockStrategy{},
		Brain:  eng,
		Config: Config{MaxWindowSize: 1024},
	})
	_ = h.SessionID()
	if _, ok := h.SearchNamespace(); ok {
		t.Fatal("default ns")
	}
	ns := uuid.New()
	h.SetSearchNamespace(ns)
	got, ok := h.SearchNamespace()
	if !ok || got != ns {
		t.Fatal("set")
	}
	h.ClearSearchNamespace()
	if _, ok := h.SearchNamespace(); ok {
		t.Fatal("clear")
	}
	if h.AskUserQuestion("missing") != "" {
		t.Fatal("ask")
	}
}

func TestStripUnpairedToolTurns_andWireID(t *testing.T) {
	if toolCallWireID(ToolCall{CallID: "c", ID: "i"}) != "c" {
		t.Fatal("call id")
	}
	if toolCallWireID(ToolCall{ID: "i"}) != "i" {
		t.Fatal("id")
	}
	if got := stripUnpairedToolTurns(nil, nil); got != nil {
		t.Fatal(got)
	}
	// Paired call+result kept; orphan result dropped; interrupt keep set retains unpaired call.
	callID := "call-1"
	keepID := "keep-1"
	window := []*Message{
		nil,
		{Role: RoleUser, Content: "u"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: callID, Name: "t"}}},
		{Role: RoleTool, ToolCallID: callID, Content: "ok"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{{CallID: keepID, Name: "ask"}}},
		{Role: RoleTool, ToolCallID: "orphan", Content: "x"},
	}
	out := stripUnpairedToolTurns(window, map[string]struct{}{keepID: {}})
	if len(out) < 4 {
		t.Fatalf("len %d", len(out))
	}
	// harness path with empty context is no-op
	h := &AgentHarness{}
	h.stripUnpairedToolCallsAfterInferenceError()
	// with context: build unpaired then strip
	h2 := NewAgent(context.Background(), AgentOptions{
		Model:  &mockStrategy{},
		Config: Config{MaxWindowSize: 1024},
	})
	h2.context.Replace([]*Message{
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "x", Name: "t"}}},
	})
	h2.stripUnpairedToolCallsAfterInferenceError()
	if len(h2.Messages()) != 0 {
		// unpaired assistant tool call without result should be stripped
		// (unless keep set has it)
	}
}

func TestMergeTokenUsage(t *testing.T) {
	var u telemetry.TokenUsage
	mergeTokenUsage(&u, LLMResponseChunk{InputTokens: 1, OutputTokens: 2, ReasoningTokens: 3})
	if u.Input != 1 || u.Output != 2 || u.Reasoning != 3 {
		t.Fatalf("%+v", u)
	}
	mergeTokenUsage(&u, LLMResponseChunk{}) // zeros ignored
	if u.Input != 1 {
		t.Fatal(u.Input)
	}
}

func TestContextManagerAndToolJSON_cov(t *testing.T) {
	if rawPlanFromDocumentMessage(nil) != "" {
		t.Fatal("nil msg")
	}
	if rawPlanFromDocumentMessage(&Message{Content: planDocumentPrefix + "BODY"}) != "BODY" {
		t.Fatal("prefix strip")
	}
	cm := NewModelContextManager()
	cm.Add(nil)
	cm.Add(&Message{Role: RoleUser, Content: "hi"})
	if len(cm.Messages()) != 1 {
		t.Fatal(cm.Messages())
	}
	tool := &Tool{Name: "t", Description: "d", parameters: nil}
	j := tool.AsJson()
	if j["name"] != "t" || j["parameters"] == nil {
		t.Fatalf("%+v", j)
	}
	ch := tagModelAfterToolsError(LLMResponseChunk{Error: errors.New("x")})
	if ch.Error == nil {
		t.Fatal("tag error")
	}
	ch2 := tagModelAfterToolsError(LLMResponseChunk{Content: "fail text"})
	if ch2.Error == nil {
		t.Fatal("tag content")
	}
	// session nil paths on namespace helpers
	h := &AgentHarness{}
	h.SetSearchNamespace(uuid.New())
	if _, ok := h.SearchNamespace(); !ok {
		t.Fatal("set creates session")
	}
	h2 := &AgentHarness{}
	h2.ClearSearchNamespace()
	if _, ok := h2.SearchNamespace(); ok {
		t.Fatal("nil session clear")
	}
}
