package tacklr

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewTurnManager_rejectsInvalidSpecialists(t *testing.T) {
	ok := &scriptedModel{}
	cases := []struct {
		name  string
		specs []*Specialist
	}{
		{"nil spec", []*Specialist{nil}},
		{"empty name", []*Specialist{{Name: "", Model: ok}}},
		{"nil model", []*Specialist{{Name: "w", Model: nil}}},
		{"duplicate name", []*Specialist{
			{Name: "w", Model: ok},
			{Name: "w", Model: ok},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTurnManager(context.Background(), AgentOptions{
				MaxWindowSize: 8192, Model: ok, Specialists: tc.specs,
			}); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}

func TestSystemPrompt_listsSpecialistsSorted(t *testing.T) {
	var n int
	h := mustNewTurnManager(t, AgentOptions{
		MaxWindowSize: 8192,
		Model: &scriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				n++
				if n == 1 {
					ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						toolCall("sp1", "spawn_specialist", `{"specialist":"alpha","task_description_and_context":"x"}`),
						toolCall("ls1", "list_children", `{}`),
						toolCall("cc1", "cancel_child", `{"child_id":"missing"}`),
					}, IsComplete: true}
					return
				}
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		},
		Specialists: []*Specialist{
			{Name: "zebra", Model: &scriptedModel{}, Description: "last"},
			{Name: "alpha", Model: &scriptedModel{}},
		},
	})
	t.Cleanup(h.Close)
	prompt := h.constructSystemPrompt()
	ai := strings.Index(prompt, " - alpha\n")
	zi := strings.Index(prompt, " - zebra: last\n")
	if ai < 0 || zi < 0 || ai > zi {
		t.Fatalf("want alpha then zebra in prompt: %s", prompt)
	}
	rt := turnRuntime(h)
	res, err := h.findTool("list_children", "").invoke(t.Context(), `{}`, rt)
	if err != nil || !strings.Contains(strings.ToLower(res.Output), "no jobs") {
		t.Fatalf("list_children: %q %v", res.Output, err)
	}
}

// TestJobTools_correctBadArguments: spawn_specialist and cancel_child name
// a missing or unknown argument for the model to correct.
func TestJobTools_correctBadArguments(t *testing.T) {
	h := mustNewTurnManager(t, AgentOptions{MaxWindowSize: 1024, Model: &scriptedModel{},
		Specialists: []*Specialist{{Name: "researcher", Model: &scriptedModel{}}}})
	t.Cleanup(h.Close)
	call := func(name, args string) error {
		_, err := h.findTool(name, "").invoke(t.Context(), args, turnRuntime(h))
		return err
	}

	for name, tc := range map[string]struct {
		err        error
		correction bool
		want       string
	}{
		"spawn with no task":       {call("spawn_specialist", `{"specialist":"researcher","task_description_and_context":" "}`), true, "task_description_and_context is required"},
		"spawn with no specialist": {call("spawn_specialist", `{"specialist":"","task_description_and_context":"x"}`), true, "specialist is required"},
		"cancel with no id":        {call("cancel_child", `{"child_id":""}`), true, "child_id is required"},
		"spawn of an unknown name": {call("spawn_specialist", `{"specialist":"nobody","task_description_and_context":"x"}`), true, "that specialist is not registered"},
		"cancel of an unknown job": {call("cancel_child", `{"child_id":"job-1"}`), true, "that job is unknown"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) || errors.Is(tc.err, ErrCorrection) != tc.correction {
			t.Errorf("%s = %v, want %q (correction %v)", name, tc.err, tc.want, tc.correction)
		}
	}
}
