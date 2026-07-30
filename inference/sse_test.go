package inference

import (
	"context"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func collectSSE(t *testing.T, body string) []tacklr.LLMResponseChunk {
	t.Helper()
	s := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 64)
	go func() {
		s.parseSSEResponse(context.Background(), strings.NewReader(body), ch)
		close(ch)
	}()
	var out []tacklr.LLMResponseChunk
	for c := range ch {
		out = append(out, c)
	}
	return out
}

func TestParseSSE_outputTextAlwaysMessage_likeMain(t *testing.T) {
	// DeepSeek/Foundry thinking often arrives as output_text on a reasoning or
	// message item. Main always classifies output_text as StreamEventMessage so
	// ACP agent_message_chunk carries the stream (client demuxes <think> etc.).
	body := strings.Join([]string{
		`data: {"type":"response.output_item.added","item":{"id":"rs_1","type":"reasoning"}}`,
		`data: {"type":"response.output_text.delta","item_id":"rs_1","delta":"<think>internal"}`,
		`data: {"type":"response.output_text.delta","item_id":"rs_1","delta":"</think>"}`,
		`data: {"type":"response.output_item.done","item":{"id":"rs_1","type":"reasoning"}}`,
		`data: {"type":"response.output_item.added","item":{"id":"msg_1","type":"message"}}`,
		`data: {"type":"response.output_text.delta","item_id":"msg_1","delta":"Hello"}`,
		`data: {"type":"response.output_item.done","item":{"id":"msg_1","type":"message"}}`,
		`data: [DONE]`,
		"",
	}, "\n")

	chunks := collectSSE(t, body)
	var messages []string
	for _, c := range chunks {
		if c.IsComplete || c.Content == "" {
			continue
		}
		if c.Type != tacklr.StreamEventMessage {
			t.Fatalf("output_text must be StreamEventMessage (main behavior), got type=%v content=%q", c.Type, c.Content)
		}
		messages = append(messages, c.Content)
	}
	if got := strings.Join(messages, ""); got != "<think>internal</think>Hello" {
		t.Fatalf("messages = %q", got)
	}
}

func TestParseSSE_reasoningTextIsReasoning(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.reasoning_text.delta","item_id":"rs_3","delta":"raw cot"}`,
		`data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_3","delta":" summary"}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	var parts []string
	for _, c := range chunks {
		if c.Type != tacklr.StreamEventReasoning {
			t.Fatalf("expected reasoning, got %+v", c)
		}
		parts = append(parts, c.Content)
	}
	if got := strings.Join(parts, ""); got != "raw cot summary" {
		t.Fatalf("reasoning = %q", got)
	}
}

func TestParseSSE_functionCall_llamaShape_normalizesIDs(t *testing.T) {
	// llama.cpp: call_id only, no id field (matches live gemma server).
	body := strings.Join([]string{
		`data: {"type":"response.output_item.added","item":{"arguments":"","call_id":"fc_abc","name":"echo","type":"function_call","status":"in_progress"}}`,
		`data: {"type":"response.function_call_arguments.delta","delta":"{}","item_id":"fc_abc"}`,
		`data: {"type":"response.output_item.done","item":{"type":"function_call","status":"completed","arguments":"{\"message\":\"hi\"}","call_id":"fc_abc","name":"echo"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	var fc *tacklr.LLMResponseChunk
	for i := range chunks {
		if chunks[i].Type == tacklr.StreamEventFunctionCall {
			fc = &chunks[i]
			break
		}
	}
	if fc == nil {
		t.Fatal("expected function_call chunk")
	}
	if !fc.IsComplete {
		t.Error("expected IsComplete for status=completed")
	}
	if len(fc.ToolCalls) != 1 {
		t.Fatalf("tool calls = %d", len(fc.ToolCalls))
	}
	tc := fc.ToolCalls[0]
	if tc.ID != "fc_abc" {
		t.Errorf("ID = %q, want fc_abc (normalized from call_id)", tc.ID)
	}
	if tc.CallID != "fc_abc" {
		t.Errorf("CallID = %q, want fc_abc", tc.CallID)
	}
	if tc.Name != "echo" {
		t.Errorf("Name = %q", tc.Name)
	}
	if tc.Arguments != `{"message":"hi"}` {
		t.Errorf("Arguments = %q", tc.Arguments)
	}
}

func TestParseSSE_functionCall_incompleteNotComplete(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.done","item":{"type":"function_call","status":"incomplete","arguments":"{","call_id":"fc_x","name":"echo"}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	chunks := collectSSE(t, body)
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(chunks))
	}
	if chunks[0].IsComplete {
		t.Error("incomplete status must not set IsComplete")
	}
	if chunks[0].ToolCalls[0].ID != "fc_x" {
		t.Errorf("ID = %q, want normalized fc_x", chunks[0].ToolCalls[0].ID)
	}
}
