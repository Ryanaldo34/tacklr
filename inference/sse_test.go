package inference

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func collectSSE(t *testing.T, body string) []tacklr.LLMResponseChunk {
	t.Helper()
	s := NewOpenAIInferenceStrategy(nil)
	ch := make(chan tacklr.LLMResponseChunk, 64)
	go func() {
		s.parseSSEResponse(strings.NewReader(body), ch)
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
