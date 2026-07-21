package streaming

import (
	"testing"
)

func collect(out chan StreamEvent) []StreamEvent {
	close(out)
	var events []StreamEvent
	for ev := range out {
		events = append(events, ev)
	}
	return events
}

func TestNew_returnsFreshBufferedStreamer(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}

	out := make(chan StreamEvent, 10)
	err := s.Stream(LLMResponseChunk{
		Type:       StreamEventMessage,
		MessageId:  "x",
		Content:    "hi",
		IsComplete: true,
	}, out)
	if err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}

	events := collect(out)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Content != "hi" {
		t.Errorf("expected content %q, got %q", "hi", events[0].Content)
	}
	if events[0].MessageID != "x" {
		t.Errorf("expected MessageID %q, got %q", "x", events[0].MessageID)
	}

	out2 := make(chan StreamEvent, 10)
	if err := s.Stream(LLMResponseChunk{
		Type:       StreamEventMessage,
		MessageId:  "y",
		Content:    "fresh",
		IsComplete: true,
	}, out2); err != nil {
		t.Fatalf("second Stream returned error: %v", err)
	}
	events2 := collect(out2)
	if len(events2) != 1 {
		t.Fatalf("expected no stale state: 1 event, got %d: %+v", len(events2), events2)
	}
	if events2[0].Content != "fresh" {
		t.Errorf("expected fresh content %q, got %q", "fresh", events2[0].Content)
	}
	if events2[0].MessageID != "y" {
		t.Errorf("expected MessageID %q, got %q", "y", events2[0].MessageID)
	}
}

func TestStream_reasoningPassesThrough(t *testing.T) {
	s := New()
	out := make(chan StreamEvent, 10)
	if err := s.Stream(LLMResponseChunk{
		Type:      StreamEventReasoning,
		Content:   "thinking",
		MessageId: "r1",
	}, out); err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}

	events := collect(out)
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 reasoning event, got %d: %+v", len(events), events)
	}
	ev := events[0]
	if ev.Type != StreamEventReasoning {
		t.Errorf("expected Type %v, got %v", StreamEventReasoning, ev.Type)
	}
	if ev.Content != "thinking" {
		t.Errorf("expected content %q, got %q", "thinking", ev.Content)
	}
	if ev.MessageID != "r1" {
		t.Errorf("expected MessageID %q, got %q", "r1", ev.MessageID)
	}
}

func TestStream_accumulatesContentAndFlushesOnComplete(t *testing.T) {
	s := New()
	chunks := []LLMResponseChunk{
		{Type: StreamEventMessage, MessageId: "m1", Content: "Hello"},
		{Type: StreamEventMessage, MessageId: "m1", Content: " "},
		{Type: StreamEventMessage, MessageId: "m1", Content: "world", IsComplete: true},
	}

	out := make(chan StreamEvent, 10)
	for _, c := range chunks {
		if err := s.Stream(c, out); err != nil {
			t.Fatalf("Stream returned error: %v", err)
		}
		if len(out) != 0 && !c.IsComplete {
			t.Errorf("non-complete chunk produced %d events, expected 0", len(out))
		}
	}

	events := collect(out)
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 flushed event, got %d: %+v", len(events), events)
	}
	ev := events[0]
	if ev.Type != StreamEventMessage {
		t.Errorf("expected Type %v, got %v", StreamEventMessage, ev.Type)
	}
	if ev.Content != "Hello world" {
		t.Errorf("expected content %q, got %q", "Hello world", ev.Content)
	}
	if ev.MessageID != "m1" {
		t.Errorf("expected MessageID %q, got %q", "m1", ev.MessageID)
	}
}

func TestStream_flushesOnMessageIdChange(t *testing.T) {
	s := New()
	out := make(chan StreamEvent, 10)

	if err := s.Stream(LLMResponseChunk{
		Type:      StreamEventMessage,
		MessageId: "m1",
		Content:   "first",
	}, out); err != nil {
		t.Fatalf("first Stream error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("first chunk should not flush, got %d events", len(out))
	}

	if err := s.Stream(LLMResponseChunk{
		Type:       StreamEventMessage,
		MessageId:  "m2",
		Content:    "second",
		IsComplete: true,
	}, out); err != nil {
		t.Fatalf("second Stream error: %v", err)
	}

	events := collect(out)
	if len(events) != 2 {
		t.Fatalf("expected 2 flushes, got %d: %+v", len(events), events)
	}
	if events[0].Type != StreamEventMessage {
		t.Errorf("first flush: expected Type %v, got %v", StreamEventMessage, events[0].Type)
	}
	if events[0].Content != "first" {
		t.Errorf("first flush: expected content %q, got %q", "first", events[0].Content)
	}
	if events[1].Content != "second" {
		t.Errorf("second flush: expected content %q, got %q", "second", events[1].Content)
	}
	if events[1].MessageID != "m2" {
		t.Errorf("second flush: expected MessageID %q, got %q", "m2", events[1].MessageID)
	}
}

func TestStream_accumulatesToolCalls(t *testing.T) {
	s := New()
	chunks := []LLMResponseChunk{
		{
			Type:      StreamEventFunctionCall,
			MessageId: "m1",
			ToolCalls: []ToolCall{{Name: "t1"}},
		},
		{
			Type:       StreamEventFunctionCall,
			MessageId:  "m1",
			ToolCalls:  []ToolCall{{Name: "t2"}},
			IsComplete: true,
		},
	}

	out := make(chan StreamEvent, 10)
	for _, c := range chunks {
		if err := s.Stream(c, out); err != nil {
			t.Fatalf("Stream error: %v", err)
		}
	}

	events := collect(out)
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 function_call event, got %d: %+v", len(events), events)
	}
	ev := events[0]
	if ev.Type != StreamEventFunctionCall {
		t.Errorf("expected Type %v, got %v", StreamEventFunctionCall, ev.Type)
	}
	if len(ev.ToolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %d: %+v", len(ev.ToolCalls), ev.ToolCalls)
	}
	if ev.ToolCalls[0].Name != "t1" || ev.ToolCalls[1].Name != "t2" {
		t.Errorf("unexpected tool call order/names: %+v", ev.ToolCalls)
	}
}

func TestStream_skipsEmptyContentFlush(t *testing.T) {
	s := New()
	out := make(chan StreamEvent, 10)
	if err := s.Stream(LLMResponseChunk{
		Content:    "",
		IsComplete: true,
	}, out); err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	events := collect(out)
	if len(events) != 0 {
		t.Fatalf("expected zero events for empty content flush, got %d: %+v", len(events), events)
	}
}

func TestStream_toolCallOnlyFlush(t *testing.T) {
	s := New()
	out := make(chan StreamEvent, 10)
	if err := s.Stream(LLMResponseChunk{
		Type:       StreamEventFunctionCall,
		Content:    "",
		IsComplete: true,
		ToolCalls:  []ToolCall{{Name: "t"}},
	}, out); err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	events := collect(out)
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Type != StreamEventFunctionCall {
		t.Errorf("expected Type %v, got %v", StreamEventFunctionCall, events[0].Type)
	}
	if len(events[0].ToolCalls) != 1 || events[0].ToolCalls[0].Name != "t" {
		t.Errorf("unexpected tool calls: %+v", events[0].ToolCalls)
	}
}
