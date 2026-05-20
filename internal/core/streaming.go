package core

type StreamingStategy interface {
	Stream(LLMResponseChunk, chan<- StreamEvent) error
}

// BufferedStreamer accumulates partial chunks and only emits when a chunk
// is marked IsComplete. Content is concatenated; tool calls are aggregated.
// This produces a single StreamEventMessage per completed assistant turn
// followed by a StreamEventFunctionCall if any tool calls were present.
type BufferedStreamer struct {
	content   string
	msgId     string
	toolCalls []ToolCall
}

func (s *BufferedStreamer) Stream(chunk LLMResponseChunk, out chan<- StreamEvent) error {
	if chunk.Type == StreamEventReasoning {
		out <- StreamEvent{Type: StreamEventReasoning, Content: chunk.Content, MessageID: chunk.MessageId}
		return nil
	}

	if s.msgId == "" {
		s.msgId = chunk.MessageId
	}
	if chunk.MessageId == s.msgId {
		s.content += chunk.Content
		if chunk.Type == StreamEventFunctionCall {
			s.toolCalls = append(s.toolCalls, chunk.ToolCalls...)
		}
	}
	if chunk.IsComplete || s.msgId != chunk.MessageId {
		if s.content != "" {
			out <- StreamEvent{Type: chunk.Type, Content: s.content, MessageID: chunk.MessageId}
		}
		if len(s.toolCalls) > 0 {
			out <- StreamEvent{Type: StreamEventFunctionCall, ToolCalls: s.toolCalls}
		}
		s.content = ""
		s.toolCalls = nil
		if chunk.MessageId != s.msgId {
			s.msgId = chunk.MessageId
		} else {
			s.msgId = ""
		}
	}
	return nil
}
