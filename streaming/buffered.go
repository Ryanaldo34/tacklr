package streaming

type BufferedStreamer struct {
	content   string
	msgId     string
	toolCalls []ToolCall
}

func New() *BufferedStreamer { return &BufferedStreamer{} }

func (s *BufferedStreamer) Stream(chunk LLMResponseChunk, out chan<- StreamEvent) error {
	if chunk.Type == StreamEventReasoning {
		out <- StreamEvent{Type: StreamEventReasoning, Content: chunk.Content, MessageID: chunk.MessageId}
		return nil
	}

	if s.msgId != "" && chunk.MessageId != s.msgId {
		s.flush(out, chunk.Type)
		s.msgId = ""
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
	if chunk.IsComplete {
		s.flush(out, chunk.Type)
		s.msgId = ""
	}
	return nil
}

func (s *BufferedStreamer) flush(out chan<- StreamEvent, typ StreamEventType) {
	if s.content != "" {
		out <- StreamEvent{Type: typ, Content: s.content, MessageID: s.msgId}
	}
	if len(s.toolCalls) > 0 {
		out <- StreamEvent{Type: StreamEventFunctionCall, ToolCalls: s.toolCalls}
	}
	s.content = ""
	s.toolCalls = nil
}
