package streaming

import "github.com/ryanaldo34/tacklr"

type BufferedStreamer struct {
	content   string
	msgId     string
	toolCalls []tacklr.ToolCall
}

func New() *BufferedStreamer { return &BufferedStreamer{} }

func (s *BufferedStreamer) Stream(chunk tacklr.LLMResponseChunk, out chan<- tacklr.StreamEvent) error {
	if chunk.Type == tacklr.StreamEventReasoning {
		out <- tacklr.StreamEvent{Type: tacklr.StreamEventReasoning, Content: chunk.Content, MessageID: chunk.MessageId}
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
		if chunk.Type == tacklr.StreamEventFunctionCall {
			s.toolCalls = append(s.toolCalls, chunk.ToolCalls...)
		}
	}
	if chunk.IsComplete {
		s.flush(out, chunk.Type)
		s.msgId = ""
	}
	return nil
}

func (s *BufferedStreamer) flush(out chan<- tacklr.StreamEvent, typ tacklr.StreamEventType) {
	if s.content != "" {
		out <- tacklr.StreamEvent{Type: typ, Content: s.content, MessageID: s.msgId}
	}
	if len(s.toolCalls) > 0 {
		out <- tacklr.StreamEvent{Type: tacklr.StreamEventFunctionCall, ToolCalls: s.toolCalls}
	}
	s.content = ""
	s.toolCalls = nil
}
