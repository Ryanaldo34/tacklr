package tacklr

// streamAssembler accumulates model stream deltas by type+message id and
// resolves full content when a complete chunk arrives (with empty Content).
// Shared by the Run turn loop and ContextManager.Handoff.
type streamAssembler struct {
	buf map[string]string
}

func newStreamAssembler() *streamAssembler {
	return &streamAssembler{buf: make(map[string]string)}
}

func streamChunkKey(chunk LLMResponseChunk) string {
	return string(chunk.Type) + ":" + chunk.MessageId
}

// AddDelta records a non-complete content delta for message or reasoning streams.
func (s *streamAssembler) AddDelta(chunk LLMResponseChunk) {
	if s == nil || s.buf == nil || chunk.IsComplete || chunk.Content == "" {
		return
	}
	if chunk.Type != StreamEventMessage && chunk.Type != StreamEventReasoning {
		return
	}
	s.buf[streamChunkKey(chunk)] += chunk.Content
}

// CompleteContent returns the full text for a completed chunk, preferring
// chunk.Content and falling back to accumulated deltas.
func (s *streamAssembler) CompleteContent(chunk LLMResponseChunk) string {
	if chunk.Content != "" {
		return chunk.Content
	}
	if s == nil || s.buf == nil {
		return ""
	}
	return s.buf[streamChunkKey(chunk)]
}

// MessageFromComplete builds a context-window Message for a completed
// message or reasoning chunk (not function calls).
func (s *streamAssembler) MessageFromComplete(chunk LLMResponseChunk) *Message {
	role := RoleAssistant
	if chunk.Type == StreamEventReasoning {
		role = RoleReasoning
	}
	return &Message{
		Role:      role,
		Content:   s.CompleteContent(chunk),
		ToolCalls: append([]ToolCall(nil), chunk.ToolCalls...),
		MessageID: chunk.MessageId,
	}
}
