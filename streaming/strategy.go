package streaming

// StreamingStrategy transforms LLM response chunks into user-facing stream
// events. It is the extension point for custom streaming behaviors (buffered,
// websocket-bridged, etc.).
type StreamingStrategy interface {
	Stream(LLMResponseChunk, chan<- StreamEvent) error
}
