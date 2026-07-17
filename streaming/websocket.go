package streaming

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
)

// WebSocketStreamer is a StreamingStrategy that forwards inference chunks
// directly to a WebSocket connection. It behaves like the default pass-through
// strategy on the outgoing event channel, so the harness loop and any other
// consumers still receive StreamEvents normally.
type WebSocketStreamer struct {
	ctx  context.Context
	conn *websocket.Conn
}

// NewWebSocketStreamer creates a streamer that writes chunks to conn using the
// provided connection context. The context should be cancelled when the
// connection is closed to stop further writes.
func NewWebSocketStreamer(ctx context.Context, conn *websocket.Conn) *WebSocketStreamer {
	return &WebSocketStreamer{ctx: ctx, conn: conn}
}

// Stream implements tacklr.StreamingStrategy. It writes each non-empty
// inference chunk to the WebSocket as a JSON event and also forwards the
// StreamEvent to out.
func (s *WebSocketStreamer) Stream(chunk tacklr.LLMResponseChunk, out chan<- tacklr.StreamEvent) error {
	ev := tacklr.StreamEvent{
		Type:      chunk.Type,
		TurnID:    chunk.TurnId,
		MessageID: chunk.MessageId,
		ToolCalls: chunk.ToolCalls,
		Content:   chunk.Content,
	}

	if chunk.Type == "" && chunk.IsComplete {
		ev.Type = tacklr.StreamEventComplete
	}

	if ev.Type != "" {
		if err := s.WriteEvent(ev); err != nil {
			return err
		}
	}

	out <- ev
	return nil
}

// WriteEvent writes a StreamEvent to the underlying WebSocket connection.
// github.com/coder/websocket supports concurrent writes, so no additional
// synchronization is required.
func (s *WebSocketStreamer) WriteEvent(ev tacklr.StreamEvent) error {
	wire := wsServerEvent{
		Type:      string(ev.Type),
		TurnID:    ev.TurnID,
		MessageID: ev.MessageID,
		Content:   ev.Content,
		Data:      ev.Data,
		ToolCalls: ev.ToolCalls,
	}
	if ev.Error != nil {
		wire.Error = ev.Error.Error()
	}

	return wsjson.Write(s.ctx, s.conn, wire)
}

type wsServerEvent struct {
	Type      string             `json:"type"`
	TurnID    string             `json:"turn_id,omitempty"`
	MessageID string             `json:"message_id,omitempty"`
	Content   string             `json:"content,omitempty"`
	Data      json.RawMessage    `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	Error     string             `json:"error,omitempty"`
}
