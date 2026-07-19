package server

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
)

type wsErrorMessage struct {
	Type  string `json:"type"`
	Error string `json:"error"`
}

// wsServerEvent is the wire JSON format for events written to WebSocket clients.
type wsServerEvent struct {
	Type      string            `json:"type"`
	TurnID    string            `json:"turn_id,omitempty"`
	MessageID string            `json:"message_id,omitempty"`
	Content   string            `json:"content,omitempty"`
	Data      json.RawMessage   `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	Error     string            `json:"error,omitempty"`
}

func writeWSClientError(ctx context.Context, c *websocket.Conn, err error) error {
	slog.Debug("websocket client error", "error", err)
	return writeWSJSON(ctx, c, wsErrorMessage{Type: "error", Error: err.Error()})
}

func writeWSInternalError(ctx context.Context, c *websocket.Conn) error {
	return writeWSJSON(ctx, c, wsErrorMessage{Type: "error", Error: ErrInternal.Error()})
}

func writeWSJSON(ctx context.Context, c *websocket.Conn, v any) error {
	return wsjson.Write(ctx, c, v)
}
