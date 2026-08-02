package server

import (
	"encoding/json"

	"github.com/coder/websocket/wsjson"

	"github.com/ryanaldo34/tacklr"
)

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

// wsWriteJSON is the WebSocket JSON write implementation. Tests may swap it.
var wsWriteJSON = wsjson.Write
