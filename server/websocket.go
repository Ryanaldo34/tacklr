package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

type wsErrorMessage struct {
	Type  string `json:"type"`
	Error string `json:"error"`
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

func (s *Server) handleWebSocketPrompt(w http.ResponseWriter, r *http.Request) {
	s.handleWebSocket(w, r, false)
}

func (s *Server) handleWebSocketResume(w http.ResponseWriter, r *http.Request) {
	s.handleWebSocket(w, r, true)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, resume bool) {
	// InsecureSkipVerify preserves the previous behavior of allowing any origin.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		slog.Warn("websocket accept failed", "error", err)
		return
	}
	defer func() {
		if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
			slog.Debug("failed to close websocket cleanly", "error", err)
		}
	}()

	// Create a context that is cancelled when the handler finishes, the client
	// closes the connection, or the underlying request is cancelled.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var req turnRequest
	if err := wsjson.Read(ctx, c, &req); err != nil {
		slog.Debug("failed to read websocket message", "error", err)
		if werr := s.writeWSClientError(ctx, c, clientErrorf(ErrInvalidRequest, "failed to read message: %v", err)); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	// Start a goroutine that discards any further client messages and cancels
	// the agent context if the client disconnects mid-stream.
	go func() {
		defer cancel()
		for {
			_, _, err := c.Read(ctx)
			if err != nil {
				return
			}
		}
	}()

	if err := validateRequest(req, resume); err != nil {
		if werr := s.writeWSClientError(ctx, c, err); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(req, resume)
	h, _, err := s.loadAgent(ctx, req.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			if werr := s.writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", req.AgentID, "thread_id", threadID)
		if werr := s.writeWSInternalError(ctx, c); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}
	defer h.Close()

	streamer := streaming.NewWebSocketStreamer(ctx, c)
	h.WithStreamingStrategy(streamer)

	if err := streamer.WriteEvent(tacklr.StreamEvent{
		Type:    tacklr.StreamEventType("thread"),
		Content: threadID,
	}); err != nil {
		slog.Warn("failed to write thread event to websocket", "error", err, "thread_id", threadID)
		return
	}

	events, err := runHarness(ctx, h, req, resume)
	if err != nil {
		if IsClientError(err) {
			if werr := streamer.WriteEvent(tacklr.StreamEvent{
				Type:  tacklr.StreamEventError,
				Error: err,
			}); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", req.AgentID, "thread_id", threadID)
		if werr := streamer.WriteEvent(tacklr.StreamEvent{
			Type:  tacklr.StreamEventError,
			Error: ErrInternal,
		}); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	for ev := range events {
		if isInferenceEventType(string(ev.Type)) {
			continue
		}
		if err := streamer.WriteEvent(ev); err != nil {
			slog.Warn("failed to write websocket event", "error", err, "thread_id", threadID, "event_type", ev.Type)
			return
		}
	}
}

func (s *Server) writeWSClientError(ctx context.Context, c *websocket.Conn, err error) error {
	slog.Debug("websocket client error", "error", err)
	return writeWSJSON(ctx, c, wsErrorMessage{Type: "error", Error: err.Error()})
}

func (s *Server) writeWSInternalError(ctx context.Context, c *websocket.Conn) error {
	return writeWSJSON(ctx, c, wsErrorMessage{Type: "error", Error: ErrInternal.Error()})
}

func isInferenceEventType(typ string) bool {
	switch typ {
	case string(tacklr.StreamEventMessage),
		string(tacklr.StreamEventReasoning),
		string(tacklr.StreamEventFunctionCall),
		string(tacklr.StreamEventComplete),
		"":
		return true
	}
	return false
}

func writeWSJSON(ctx context.Context, c *websocket.Conn, v any) error {
	return wsjson.Write(ctx, c, v)
}
