package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
)

type sseEvent struct {
	Type      string             `json:"type"`
	Content   string             `json:"content,omitempty"`
	Data      json.RawMessage    `json:"data,omitempty"`
	ToolCalls []tacklr.ToolCall `json:"tool_calls,omitempty"`
	Error     string             `json:"error,omitempty"`
}

type threadEvent struct {
	ThreadID string `json:"thread_id"`
}

func acceptsSSE(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	if !acceptsSSE(r) {
		http.Error(w, "SSE endpoint requires Accept: text/event-stream", http.StatusNotAcceptable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		slog.Error("response writer does not support flushing")
		http.Error(w, ErrStreamingNotSupported.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.sseInternalError(w, flusher, "failed to read request body", err)
		return
	}

	var req turnRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.sseClientError(w, flusher, clientErrorf(ErrInvalidRequest, "invalid JSON: %v", err))
		return
	}
	if err := validateRequest(req, false); err != nil {
		s.sseClientError(w, flusher, err)
		return
	}

	threadID, load := resolveThread(req, false)
	h, _, err := s.loadAgent(r.Context(), req.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			s.sseClientError(w, flusher, err)
			return
		}
		s.sseInternalError(w, flusher, "failed to load agent", err, "agent_id", req.AgentID, "thread_id", threadID)
		return
	}
	defer h.Close()

	w.Header().Set("X-Thread-ID", threadID)
	threadData, err := json.Marshal(threadEvent{ThreadID: threadID})
	if err != nil {
		s.sseInternalError(w, flusher, "failed to marshal thread event", err, "thread_id", threadID)
		return
	}
	if err := writeSSEEvent(w, flusher, "thread", threadData); err != nil {
		slog.Warn("failed to write thread event", "error", err, "thread_id", threadID)
		return
	}

	events, err := runHarness(r.Context(), h, req, false)
	if err != nil {
		if IsClientError(err) {
			s.sseClientError(w, flusher, err)
			return
		}
		s.sseInternalError(w, flusher, "agent run failed", err, "agent_id", req.AgentID, "thread_id", threadID)
		return
	}

	for ev := range events {
		if err := s.writeSSEStreamEvent(w, flusher, ev); err != nil {
			slog.Warn("failed to write SSE event", "error", err, "thread_id", threadID, "event_type", ev.Type)
			return
		}
	}
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	if !acceptsSSE(r) {
		http.Error(w, "SSE endpoint requires Accept: text/event-stream", http.StatusNotAcceptable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		slog.Error("response writer does not support flushing")
		http.Error(w, ErrStreamingNotSupported.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.sseInternalError(w, flusher, "failed to read request body", err)
		return
	}

	var req turnRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.sseClientError(w, flusher, clientErrorf(ErrInvalidRequest, "invalid JSON: %v", err))
		return
	}
	if err := validateRequest(req, true); err != nil {
		s.sseClientError(w, flusher, err)
		return
	}

	threadID, load := resolveThread(req, true)
	h, _, err := s.loadAgent(r.Context(), req.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			s.sseClientError(w, flusher, err)
			return
		}
		s.sseInternalError(w, flusher, "failed to load agent", err, "agent_id", req.AgentID, "thread_id", threadID)
		return
	}
	defer h.Close()

	w.Header().Set("X-Thread-ID", threadID)

	events, err := runHarness(r.Context(), h, req, true)
	if err != nil {
		if IsClientError(err) {
			s.sseClientError(w, flusher, err)
			return
		}
		s.sseInternalError(w, flusher, "agent resume failed", err, "agent_id", req.AgentID, "thread_id", threadID)
		return
	}

	for ev := range events {
		if err := s.writeSSEStreamEvent(w, flusher, ev); err != nil {
			slog.Warn("failed to write SSE event", "error", err, "thread_id", threadID, "event_type", ev.Type)
			return
		}
	}
}

func (s *Server) writeSSEStreamEvent(w http.ResponseWriter, flusher http.Flusher, ev tacklr.StreamEvent) error {
	data, err := json.Marshal(toSSEEvent(ev))
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data); err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	flusher.Flush()
	return nil
}

func (s *Server) sseClientError(w http.ResponseWriter, flusher http.Flusher, err error) {
	slog.Debug("sse client error", "error", err)
	if err := writeSSEError(w, flusher, err.Error()); err != nil {
		slog.Warn("failed to write SSE error", "error", err)
	}
}

func (s *Server) sseInternalError(w http.ResponseWriter, flusher http.Flusher, msg string, err error, attrs ...any) {
	slog.Error(msg, append(attrs, "error", err)...)
	if err := writeSSEError(w, flusher, ErrInternal.Error()); err != nil {
		slog.Warn("failed to write SSE error", "error", err)
	}
}

func toSSEEvent(ev tacklr.StreamEvent) sseEvent {
	e := sseEvent{Type: string(ev.Type), Content: ev.Content, Data: ev.Data, ToolCalls: ev.ToolCalls}
	if ev.Error != nil {
		e.Error = ev.Error.Error()
	}
	return e
}

func writeSSEEvent(w io.Writer, flusher http.Flusher, evType string, data []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, msg string) error {
	data, err := json.Marshal(sseEvent{Type: "error", Error: msg})
	if err != nil {
		return fmt.Errorf("marshal error event: %w", err)
	}
	return writeSSEEvent(w, flusher, "error", data)
}
