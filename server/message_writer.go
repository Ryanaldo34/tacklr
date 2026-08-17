package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// MessageWriter is the transport-agnostic sink for protocol responses and
// streamed frames. Transports adapt HTTP, stdio, and WebSocket to this interface.
type MessageWriter interface {
	WriteResult(id json.RawMessage, result any) error
	// WriteError writes a failure. Implementations should use PublicError /
	// JSONRPCErrorCode so internal details are not leaked on the wire.
	WriteError(id json.RawMessage, err error) error
	WriteFrame(data []byte) error
}

// lineMessageWriter writes NDJSON frames (stdio / line-delimited RPC).
type lineMessageWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (m *lineMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return writeJSONLine(m.w, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *lineMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return writeJSONLine(m.w, jsonRPCErrorBody(id, err))
}

func (m *lineMessageWriter) WriteFrame(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.w.Write(data); err != nil {
		return err
	}
	if _, err := m.w.Write([]byte{'\n'}); err != nil {
		return err
	}
	if s, ok := m.w.(interface{ Sync() error }); ok {
		_ = s.Sync()
	}
	return nil
}

// jsonRPCMessageWriter writes JSON-RPC over an HTTP response.
type jsonRPCMessageWriter struct {
	w       http.ResponseWriter
	wroteCT bool
}

func (m *jsonRPCMessageWriter) ensureCT() {
	if !m.wroteCT {
		m.w.Header().Set("Content-Type", "application/json")
		m.wroteCT = true
	}
}

func (m *jsonRPCMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.ensureCT()
	return json.NewEncoder(m.w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *jsonRPCMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.ensureCT()
	return json.NewEncoder(m.w).Encode(jsonRPCErrorBody(id, err))
}

func (m *jsonRPCMessageWriter) WriteFrame(data []byte) error {
	m.ensureCT()
	if _, err := m.w.Write(data); err != nil {
		return err
	}
	_, err := m.w.Write([]byte{'\n'})
	return err
}

// sseMessageWriter wraps SSE framing.
type sseMessageWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func (m *sseMessageWriter) WriteResult(id json.RawMessage, result any) error {
	data, err := json.Marshal(map[string]any{"id": id, "result": result})
	if err != nil {
		return err // e.g. channel in result
	}
	return writeSSEEvent(m.w, m.flusher, "result", data)
}

func (m *sseMessageWriter) WriteError(id json.RawMessage, err error) error {
	return writeSSEError(m.w, m.flusher, PublicError(err).Error())
}

func (m *sseMessageWriter) WriteFrame(data []byte) error {
	var holder struct {
		Type string `json:"type"`
	}
	eventType := "message"
	if err := json.Unmarshal(data, &holder); err == nil && holder.Type != "" {
		eventType = holder.Type
	}
	return writeSSEEvent(m.w, m.flusher, eventType, data)
}

// wsMessageWriter writes SSE-protocol-shaped frames over a WebSocket.
// Used by the native SSE protocol only — not ACP.
type wsMessageWriter struct {
	ctx context.Context
	c   *websocket.Conn
}

func (m *wsMessageWriter) WriteResult(id json.RawMessage, result any) error {
	return wsWriteJSON(m.ctx, m.c, map[string]any{"id": id, "result": result})
}

func (m *wsMessageWriter) WriteError(id json.RawMessage, err error) error {
	return wsWriteJSON(m.ctx, m.c, presentationError(err))
}

func (m *wsMessageWriter) WriteFrame(data []byte) error {
	return m.c.Write(m.ctx, websocket.MessageText, data)
}

// jsonRPCWSMessageWriter writes full JSON-RPC 2.0 envelopes over a WebSocket.
// Used by ACP WebSocket transport (and future Streamable HTTP demux writers).
type jsonRPCWSMessageWriter struct {
	ctx context.Context
	c   *websocket.Conn
	mu  sync.Mutex
}

func (m *jsonRPCWSMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsWriteJSON(m.ctx, m.c, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *jsonRPCWSMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsWriteJSON(m.ctx, m.c, jsonRPCErrorBody(id, err))
}

func (m *jsonRPCWSMessageWriter) WriteFrame(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c.Write(m.ctx, websocket.MessageText, data)
}

func jsonRPCErrorBody(id json.RawMessage, err error) map[string]any {
	pub := PublicError(err)
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    JSONRPCErrorCode(err),
			"message": pub.Error(),
		},
	}
}

func writeJSONLine(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := w.Write([]byte{'\n'}); err != nil {
		return err
	}
	if s, ok := w.(interface{ Sync() error }); ok {
		_ = s.Sync()
	}
	return nil
}
