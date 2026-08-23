package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// MessageWriter is the sink a Protocol uses for results and streamed frames.
// Each protocol supplies an implementation for its wire (JSON-RPC, custom HTTP, WebSocket).
type MessageWriter interface {
	WriteResult(id json.RawMessage, result any) error
	// WriteError writes a failure. Implementations should use PublicError /
	// JSONRPCErrorCode so internal details are not leaked on the wire.
	WriteError(id json.RawMessage, err error) error
	WriteFrame(data []byte) error
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

// jsonRPCWSMessageWriter writes full JSON-RPC 2.0 envelopes over a WebSocket.
type jsonRPCWSMessageWriter struct {
	ctx context.Context
	c   *websocket.Conn
	mu  sync.Mutex
}

func (m *jsonRPCWSMessageWriter) WriteResult(id json.RawMessage, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsjson.Write(m.ctx, m.c, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (m *jsonRPCWSMessageWriter) WriteError(id json.RawMessage, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return wsjson.Write(m.ctx, m.c, jsonRPCErrorBody(id, err))
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
