package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// Header names for the ACP Streamable HTTP / WebSocket transport (RFD).
const (
	HeaderAcpConnectionID = "Acp-Connection-Id"
	HeaderAcpSessionID    = "Acp-Session-Id"

	// CookieAcpAffinity sticks a client to the same backend for one connection.
	CookieAcpAffinity = "acp_affinity"
)

var errSSESinkClosed = errors.New("acp sse sink closed")

// Connection is one client transport connection (WebSocket or Streamable HTTP).
// Harness sessions live in Registry; this is ephemeral wire state only.
type Connection struct {
	ID     string
	Bridge *ClientBridge
	Writer MessageWriter

	// ctx is cancelled when the connection is torn down (DELETE, WS close, Remove).
	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	connSink     *sseSink
	sessionSinks map[string]*sseSink
	// routes maps JSON-RPC request id → delivery target for the matching result.
	routes   map[string]streamRoute
	sessions map[string]struct{}
	closed   bool
}

type streamRoute struct {
	method    string
	sessionID string
	// connLevel forces delivery on the connection-scoped SSE stream.
	connLevel bool
}

// sseSink is one long-lived GET text/event-stream writer.
type sseSink struct {
	mu     sync.Mutex
	w      http.ResponseWriter
	f      http.Flusher
	closed bool
}

func (s *sseSink) writeJSONRPC(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSSESinkClosed
	}
	// RFD: each SSE event body is one JSON-RPC message.
	return writeSSEEvent(s.w, s.f, "message", data)
}

func (s *sseSink) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// ConnectionRegistry tracks active ACP connections by Acp-Connection-Id.
type ConnectionRegistry struct {
	mu   sync.Mutex
	byID map[string]*Connection
}

// NewConnectionRegistry returns an empty registry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{byID: make(map[string]*Connection)}
}

// Create registers a new connection with a generated id and cancellable context.
// bridge/writer may be filled in after Create (WebSocket accept path).
func (r *ConnectionRegistry) Create(bridge *ClientBridge, writer MessageWriter) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		ID:           uuid.NewString(),
		Bridge:       bridge,
		Writer:       writer,
		ctx:          ctx,
		cancel:       cancel,
		sessionSinks: make(map[string]*sseSink),
		routes:       make(map[string]streamRoute),
		sessions:     make(map[string]struct{}),
	}
	if r == nil {
		return c
	}
	r.mu.Lock()
	r.byID[c.ID] = c
	r.mu.Unlock()
	return c
}

// Get returns the connection for id, or nil.
func (r *ConnectionRegistry) Get(id string) *Connection {
	if r == nil || id == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byID[id]
}

// Remove deletes the connection and cancels its context. Safe if missing or r is nil.
func (r *ConnectionRegistry) Remove(id string) {
	if r == nil || id == "" {
		return
	}
	r.mu.Lock()
	c := r.byID[id]
	delete(r.byID, id)
	r.mu.Unlock()
	if c != nil {
		c.shutdown()
	}
}

func (c *Connection) shutdown() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.connSink != nil {
		c.connSink.close()
		c.connSink = nil
	}
	for id, s := range c.sessionSinks {
		s.close()
		delete(c.sessionSinks, id)
	}
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Context is cancelled when the connection is removed or shut down.
func (c *Connection) Context() context.Context {
	if c == nil || c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// rememberRoute records where a request's JSON-RPC result should be delivered.
func (c *Connection) rememberRoute(id json.RawMessage, method, sessionID string) {
	if c == nil || len(id) == 0 {
		return
	}
	connLevel := method == "session/new" || method == "session/load" ||
		method == "initialize" || method == "authenticate"
	c.mu.Lock()
	c.routes[string(id)] = streamRoute{method: method, sessionID: sessionID, connLevel: connLevel}
	c.mu.Unlock()
}

func (c *Connection) takeRoute(id json.RawMessage) streamRoute {
	if c == nil || len(id) == 0 {
		return streamRoute{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.routes[string(id)]
	if ok {
		delete(c.routes, string(id))
	}
	return r
}

func (c *Connection) noteSession(sessionID string) {
	if c == nil || sessionID == "" {
		return
	}
	c.mu.Lock()
	c.sessions[sessionID] = struct{}{}
	c.mu.Unlock()
}

// attachConnSSE registers the connection-scoped GET stream. Returns a detach func.
func (c *Connection) attachConnSSE(w http.ResponseWriter, f http.Flusher) (detach func(), err error) {
	if c == nil {
		return nil, fmt.Errorf("nil connection")
	}
	sink := &sseSink{w: w, f: f}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errSSESinkClosed
	}
	if c.connSink != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("connection-scoped SSE already open")
	}
	c.connSink = sink
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		if c.connSink == sink {
			c.connSink = nil
		}
		c.mu.Unlock()
		sink.close()
	}, nil
}

// attachSessionSSE registers a session-scoped GET stream.
func (c *Connection) attachSessionSSE(sessionID string, w http.ResponseWriter, f http.Flusher) (detach func(), err error) {
	if c == nil || sessionID == "" {
		return nil, fmt.Errorf("session id required")
	}
	sink := &sseSink{w: w, f: f}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errSSESinkClosed
	}
	if _, ok := c.sessionSinks[sessionID]; ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("session-scoped SSE already open for %s", sessionID)
	}
	c.sessions[sessionID] = struct{}{}
	c.sessionSinks[sessionID] = sink
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		if c.sessionSinks[sessionID] == sink {
			delete(c.sessionSinks, sessionID)
		}
		c.mu.Unlock()
		sink.close()
	}, nil
}

// deliver sends one JSON-RPC message to the appropriate SSE stream(s).
// connLevel or empty sessionID → connection stream; else session stream with
// fallback to connection stream so messages are not dropped if session SSE is late.
func (c *Connection) deliver(sessionID string, data []byte, connLevel bool) error {
	if c == nil {
		return fmt.Errorf("nil connection")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errSSESinkClosed
	}
	var targets []*sseSink
	if connLevel || sessionID == "" {
		if c.connSink != nil {
			targets = append(targets, c.connSink)
		}
	} else {
		if s := c.sessionSinks[sessionID]; s != nil {
			targets = append(targets, s)
		} else if c.connSink != nil {
			targets = append(targets, c.connSink)
		}
	}
	c.mu.Unlock()

	if len(targets) == 0 {
		// No listener yet — drop (RFD v1: no message replay).
		return nil
	}
	var first error
	for _, t := range targets {
		if err := t.writeJSONRPC(data); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// acpStreamWriter implements MessageWriter for Streamable HTTP: agent→client
// traffic is delivered on connection- and session-scoped SSE streams.
type acpStreamWriter struct {
	conn *Connection
}

func (w *acpStreamWriter) WriteResult(id json.RawMessage, result any) error {
	body := map[string]any{"jsonrpc": "2.0", "id": id, "result": result}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	route := w.conn.takeRoute(id)
	sessionID := route.sessionID
	connLevel := route.connLevel
	if res, ok := result.(map[string]any); ok {
		if sid, _ := res["sessionId"].(string); sid != "" {
			w.conn.noteSession(sid)
			if route.method == "session/new" || route.method == "session/load" {
				connLevel = true
				sessionID = ""
			}
		}
	}
	return w.conn.deliver(sessionID, data, connLevel)
}

func (w *acpStreamWriter) WriteError(id json.RawMessage, err error) error {
	data, mErr := json.Marshal(jsonRPCErrorBody(id, err))
	if mErr != nil {
		return mErr
	}
	route := w.conn.takeRoute(id)
	return w.conn.deliver(route.sessionID, data, route.connLevel)
}

func (w *acpStreamWriter) WriteFrame(data []byte) error {
	// JSON-RPC responses (prompt stopReason, errors) often lack sessionId in the
	// body — route by the pending request id recorded at POST time (RFD: session stream).
	var env struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if json.Unmarshal(data, &env) == nil && env.Method == "" && len(env.ID) > 0 && string(env.ID) != "null" {
		route := w.conn.takeRoute(env.ID)
		// If we had a remembered route, use it; otherwise fall through to body sessionId.
		if route.method != "" || route.sessionID != "" || route.connLevel {
			return w.conn.deliver(route.sessionID, data, route.connLevel)
		}
	}
	sessionID := extractSessionIDFromJSONRPC(data)
	return w.conn.deliver(sessionID, data, sessionID == "")
}

func extractSessionIDFromJSONRPC(data []byte) string {
	var env struct {
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(data, &env) != nil {
		return ""
	}
	for _, raw := range []json.RawMessage{env.Params, env.Result} {
		if len(raw) == 0 {
			continue
		}
		var p struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(raw, &p) == nil && p.SessionID != "" {
			return p.SessionID
		}
	}
	return ""
}

// setAffinityCookie is set for load-balancer sticky affinity (RFD). Clients must
// store cookies; the server always keys connections by Acp-Connection-Id header.
func setAffinityCookie(w http.ResponseWriter, connectionID string) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieAcpAffinity,
		Value:    connectionID,
		Path:     "/acp",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}

// isJSONContentType reports whether Content-Type is application/json (optional params).
func isJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	media, _, _ := strings.Cut(ct, ";")
	return strings.TrimSpace(strings.ToLower(media)) == "application/json"
}

// acceptSSE reports whether Accept includes text/event-stream.
func acceptSSE(accept string) bool {
	return strings.Contains(strings.ToLower(accept), "text/event-stream")
}

// sessionScopedACPMethod is true when the RFD requires Acp-Session-Id on POST.
func sessionScopedACPMethod(method string) bool {
	switch method {
	case "session/prompt", "session/resume", "session/cancel",
		"session/set_config_option", "session/close":
		return true
	default:
		return false
	}
}
