package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
)

// handleACPPost implements Streamable HTTP client→agent messages (POST /acp).
//
//	initialize → 200 + JSON body + Acp-Connection-Id (+ affinity cookie)
//	other methods / client RPC replies → 202; results arrive on GET SSE streams
func (p *acpProtocol) handleACPPost(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if env.Connections == nil {
		http.Error(w, "connection registry not configured", http.StatusInternalServerError)
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	body, err := readHTTPBody(r)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	// Batch JSON-RPC is not supported (RFD → 501).
	if trimmed[0] == '[' {
		http.Error(w, "JSON-RPC batch requests are not supported", http.StatusNotImplemented)
		return
	}

	peek, err := peekJSONRPC(trimmed)
	if err != nil {
		http.Error(w, "invalid JSON-RPC", http.StatusBadRequest)
		return
	}

	// Client JSON-RPC response (permission / elicitation reply): demux on bridge.
	if peek.IsResponse {
		conn, ok := p.lookupConnection(env, w, r)
		if !ok {
			return
		}
		_ = conn.Bridge.TryCompleteResponse(trimmed)
		setAffinityCookie(w, conn.ID)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if peek.Method == "initialize" {
		p.handleStreamableInitialize(env, w, r, trimmed, peek)
		return
	}

	conn, ok := p.lookupConnection(env, w, r)
	if !ok {
		return
	}
	setAffinityCookie(w, conn.ID)

	sessionHdr := strings.TrimSpace(r.Header.Get(HeaderAcpSessionID))
	if sessionScopedACPMethod(peek.Method) && sessionHdr == "" && !peek.IsNotification {
		// Notifications like session/cancel may still omit header if body has sessionId;
		// for requests we require the header per RFD.
		if !peek.IsNotification {
			http.Error(w, HeaderAcpSessionID+" required for "+peek.Method, http.StatusBadRequest)
			return
		}
	}
	// Prefer header; fall back to body sessionId for routing.
	sessionID := sessionHdr
	if sessionID == "" {
		sessionID = peek.SessionID
	}
	if sessionID != "" && peek.Method != "session/load" && !conn.hasSession(sessionID) {
		http.Error(w, "session is not attached to this connection", http.StatusForbidden)
		return
	}

	if !peek.IsNotification && len(peek.ID) > 0 {
		conn.rememberRoute(peek.ID, peek.Method, sessionID)
	}

	// 202 before work so the client can open/use SSE concurrently.
	w.WriteHeader(http.StatusAccepted)

	reqConn := &Conn{
		Writer:      conn.Writer,
		RPC:         conn.Bridge,
		setSecurity: conn.setSecurityContext,
	}
	securityContext := conn.securityContext()
	reqConn.Security = &securityContext
	reqEnv := ProtocolEnv{
		Registry:    env.Registry,
		Conn:        reqConn,
		Security:    env.Security,
		Connections: env.Connections,
	}
	// Use connection context — POST request context ends when the handler returns.
	ctx := conn.Context()
	go func() {
		if err := p.HandleInbound(ctx, reqEnv, trimmed); err != nil {
			slog.Debug("acp streamable post inbound", "error", err, "connection_id", conn.ID, "method", peek.Method)
		}
	}()
}

func (p *acpProtocol) handleStreamableInitialize(env ProtocolEnv, w http.ResponseWriter, r *http.Request, body []byte, peek jsonRPCPeek) {
	// initialize must not carry an existing connection id.
	if id := strings.TrimSpace(r.Header.Get(HeaderAcpConnectionID)); id != "" {
		http.Error(w, "initialize must not include "+HeaderAcpConnectionID, http.StatusBadRequest)
		return
	}

	sw := &acpStreamWriter{}
	conn := env.Connections.Create(nil, nil)
	if env.Conn != nil && env.Conn.Security != nil {
		conn.setSecurityContext(*env.Conn.Security)
	}
	sw.conn = conn
	bridge := NewClientBridge(sw)
	conn.Bridge = bridge
	conn.Writer = sw

	// Initialize result goes on the HTTP response body (200), not SSE.
	hw := &httpBufferWriter{}
	securityContext := conn.securityContext()
	reqConn := &Conn{
		Writer:      hw,
		RPC:         bridge,
		Security:    &securityContext,
		setSecurity: conn.setSecurityContext,
	}
	reqEnv := ProtocolEnv{Registry: env.Registry, Conn: reqConn, Security: env.Security, Connections: env.Connections}
	if err := p.HandleInbound(r.Context(), reqEnv, body); err != nil {
		slog.Debug("acp streamable initialize", "error", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(HeaderAcpConnectionID, conn.ID)
	setAffinityCookie(w, conn.ID)
	if len(hw.buf) == 0 {
		http.Error(w, "initialize produced no response", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(hw.buf)
}

// handleACPGet dispatches WebSocket upgrade vs Streamable HTTP SSE.
func (p *acpProtocol) handleACPGet(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if isWebSocketUpgrade(r) {
		p.handleACPWebSocket(env, w, r)
		return
	}
	p.handleACPStreamSSE(env, w, r)
}

// handleACPStreamSSE opens a connection- or session-scoped long-lived SSE stream.
func (p *acpProtocol) handleACPStreamSSE(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if env.Connections == nil {
		http.Error(w, "connection registry not configured", http.StatusInternalServerError)
		return
	}
	if !acceptSSE(r.Header.Get("Accept")) {
		http.Error(w, "Accept: text/event-stream required", http.StatusNotAcceptable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, ErrStreamingNotSupported.Error(), http.StatusInternalServerError)
		return
	}

	connID := strings.TrimSpace(r.Header.Get(HeaderAcpConnectionID))
	if connID == "" {
		// Cookie affinity alone is not enough to open a stream without the header (RFD).
		http.Error(w, HeaderAcpConnectionID+" required", http.StatusBadRequest)
		return
	}
	conn := env.Connections.Get(connID)
	if conn == nil {
		http.Error(w, "unknown connection", http.StatusNotFound)
		return
	}
	setAffinityCookie(w, conn.ID)

	sessionID := strings.TrimSpace(r.Header.Get(HeaderAcpSessionID))

	// Headers before attach: once the sink is registered, concurrent POSTs may
	// deliver events; http.ResponseWriter body access must stay on the sink mutex.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set(HeaderAcpConnectionID, conn.ID)
	if sessionID != "" {
		w.Header().Set(HeaderAcpSessionID, sessionID)
	}

	var (
		detach func()
		sink   *sseSink
		err    error
	)
	if sessionID == "" {
		detach, sink, err = conn.attachConnSSE(w, flusher)
	} else {
		detach, sink, err = conn.attachSessionSSE(sessionID, w, flusher)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	defer detach()

	// Open comment + flush under sink.mu so it cannot race deliver/writeJSONRPC.
	if err := sink.writeOpen(); err != nil {
		return
	}

	// Block until client disconnects or connection is torn down.
	select {
	case <-r.Context().Done():
	case <-conn.Context().Done():
	}
}

// handleACPDelete tears down a Streamable HTTP / logical connection.
func (p *acpProtocol) handleACPDelete(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if env.Connections == nil {
		http.Error(w, "connection registry not configured", http.StatusInternalServerError)
		return
	}
	connID := strings.TrimSpace(r.Header.Get(HeaderAcpConnectionID))
	if connID == "" {
		http.Error(w, HeaderAcpConnectionID+" required", http.StatusBadRequest)
		return
	}
	if env.Connections.Get(connID) == nil {
		http.Error(w, "unknown connection", http.StatusNotFound)
		return
	}
	env.Connections.Remove(connID)
	// Clear affinity cookie.
	http.SetCookie(w, &http.Cookie{
		Name:     CookieAcpAffinity,
		Value:    "",
		Path:     "/acp",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	w.WriteHeader(http.StatusAccepted)
}

func (p *acpProtocol) lookupConnection(env ProtocolEnv, w http.ResponseWriter, r *http.Request) (*Connection, bool) {
	connID := strings.TrimSpace(r.Header.Get(HeaderAcpConnectionID))
	if connID == "" {
		http.Error(w, HeaderAcpConnectionID+" required", http.StatusBadRequest)
		return nil, false
	}
	conn := env.Connections.Get(connID)
	if conn == nil {
		http.Error(w, "unknown connection", http.StatusNotFound)
		return nil, false
	}
	if conn.Bridge == nil || conn.Writer == nil {
		http.Error(w, "connection not ready", http.StatusInternalServerError)
		return nil, false
	}
	return conn, true
}

// jsonRPCPeek is a lightweight parse of an inbound JSON-RPC message.
type jsonRPCPeek struct {
	ID             json.RawMessage
	Method         string
	SessionID      string
	IsResponse     bool
	IsNotification bool
}

func peekJSONRPC(body []byte) (jsonRPCPeek, error) {
	var env struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return jsonRPCPeek{}, err
	}
	p := jsonRPCPeek{ID: env.ID, Method: env.Method}
	if env.Method == "" && len(env.ID) > 0 && string(env.ID) != "null" {
		// Response: has id, no method, and result or error.
		if len(env.Result) > 0 || len(env.Error) > 0 {
			p.IsResponse = true
			return p, nil
		}
	}
	if env.Method != "" && (len(env.ID) == 0 || string(env.ID) == "null") {
		p.IsNotification = true
	}
	if len(env.Params) > 0 {
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(env.Params, &params) == nil {
			p.SessionID = params.SessionID
		}
	}
	return p, nil
}

// httpBufferWriter captures a single JSON-RPC write for initialize's 200 body.
type httpBufferWriter struct {
	mu  sync.Mutex
	buf []byte
}

func (h *httpBufferWriter) WriteResult(id json.RawMessage, result any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		return err
	}
	h.buf = append(data, '\n')
	return nil
}

func (h *httpBufferWriter) WriteError(id json.RawMessage, err error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	data, mErr := json.Marshal(jsonRPCErrorBody(id, err))
	if mErr != nil {
		return mErr
	}
	h.buf = append(data, '\n')
	return nil
}

func (h *httpBufferWriter) WriteFrame(data []byte) error {
	// initialize should not stream frames on the HTTP body path.
	return nil
}
