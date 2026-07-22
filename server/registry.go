package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

type AgentSpec struct {
	Config            tacklr.Config
	Model             tacklr.InferenceStrategy
	Tools             []*tacklr.Tool
	MCPConfigs        []mcp.MCPConfig
	WatchDog          tacklr.AgentWatchDog
	StreamingStrategy tacklr.StreamingStrategy
	Store             stores.BaseStore
}

// sessionState holds per-session configuration provided by the client at
// session creation time (session/new) and updated via session/set_config_option.
type sessionState struct {
	cwd          string
	mcpServers   []mcp.MCPConfig
	prompted     bool // true after the first prompt turn has been initiated
	configValues map[string]string
}

type Registry struct {
	agents       map[string]AgentSpec
	defaultAgent string
	store        stores.BaseStore
	activeCtx    sync.Map // threadID → context.CancelFunc
	sessions     sync.Map // threadID → *sessionState
}

func NewRegistry(store stores.BaseStore, defaultAgent string) *Registry {
	return &Registry{
		agents:       make(map[string]AgentSpec),
		defaultAgent: defaultAgent,
		store:        store,
	}
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
	r.agents[agentID] = spec
}

// buildConfigOptions returns the ACP config options for a session, with
// currentAgent as the selected agent value (falls back to defaultAgent).
func (r *Registry) buildConfigOptions(currentAgent string) []ConfigOption {
	if currentAgent == "" {
		currentAgent = r.defaultAgent
	}
	ids := make([]string, 0, len(r.agents))
	for id := range r.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	opts := make([]ConfigOptionValue, 0, len(ids))
	for _, id := range ids {
		opts = append(opts, ConfigOptionValue{
			Value: id,
			Name:  id,
		})
	}
	return []ConfigOption{
		{
			ID:           "agent",
			Name:         "Agent",
			Description:  "Select which registered agent handles this session",
			Category:     "agent",
			Type:         "select",
			CurrentValue: currentAgent,
			Options:      opts,
		},
	}
}

func (r *Registry) sessionAgentID(sess *sessionState) string {
	if sess != nil && sess.configValues != nil {
		if id := sess.configValues["agent"]; id != "" {
			return id
		}
	}
	return r.defaultAgent
}

func (r *Registry) loadAgent(ctx context.Context, agentID, threadID string, load bool) (*tacklr.AgentHarness, *AgentSpec, error) {
	spec, ok := r.agents[agentID]
	if !ok {
		return nil, nil, clientErrorf(ErrAgentNotFound, "agent %q not found", agentID)
	}

	store := r.store
	if spec.Store != nil {
		store = spec.Store
	}

	var h *tacklr.AgentHarness
	var err error
	if load {
		if store == nil {
			return nil, nil, clientErrorf(ErrSessionStoreNotConfigured, "session store is not configured")
		}
		h, err = tacklr.NewAgentHarnessFromSession(ctx, threadID, spec.Config, spec.Model, store, spec.WatchDog)
		if err != nil {
			return nil, nil, err
		}
	} else {
		h = tacklr.NewAgent(spec.Config, spec.Model, store, spec.WatchDog)
	}

	h.SessionId = threadID
	h.Tools = spec.Tools
	h.MCPConfigs = spec.MCPConfigs
	if spec.StreamingStrategy != nil {
		h.WithStreamingStrategy(spec.StreamingStrategy)
	}
	return h, &spec, nil
}

// streamEvents applies the protocol hook to each StreamEvent and writes the
// resulting frames via the provided writeFrame callback.
func (r *Registry) streamEvents(threadID string, events <-chan tacklr.StreamEvent, hook StreamEventHandler, writeFrame func([]byte) error) error {
	for ev := range events {
		frames, err := hook(threadID, &ev)
		if err != nil {
			return fmt.Errorf("protocol hook: %w", err)
		}
		for _, f := range frames {
			if err := writeFrame(f); err != nil {
				return fmt.Errorf("write frame: %w", err)
			}
		}
	}
	return nil
}

// ListenAndServe starts an HTTP server with SSE and WebSocket endpoints that
// use the given protocol to format all streamed events.
func (r *Registry) ListenAndServe(addr string, protocol Protocol) error {
	hook := handlers[protocol]
	if hook == nil {
		return fmt.Errorf("unsupported protocol: %s", protocol)
	}
	validate := validators[protocol]
	if validate == nil {
		return fmt.Errorf("no request validator for protocol: %s", protocol)
	}

	mux := http.NewServeMux()
	if protocol == ProtocolACP {
		mux.HandleFunc("POST /", func(w http.ResponseWriter, req *http.Request) {
			r.HandleRPC(w, req, hook, validate)
		})
		return http.ListenAndServe(addr, mux)
	}

	mux.HandleFunc("POST /", func(w http.ResponseWriter, req *http.Request) {
		r.serveSSE(w, req, hook, validate)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, req *http.Request) {
		r.serveWS(w, req, hook, validate)
	})
	mux.HandleFunc("POST /resume", func(w http.ResponseWriter, req *http.Request) {
		r.serveSSE(w, req, hook, validate)
	})
	mux.HandleFunc("GET /resume", func(w http.ResponseWriter, req *http.Request) {
		r.serveWS(w, req, hook, validate)
	})
	return http.ListenAndServe(addr, mux)
}

// handleRPC dispatches JSON-RPC-style requests for ACP. Prompt and resume
// requests stream events through the protocol hook; session lifecycle methods
// are handled inline.
func (r *Registry) HandleRPC(w http.ResponseWriter, req *http.Request, hook StreamEventHandler, validate RequestValidator) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		slog.Error("failed to read request body", "error", err)
		r.writeJSONRPCError(w, nil, ErrInternal.Error())
		return
	}

	pr, err := validate(body)
	if err != nil {
		slog.Debug("acp client error", "error", err)
		// Notifications must never receive a response, including errors.
		if pr != nil && pr.Notification {
			return
		}
		var id json.RawMessage
		if pr != nil {
			id = pr.ID
		}
		r.writeJSONRPCError(w, id, err.Error())
		return
	}

	// JSON-RPC notifications: handle side effects, never write a response.
	if pr.Notification {
		if pr.Method == "session/cancel" && pr.ThreadID != "" {
			if c, ok := r.activeCtx.LoadAndDelete(pr.ThreadID); ok {
				c.(context.CancelFunc)()
			}
		} else {
			slog.Debug("ignored acp notification", "method", pr.Method)
		}
		return
	}

	switch pr.Method {
	case "session/prompt", "session/resume":
		r.runPromptTurn(w, req, pr, hook)
	case "initialize":
		r.writeJSONRPCResult(w, pr.ID, r.acpCapabilities())
	case "authenticate":
		r.writeJSONRPCResult(w, pr.ID, map[string]any{})
	case "session/new":
		threadID := uuid.New().String()
		configValues := map[string]string{}
		if r.defaultAgent != "" {
			configValues["agent"] = r.defaultAgent
		}
		r.sessions.Store(threadID, &sessionState{
			cwd:          pr.CWD,
			mcpServers:   pr.MCPServers,
			configValues: configValues,
		})
		r.writeJSONRPCResult(w, pr.ID, map[string]any{
			"sessionId":     threadID,
			"configOptions": r.buildConfigOptions(r.defaultAgent),
		})
	case "session/load":
		state, ok := r.sessions.Load(pr.ThreadID)
		if !ok {
			r.writeJSONRPCError(w, pr.ID, clientErrorf(ErrSessionNotFound, "session %q not found", pr.ThreadID).Error())
			return
		}
		sess := state.(*sessionState)
		agentID := r.sessionAgentID(sess)
		// No history replay yet; return session metadata only.
		r.writeJSONRPCResult(w, pr.ID, map[string]any{
			"sessionId":     pr.ThreadID,
			"configOptions": r.buildConfigOptions(agentID),
		})
	case "session/set_config_option":
		state, ok := r.sessions.Load(pr.ThreadID)
		if !ok {
			r.writeJSONRPCError(w, pr.ID, clientErrorf(ErrSessionNotFound, "session %q not found", pr.ThreadID).Error())
			return
		}
		sess := state.(*sessionState)
		if sess.configValues == nil {
			sess.configValues = map[string]string{}
		}
		switch pr.ConfigID {
		case "agent":
			if _, exists := r.agents[pr.ConfigValue]; !exists {
				r.writeJSONRPCError(w, pr.ID, clientErrorf(ErrAgentNotFound, "agent %q not found", pr.ConfigValue).Error())
				return
			}
			sess.configValues["agent"] = pr.ConfigValue
		default:
			r.writeJSONRPCError(w, pr.ID, clientErrorf(ErrInvalidRequest, "unknown configId %q", pr.ConfigID).Error())
			return
		}
		r.writeJSONRPCResult(w, pr.ID, map[string]any{
			"configOptions": r.buildConfigOptions(r.sessionAgentID(sess)),
		})
	case "session/close":
		r.sessions.Delete(pr.ThreadID)
		if c, ok := r.activeCtx.LoadAndDelete(pr.ThreadID); ok {
			c.(context.CancelFunc)()
		}
		r.writeJSONRPCResult(w, pr.ID, map[string]any{})
	case "session/cancel":
		if c, ok := r.activeCtx.LoadAndDelete(pr.ThreadID); ok {
			c.(context.CancelFunc)()
		}
		r.writeJSONRPCResult(w, pr.ID, map[string]any{})
	default:
		r.writeJSONRPCError(w, pr.ID, "method not found")
	}
}

// runPromptTurn is the shared prompt/resume execution path for ACP. It
// resolves the thread, loads the agent, runs the harness, and streams events
// through the protocol hook.
func (r *Registry) runPromptTurn(w http.ResponseWriter, req *http.Request, pr *parsedRequest, hook StreamEventHandler) {
	threadID := pr.ThreadID

	// Resolve session state and determine whether to load from store.
	// session/resume always loads; session/prompt only loads if the session
	// has already completed a turn (i.e. was persisted by checkpointSession).
	var sess *sessionState
	if state, ok := r.sessions.Load(threadID); ok {
		sess = state.(*sessionState)
	}

	agentID := r.sessionAgentID(sess)
	if agentID == "" {
		r.writeJSONRPCError(w, pr.ID, clientErrorf(ErrInvalidRequest, "no agent configured for session and no default agent configured").Error())
		return
	}

	load := false
	if pr.Method == "session/resume" {
		load = true
	} else if sess != nil {
		load = sess.prompted
		sess.prompted = true
	}

	h, _, err := r.loadAgent(req.Context(), agentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			r.writeJSONRPCError(w, pr.ID, err.Error())
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", agentID, "thread_id", threadID)
		r.writeJSONRPCError(w, pr.ID, ErrInternal.Error())
		return
	}
	defer h.Close()

	// Merge per-session MCP configs from session/new into the harness.
	if sess != nil && len(sess.mcpServers) > 0 {
		h.MCPConfigs = append(h.MCPConfigs, sess.mcpServers...)
	}

	ctx, cancel := context.WithCancel(req.Context())
	r.activeCtx.Store(threadID, cancel)
	defer func() {
		r.activeCtx.Delete(threadID)
		cancel()
	}()

	events, err := runHarness(ctx, h, pr)
	if err != nil {
		if IsClientError(err) {
			r.writeJSONRPCError(w, pr.ID, err.Error())
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", agentID, "thread_id", threadID)
		r.writeJSONRPCError(w, pr.ID, ErrInternal.Error())
		return
	}

	// Wrap the hook to inject the client's JSON-RPC request ID into
	// complete and error frames so the client can correlate responses.
	idHook := func(threadID string, ev *streaming.StreamEvent) ([][]byte, error) {
		frames, err := hook(threadID, ev)
		if err != nil {
			return nil, err
		}
		if ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError {
			for i, frame := range frames {
				var msg map[string]any
				if json.Unmarshal(frame, &msg) == nil {
					msg["id"] = pr.ID
					frames[i], _ = json.Marshal(msg)
				}
			}
		}
		return frames, nil
	}

	writeFrame := func(frame []byte) error {
		_, err := w.Write(frame)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte{'\n'})
		return err
	}
	if err := r.streamEvents(threadID, events, idHook, writeFrame); err != nil {
		slog.Warn("failed to stream ACP events", "error", err, "thread_id", threadID)
		return
	}
}

// acpCapabilities returns the capability advertisement for ACP initialization.
func (r *Registry) acpCapabilities() map[string]any {
	return map[string]any{
		"protocolVersion": 1,
		"agentCapabilities": map[string]any{
			// Full load requires conversation replay; advertise false until implemented.
			"loadSession": false,
			"promptCapabilities": map[string]any{
				"image":           false,
				"audio":           false,
				"embeddedContext": true,
			},
			"mcpCapabilities": map[string]any{
				"http": false,
				"sse":  false,
			},
			"sessionCapabilities": map[string]any{
				"close": struct{}{},
			},
		},
		"agentInfo": map[string]string{
			"name":    "tacklr",
			"title":   "Tacklr ACP",
			"version": "0.1.0",
		},
		"authMethods": []string{},
	}
}

func (r *Registry) writeJSONRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	data := map[string]any{"jsonrpc": "2.0", "id": id, "result": result}
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Warn("failed to write JSON-RPC result", "error", err)
	}
}

func (r *Registry) writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, msg string) {
	w.Header().Set("Content-Type", "application/json")
	data := map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": -32603, "message": msg},
	}
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Warn("failed to write JSON-RPC error", "error", err)
	}
}

// stdioResponseWriter adapts an io.Writer to http.ResponseWriter for stdio mode.
// Every Write is flushed when the underlying writer supports Sync.
type stdioResponseWriter struct {
	header http.Header
	w      io.Writer
}

func (w *stdioResponseWriter) Header() http.Header { return w.header }
func (w *stdioResponseWriter) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	if err == nil {
		if s, ok := w.w.(interface{ Sync() error }); ok {
			_ = s.Sync()
		}
	}
	return n, err
}
func (w *stdioResponseWriter) WriteHeader(int) {}

// ServeACPStdio reads line-delimited JSON-RPC ACP requests from stdin and
// writes responses to stdout. Each request is processed synchronously through
// the same HandleRPC dispatch as the HTTP mode.
func (r *Registry) ServeACPStdio() error {
	slog.Info("starting ACP stdio mode")
	return r.ServeACPIO(os.Stdin, os.Stdout)
}

// ServeACPIO is the testable core of ServeACPStdio: line-delimited JSON-RPC
// over arbitrary reader/writer streams.
func (r *Registry) ServeACPIO(in io.Reader, out io.Writer) error {
	reader := bufio.NewReader(in)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if trimmed := bytes.TrimRight(line, "\n\r"); len(trimmed) > 0 {
					r.dispatchACPLine(out, trimmed)
				}
				return nil
			}
			return fmt.Errorf("stdin read: %w", err)
		}
		line = bytes.TrimRight(line, "\n\r")
		if len(line) == 0 {
			continue
		}
		r.dispatchACPLine(out, line)
	}
}

func (r *Registry) dispatchACPLine(out io.Writer, body []byte) {
	req := &http.Request{
		Method: "POST",
		Body:   io.NopCloser(bytes.NewReader(body)),
		Header: http.Header{},
	}
	w := &stdioResponseWriter{header: http.Header{}, w: out}
	r.HandleRPC(w, req, handlers[ProtocolACP], validators[ProtocolACP])
}

// serveSSE is the common SSE handler for both prompt and resume turns.
func (r *Registry) serveSSE(w http.ResponseWriter, req *http.Request, hook StreamEventHandler, validate RequestValidator) {
	if !acceptsSSE(req) {
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

	body, err := io.ReadAll(req.Body)
	if err != nil {
		slog.Error("failed to read request body", "error", err)
		if werr := writeSSEError(w, flusher, ErrInternal.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}

	pr, err := validate(body)
	if err != nil {
		slog.Debug("sse client error", "error", err)
		if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(pr)
	h, _, err := r.loadAgent(req.Context(), pr.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			slog.Debug("sse client error", "error", err)
			if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
				slog.Warn("failed to write SSE error", "error", werr)
			}
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", pr.AgentID, "thread_id", threadID)
		if werr := writeSSEError(w, flusher, ErrInternal.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}
	defer h.Close()

	w.Header().Set("X-Thread-ID", threadID)
	threadData, err := json.Marshal(threadEvent{ThreadID: threadID})
	if err != nil {
		slog.Error("failed to marshal thread event", "error", err, "thread_id", threadID)
		if werr := writeSSEError(w, flusher, ErrInternal.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}
	if err := writeSSEEvent(w, flusher, "thread", threadData); err != nil {
		slog.Warn("failed to write thread event", "error", err, "thread_id", threadID)
		return
	}

	events, err := runHarness(req.Context(), h, pr)
	if err != nil {
		if IsClientError(err) {
			slog.Debug("sse client error", "error", err)
			if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
				slog.Warn("failed to write SSE error", "error", werr)
			}
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", pr.AgentID, "thread_id", threadID)
		if werr := writeSSEError(w, flusher, ErrInternal.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}

	writeFrame := func(frame []byte) error {
		var holder struct {
			Type string `json:"type"`
		}
		eventType := "message"
		if err := json.Unmarshal(frame, &holder); err == nil && holder.Type != "" {
			eventType = holder.Type
		}
		return writeSSEEvent(w, flusher, eventType, frame)
	}
	if err := r.streamEvents(threadID, events, hook, writeFrame); err != nil {
		slog.Warn("failed to stream SSE events", "error", err, "thread_id", threadID)
		return
	}
}

// serveWS is the common WebSocket handler for both prompt and resume turns.
func (r *Registry) serveWS(w http.ResponseWriter, req *http.Request, hook StreamEventHandler, validate RequestValidator) {
	c, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		slog.Warn("websocket accept failed", "error", err)
		return
	}
	defer func() {
		if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
			slog.Debug("failed to close websocket cleanly", "error", err)
		}
	}()

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	var raw json.RawMessage
	if err := wsjson.Read(ctx, c, &raw); err != nil {
		slog.Debug("failed to read websocket message", "error", err)
		if werr := writeWSClientError(ctx, c, clientErrorf(ErrInvalidRequest, "failed to read message: %v", err)); werr != nil {
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

	pr, err := validate(raw)
	if err != nil {
		if werr := writeWSClientError(ctx, c, err); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(pr)
	h, _, err := r.loadAgent(ctx, pr.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			if werr := writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", pr.AgentID, "thread_id", threadID)
		if werr := writeWSInternalError(ctx, c); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}
	defer h.Close()

	if err := writeWSJSON(ctx, c, wsServerEvent{Type: "thread", Content: threadID}); err != nil {
		slog.Warn("failed to write thread event to websocket", "error", err, "thread_id", threadID)
		return
	}

	events, err := runHarness(ctx, h, pr)
	if err != nil {
		if IsClientError(err) {
			if werr := writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", pr.AgentID, "thread_id", threadID)
		if werr := writeWSInternalError(ctx, c); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	writeFrame := func(frame []byte) error {
		return c.Write(ctx, websocket.MessageText, frame)
	}
	if err := r.streamEvents(threadID, events, hook, writeFrame); err != nil {
		slog.Warn("failed to stream websocket events", "error", err, "thread_id", threadID)
		return
	}
}
