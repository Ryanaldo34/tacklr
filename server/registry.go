package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
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

type Registry struct {
	agents map[string]AgentSpec
	store  stores.BaseStore
}

func NewRegistry(store stores.BaseStore) *Registry {
	return &Registry{agents: make(map[string]AgentSpec), store: store}
}

func (r *Registry) Register(agentID string, spec AgentSpec) {
	r.agents[agentID] = spec
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
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", func(w http.ResponseWriter, req *http.Request) {
		r.serveSSE(w, req, false, hook)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, req *http.Request) {
		r.serveWS(w, req, false, hook)
	})
	mux.HandleFunc("POST /resume", func(w http.ResponseWriter, req *http.Request) {
		r.serveSSE(w, req, true, hook)
	})
	mux.HandleFunc("GET /resume", func(w http.ResponseWriter, req *http.Request) {
		r.serveWS(w, req, true, hook)
	})
	return http.ListenAndServe(addr, mux)
}

// serveSSE is the common SSE handler for both prompt and resume turns.
func (r *Registry) serveSSE(w http.ResponseWriter, req *http.Request, resume bool, hook StreamEventHandler) {
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

	var turnReq turnRequest
	if err := json.Unmarshal(body, &turnReq); err != nil {
		slog.Debug("sse client error", "error", err)
		if werr := writeSSEError(w, flusher, clientErrorf(ErrInvalidRequest, "invalid JSON: %v", err).Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}
	if err := validateRequest(turnReq, resume); err != nil {
		slog.Debug("sse client error", "error", err)
		if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(turnReq, resume)
	h, _, err := r.loadAgent(req.Context(), turnReq.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			slog.Debug("sse client error", "error", err)
			if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
				slog.Warn("failed to write SSE error", "error", werr)
			}
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", turnReq.AgentID, "thread_id", threadID)
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

	events, err := runHarness(req.Context(), h, turnReq, resume)
	if err != nil {
		if IsClientError(err) {
			slog.Debug("sse client error", "error", err)
			if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
				slog.Warn("failed to write SSE error", "error", werr)
			}
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", turnReq.AgentID, "thread_id", threadID)
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
func (r *Registry) serveWS(w http.ResponseWriter, req *http.Request, resume bool, hook StreamEventHandler) {
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

	var turnReq turnRequest
	if err := wsjson.Read(ctx, c, &turnReq); err != nil {
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

	if err := validateRequest(turnReq, resume); err != nil {
		if werr := writeWSClientError(ctx, c, err); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(turnReq, resume)
	h, _, err := r.loadAgent(ctx, turnReq.AgentID, threadID, load)
	if err != nil {
		if IsClientError(err) {
			if werr := writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("failed to load agent", "error", err, "agent_id", turnReq.AgentID, "thread_id", threadID)
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

	events, err := runHarness(ctx, h, turnReq, resume)
	if err != nil {
		if IsClientError(err) {
			if werr := writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		slog.Error("agent run failed", "error", err, "agent_id", turnReq.AgentID, "thread_id", threadID)
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
