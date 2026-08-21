package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr/streaming"
)

// sseProtocol implements Protocol for the native SSE/WebSocket API.
type sseProtocol struct{}

// SSEProtocol returns the SSE/WS wire protocol module.
func SSEProtocol() Protocol { return sseProtocol{} }

func (sseProtocol) Name() string { return "sse" }

func (sseProtocol) HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error {
	// SSE is HTTP-oriented; connection-oriented inbound is unused.
	return nil
}

func (p sseProtocol) HTTPRoutes() []HTTPRoute {
	return []HTTPRoute{
		{Method: http.MethodPost, Pattern: "/{$}", Handler: p.handleSSE},
		{Method: http.MethodPost, Pattern: "/resume", Handler: p.handleSSE},
		{Method: http.MethodGet, Pattern: "/{$}", Handler: p.handleWS},
		{Method: http.MethodGet, Pattern: "/resume", Handler: p.handleWS},
	}
}

func (p sseProtocol) handleSSE(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		http.Error(w, "Accept: text/event-stream required", http.StatusNotAcceptable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, ErrStreamingNotSupported.Error(), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	pr, err := validateSSERequest(body)
	if err != nil {
		mw := &sseMessageWriter{w: w, flusher: flusher}
		writeWireError(mw, nil, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if err := p.runSSEProtocolTurn(r.Context(), env, pr, func(threadID string) MessageWriter {
		w.Header().Set("X-Thread-ID", threadID)
		flusher.Flush()
		threadData, _ := json.Marshal(threadEvent{ThreadID: threadID})
		_ = writeSSEEvent(w, flusher, "thread", threadData)
		return &sseMessageWriter{w: w, flusher: flusher}
	}, func(err error) {
		flusher.Flush()
		mw := &sseMessageWriter{w: w, flusher: flusher}
		writeWireError(mw, nil, err)
	}); err != nil && !IsClientError(err) {
		slog.Debug("sse turn stream ended", "error", err)
	}
}

func (p sseProtocol) handleWS(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		slog.Warn("websocket accept failed", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	// First message is the turn request body.
	_, data, err := c.Read(ctx)
	if err != nil {
		return
	}
	pr, err := validateSSERequest(data)
	if err != nil {
		_ = wsWriteJSON(ctx, c, presentationError(err))
		return
	}

	_ = p.runSSEProtocolTurn(ctx, env, pr, func(threadID string) MessageWriter {
		_ = wsWriteJSON(ctx, c, presentationEvent{Type: "thread", Content: threadID})
		return &wsMessageWriter{ctx: ctx, c: c}
	}, func(err error) {
		_ = wsWriteJSON(ctx, c, presentationError(err))
	})
}

func (p sseProtocol) runSSEProtocolTurn(
	ctx context.Context,
	env ProtocolEnv,
	pr *parsedRequest,
	ready func(threadID string) MessageWriter,
	onStartError func(error),
) error {
	threadID, load := resolveThread(pr)
	req := TurnRequest{
		AgentID:   pr.AgentID,
		ThreadID:  threadID,
		Prompt:    pr.Prompt,
		Responses: pr.Responses,
		Load:      load,
	}
	stream, err := env.Registry.RunTurn(ctx, req)
	if err != nil {
		if !IsClientError(err) {
			logTurnError(err, pr.AgentID, threadID)
		}
		if onStartError != nil {
			onStartError(err)
		}
		return err
	}
	defer func() {
		stream.Cancel()
		stream.Close()
	}()
	if stream.SessionID() != "" {
		threadID = stream.SessionID()
	}
	env.Conn = &Conn{Writer: ready(threadID)}
	return runTurnStream(ctx, env, p, threadID, stream, nil)
}

func (p sseProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	presented, err := presentStreamEvent(ev)
	if err != nil {
		return StreamControl{Err: err, Finished: true}
	}
	data, err := json.Marshal(presented)
	if err != nil {
		return StreamControl{Err: err, Finished: true}
	}
	frames := [][]byte{data}
	terminal := ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError
	return StreamControl{Frames: frames, Finished: terminal}
}

func (p sseProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

func (sseProtocol) CreateSession(context.Context, ProtocolEnv, json.RawMessage) (string, any, error) {
	return "", nil, ErrWireSessionUnsupported
}

func (sseProtocol) LoadSession(context.Context, ProtocolEnv, string, json.RawMessage) (any, error) {
	return nil, ErrWireSessionUnsupported
}

func (sseProtocol) BindTurn(context.Context, ProtocolEnv, string, string, json.RawMessage) (TurnRequest, error) {
	return TurnRequest{}, ErrWireSessionUnsupported
}

func (sseProtocol) CloseSession(context.Context, ProtocolEnv, string) error {
	return ErrWireSessionUnsupported
}
