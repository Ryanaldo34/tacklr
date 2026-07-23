package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Server serves a Registry over transports using a fixed Protocol.
type Server struct {
	Registry *Registry
	Protocol Protocol
}

// NewServer wraps a Registry and Protocol for serving over transports.
func NewServer(r *Registry, p Protocol) *Server {
	if r == nil {
		panic("server: Registry is required")
	}
	if p == nil {
		panic("server: Protocol is required")
	}
	return &Server{Registry: r, Protocol: p}
}

// ServeStdio serves line-delimited JSON messages over in/out (typically
// os.Stdin/os.Stdout). It returns when in reaches EOF or ctx is cancelled.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	reader := bufio.NewReader(in)
	w := &lineMessageWriter{w: out}

	type readResult struct {
		line []byte
		err  error
	}
	// Buffered so a completed read is not lost if the main loop is between
	// selects when the reader exits on ctx cancel.
	readCh := make(chan readResult, 1)

	go func() {
		defer close(readCh)
		for {
			line, err := reader.ReadBytes('\n')
			select {
			case readCh <- readResult{line: line, err: err}:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rr, ok := <-readCh:
			if !ok {
				return ctx.Err()
			}
			if rr.err != nil {
				if errors.Is(rr.err, io.EOF) {
					if trimmed := bytes.TrimRight(rr.line, "\n\r"); len(trimmed) > 0 {
						wg.Add(1)
						go func(body []byte) {
							defer wg.Done()
							s.HandleMessage(ctx, body, w)
						}(trimmed)
					}
					wg.Wait()
					return nil
				}
				return fmt.Errorf("stdio read: %w", rr.err)
			}
			line := bytes.TrimRight(rr.line, "\n\r")
			if len(line) == 0 {
				continue
			}
			wg.Add(1)
			go func(body []byte) {
				defer wg.Done()
				s.HandleMessage(ctx, body, w)
			}(line)
		}
	}
}

// ServeHTTP starts an HTTP server. Routes depend on Protocol.HTTPMode().
// It returns when the server fails or ctx is cancelled (graceful shutdown).
func (s *Server) ServeHTTP(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	mux := http.NewServeMux()
	switch s.Protocol.HTTPMode() {
	case HTTPModeRPC:
		mux.HandleFunc("POST /", s.serveHTTPRPC)
	case HTTPModeStream:
		mux.HandleFunc("POST /", s.serveHTTPSSE)
		mux.HandleFunc("GET /", s.ServeWS)
		mux.HandleFunc("POST /resume", s.serveHTTPSSE)
		mux.HandleFunc("GET /resume", s.ServeWS)
	default:
		return fmt.Errorf("unsupported HTTP mode: %v", s.Protocol.HTTPMode())
	}

	hs := &http.Server{Addr: addr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		errCh <- hs.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutdownCtx)
		err := <-errCh
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return ctx.Err()
		}
		return err
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// ServeWS upgrades the connection and handles a single turn over WebSocket.
func (s *Server) ServeWS(w http.ResponseWriter, req *http.Request) {
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

	// Discard further client messages; cancel if the client disconnects.
	go func() {
		defer cancel()
		for {
			_, _, err := c.Read(ctx)
			if err != nil {
				return
			}
		}
	}()

	pr, err := s.Protocol.Parse(raw)
	if err != nil {
		if werr := writeWSClientError(ctx, c, err); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(pr)
	stream, err := s.Registry.RunTurn(ctx, TurnRequest{
		AgentID:   pr.AgentID,
		ThreadID:  threadID,
		Prompt:    pr.Prompt,
		Responses: pr.Responses,
		Load:      load,
	})
	if err != nil {
		if IsClientError(err) {
			if werr := writeWSClientError(ctx, c, err); werr != nil {
				slog.Warn("failed to write websocket error", "error", werr)
			}
			return
		}
		logTurnError(err, pr.AgentID, threadID)
		if werr := writeWSInternalError(ctx, c); werr != nil {
			slog.Warn("failed to write websocket error", "error", werr)
		}
		return
	}
	defer stream.Cancel()

	if err := writeWSJSON(ctx, c, wsServerEvent{Type: "thread", Content: threadID}); err != nil {
		slog.Warn("failed to write thread event to websocket", "error", err, "thread_id", threadID)
		return
	}

	mw := &wsMessageWriter{ctx: ctx, c: c}
	if err := s.streamTurn(ctx, threadID, stream, mw, nil); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("failed to stream websocket events", "error", err, "thread_id", threadID)
		}
	}
}

// HandleMessage is the shared dispatch path for RPC-style protocols (stdio/HTTP).
// It is the primary unit under test for lifecycle and prompt turns.
func (s *Server) HandleMessage(ctx context.Context, body []byte, w MessageWriter) {
	if err := ctx.Err(); err != nil {
		_ = w.WriteError(nil, err)
		return
	}

	pr, err := s.Protocol.Parse(body)
	if err != nil {
		slog.Debug("client error", "error", err)
		if pr != nil && pr.Notification {
			return
		}
		var id json.RawMessage
		if pr != nil {
			id = pr.ID
		}
		_ = w.WriteError(id, err)
		return
	}

	if pr.Notification {
		if pr.Method == "session/cancel" && pr.ThreadID != "" {
			s.Registry.CancelSession(pr.ThreadID)
		} else {
			slog.Debug("ignored notification", "method", pr.Method)
		}
		return
	}

	// Stream-style protocols (no Method) run a direct agent turn.
	if pr.Method == "" {
		s.handleDirectTurn(ctx, pr, w)
		return
	}

	switch pr.Method {
	case "session/prompt", "session/resume":
		s.handleSessionTurn(ctx, pr, w)
	case "initialize":
		_ = w.WriteResult(pr.ID, s.Registry.Capabilities())
	case "authenticate":
		_ = w.WriteResult(pr.ID, map[string]any{})
	case "session/new":
		view := s.Registry.CreateSession(pr.CWD, pr.MCPServers)
		_ = w.WriteResult(pr.ID, map[string]any{
			"sessionId":     view.SessionID,
			"configOptions": view.ConfigOptions,
		})
	case "session/load":
		view, err := s.Registry.GetSession(pr.ThreadID)
		if err != nil {
			_ = w.WriteError(pr.ID, err)
			return
		}
		_ = w.WriteResult(pr.ID, map[string]any{
			"sessionId":     view.SessionID,
			"configOptions": view.ConfigOptions,
		})
	case "session/set_config_option":
		view, err := s.Registry.SetConfigOption(pr.ThreadID, pr.ConfigID, pr.ConfigValue)
		if err != nil {
			_ = w.WriteError(pr.ID, err)
			return
		}
		_ = w.WriteResult(pr.ID, map[string]any{
			"configOptions": view.ConfigOptions,
		})
	case "session/close":
		s.Registry.CloseSession(pr.ThreadID)
		_ = w.WriteResult(pr.ID, map[string]any{})
	case "session/cancel":
		s.Registry.CancelSession(pr.ThreadID)
		_ = w.WriteResult(pr.ID, map[string]any{})
	default:
		_ = w.WriteError(pr.ID, clientErrorf(ErrMethodNotFound, "method not found"))
	}
}

func (s *Server) handleSessionTurn(ctx context.Context, pr *parsedRequest, w MessageWriter) {
	stream, err := s.Registry.RunTurn(ctx, TurnRequest{
		SessionID: pr.ThreadID,
		Prompt:    pr.Prompt,
		Responses: pr.Responses,
		Resume:    pr.Method == "session/resume",
	})
	if err != nil {
		if !IsClientError(err) {
			logTurnError(err, pr.AgentID, pr.ThreadID)
		}
		_ = w.WriteError(pr.ID, err)
		return
	}
	defer stream.Cancel()

	if err := s.streamTurn(ctx, pr.ThreadID, stream, w, pr.ID); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("failed to stream events", "error", err, "thread_id", pr.ThreadID)
		}
	}
}

func (s *Server) handleDirectTurn(ctx context.Context, pr *parsedRequest, w MessageWriter) {
	threadID, load := resolveThread(pr)
	stream, err := s.Registry.RunTurn(ctx, TurnRequest{
		AgentID:   pr.AgentID,
		ThreadID:  threadID,
		Prompt:    pr.Prompt,
		Responses: pr.Responses,
		Load:      load,
	})
	if err != nil {
		if !IsClientError(err) {
			logTurnError(err, pr.AgentID, threadID)
		}
		_ = w.WriteError(nil, err)
		return
	}
	defer stream.Cancel()

	if err := s.streamTurn(ctx, threadID, stream, w, nil); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("failed to stream events", "error", err, "thread_id", threadID)
		}
	}
}

// streamTurn encodes harness events via Protocol and writes frames.
// If reqID is non-nil, it is injected into complete/error frames (ACP correlation).
// When the turn ends without a complete/error frame (session/cancel or ctx cancel),
// a result with stopReason "cancelled" is written for the original request id.
func (s *Server) streamTurn(ctx context.Context, threadID string, stream *EventStream, w MessageWriter, reqID json.RawMessage) error {
	finished := false
	writeCancelled := func() {
		if finished || len(reqID) == 0 {
			return
		}
		finished = true
		// ACP: cancelled prompt turns complete with stopReason "cancelled".
		_ = w.WriteResult(reqID, map[string]string{"stopReason": "cancelled"})
	}

	for {
		select {
		case <-ctx.Done():
			stream.Cancel()
			// Drain remaining events briefly so we do not race with a natural complete.
			for {
				select {
				case ev, ok := <-stream.Events:
					if !ok {
						writeCancelled()
						return ctx.Err()
					}
					if err := s.writeStreamEvent(threadID, &ev, w, reqID, &finished); err != nil {
						return err
					}
					if finished {
						return ctx.Err()
					}
				default:
					writeCancelled()
					return ctx.Err()
				}
			}
		case ev, ok := <-stream.Events:
			if !ok {
				writeCancelled()
				return nil
			}
			if ev.Type == streaming.StreamEventComplete && len(reqID) > 0 && s.Registry.WasCancelled(threadID) {
				writeCancelled()
				continue
			}
			if err := s.writeStreamEvent(threadID, &ev, w, reqID, &finished); err != nil {
				return err
			}
		}
	}
}

func (s *Server) writeStreamEvent(threadID string, ev *streaming.StreamEvent, w MessageWriter, reqID json.RawMessage, finished *bool) error {
	if ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError {
		*finished = true
	}
	frames, err := s.Protocol.EncodeEvent(threadID, ev)
	if err != nil {
		return fmt.Errorf("protocol encode: %w", err)
	}
	if len(reqID) > 0 && (ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError) {
		for i, frame := range frames {
			var msg map[string]any
			if json.Unmarshal(frame, &msg) == nil {
				msg["id"] = reqID
				frames[i], _ = json.Marshal(msg)
			}
		}
	}
	for _, f := range frames {
		if err := w.WriteFrame(f); err != nil {
			return fmt.Errorf("write frame: %w", err)
		}
	}
	return nil
}

func (s *Server) serveHTTPRPC(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		slog.Error("failed to read request body", "error", err)
		mw := &jsonRPCMessageWriter{w: w}
		_ = mw.WriteError(nil, fmt.Errorf("read body: %w", err))
		return
	}
	s.HandleMessage(req.Context(), body, &jsonRPCMessageWriter{w: w})
}

func (s *Server) serveHTTPSSE(w http.ResponseWriter, req *http.Request) {
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

	pr, err := s.Protocol.Parse(body)
	if err != nil {
		slog.Debug("sse client error", "error", err)
		if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}

	threadID, load := resolveThread(pr)
	stream, err := s.Registry.RunTurn(req.Context(), TurnRequest{
		AgentID:   pr.AgentID,
		ThreadID:  threadID,
		Prompt:    pr.Prompt,
		Responses: pr.Responses,
		Load:      load,
	})
	if err != nil {
		if IsClientError(err) {
			slog.Debug("sse client error", "error", err)
			if werr := writeSSEError(w, flusher, err.Error()); werr != nil {
				slog.Warn("failed to write SSE error", "error", werr)
			}
			return
		}
		logTurnError(err, pr.AgentID, threadID)
		if werr := writeSSEError(w, flusher, ErrInternal.Error()); werr != nil {
			slog.Warn("failed to write SSE error", "error", werr)
		}
		return
	}
	defer stream.Cancel()

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

	mw := &sseMessageWriter{w: w, flusher: flusher}
	if err := s.streamTurn(req.Context(), threadID, stream, mw, nil); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("failed to stream SSE events", "error", err, "thread_id", threadID)
		}
	}
}

