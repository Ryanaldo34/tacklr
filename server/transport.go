package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Server serves a Registry over one or more wire Protocols.
type Server struct {
	Registry  *Registry
	Protocols []Protocol
	// Client is set for the active stdio connection (outbound Agent→Client RPC).
	// Prefer Conn.RPC inside protocol handlers; this field supports demux on stdio.
	Client *ClientBridge
}

// NewServer wraps a Registry and one or more protocols.
// The first protocol is used for connection-oriented transports (stdio).
func NewServer(r *Registry, protocols ...Protocol) *Server {
	if r == nil {
		panic("server: Registry is required")
	}
	if len(protocols) == 0 {
		panic("server: at least one Protocol is required")
	}
	return &Server{Registry: r, Protocols: protocols}
}

// primary is the connection-oriented protocol (first registered).
func (s *Server) primary() Protocol {
	return s.Protocols[0]
}

type stdioReadResult struct {
	line []byte
	err  error
}

// ServeStdio serves line-delimited JSON messages over in/out.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	reader := bufio.NewReader(in)
	w := &lineMessageWriter{w: out}
	bridge := NewClientBridge(w)
	s.Client = bridge
	defer func() { s.Client = nil }()

	proto := s.primary()

	readCh := make(chan stdioReadResult, 1)

	go func() {
		defer close(readCh)
		for {
			line, err := reader.ReadBytes('\n')
			select {
			case readCh <- stdioReadResult{line: line, err: err}:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	// Each inbound line gets its own Conn so concurrent handlers do not race on
	// Caps. Capabilities are loaded from / stored on the bridge under its mutex.
	dispatch := func(body []byte) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqConn := &Conn{
				Writer: w,
				RPC:    bridge,
				Caps:   bridge.GetCaps(),
			}
			reqEnv := ProtocolEnv{Registry: s.Registry, Conn: reqConn}
			if err := proto.HandleInbound(ctx, reqEnv, body); err != nil {
				slog.Debug("inbound handler", "error", err, "protocol", proto.Name())
			}
		}()
	}

	return runStdioLoop(ctx, readCh, bridge, dispatch, &wg)
}

// runStdioLoop is the ServeStdio select loop. Extracted so the readCh-closed
// path can be tested without racing the real reader goroutine.
func runStdioLoop(
	ctx context.Context,
	readCh <-chan stdioReadResult,
	bridge *ClientBridge,
	dispatch func([]byte),
	wg *sync.WaitGroup,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rr, ok := <-readCh:
			if !ok {
				// Reader exited without a final result (typically parent cancel).
				return ctx.Err()
			}
			if rr.err != nil {
				if errors.Is(rr.err, io.EOF) {
					if trimmed := bytes.TrimRight(rr.line, "\n\r"); len(trimmed) > 0 {
						if !bridge.TryCompleteResponse(trimmed) {
							dispatch(trimmed)
						}
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
			if bridge.TryCompleteResponse(line) {
				continue
			}
			dispatch(line)
		}
	}
}

// HTTPMux mounts all protocol HTTP routes. Used by ServeHTTP and tests.
func (s *Server) HTTPMux() *http.ServeMux {
	mux := http.NewServeMux()
	for _, p := range s.Protocols {
		for _, route := range p.HTTPRoutes() {
			r := route
			pattern := r.Method + " " + r.Pattern
			mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
				env := ProtocolEnv{Registry: s.Registry, Conn: &Conn{}}
				r.Handler(env, w, req)
			})
		}
	}
	return mux
}

// ServeHTTP starts an HTTP server mounting all protocol routes.
func (s *Server) ServeHTTP(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	hs := &http.Server{Addr: addr, Handler: s.HTTPMux(), ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		errCh <- hs.ListenAndServe()
	}()
	return waitHTTPServer(ctx, hs.Shutdown, errCh)
}

// waitHTTPServer waits for ListenAndServe to exit or ctx cancel, then maps
// shutdown/listen errors to the ServeHTTP return value. Extracted for tests.
func waitHTTPServer(ctx context.Context, shutdown func(context.Context) error, errCh <-chan error) error {
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultHTTPShutdown)
		defer cancel()
		_ = shutdown(shutdownCtx)
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

// HandleMessage dispatches one inbound body on the primary protocol.
// Used by tests and unary HTTP adapters.
func (s *Server) HandleMessage(ctx context.Context, body []byte, w MessageWriter) {
	conn := &Conn{Writer: w, RPC: s.Client}
	if s.Client != nil {
		conn.Caps = s.Client.GetCaps()
		conn.RPC = s.Client
	}
	env := ProtocolEnv{Registry: s.Registry, Conn: conn}
	if err := s.primary().HandleInbound(ctx, env, body); err != nil {
		slog.Debug("HandleMessage", "error", err)
	}
}

// serveHTTPRPC is a test/helper entry for ACP unary HTTP (POST /).
func (s *Server) serveHTTPRPC(w http.ResponseWriter, req *http.Request) {
	env := ProtocolEnv{Registry: s.Registry, Conn: &Conn{}}
	for _, p := range s.Protocols {
		if p.Name() == "acp" {
			for _, route := range p.HTTPRoutes() {
				if route.Method == http.MethodPost && route.Pattern == "/" {
					route.Handler(env, w, req)
					return
				}
			}
		}
	}
	http.Error(w, "acp protocol not registered", http.StatusInternalServerError)
}

// serveHTTPSSE is a test/helper entry for SSE POST handlers.
func (s *Server) serveHTTPSSE(w http.ResponseWriter, req *http.Request) {
	env := ProtocolEnv{Registry: s.Registry, Conn: &Conn{}}
	path := req.URL.Path
	if path == "" {
		path = "/"
	}
	for _, p := range s.Protocols {
		if p.Name() != "sse" {
			continue
		}
		for _, route := range p.HTTPRoutes() {
			if route.Method == http.MethodPost && route.Pattern == path {
				route.Handler(env, w, req)
				return
			}
		}
	}
	// Fallback: try POST /
	for _, p := range s.Protocols {
		if p.Name() != "sse" {
			continue
		}
		for _, route := range p.HTTPRoutes() {
			if route.Method == http.MethodPost && route.Pattern == "/" {
				route.Handler(env, w, req)
				return
			}
		}
	}
	http.Error(w, "sse protocol not registered", http.StatusInternalServerError)
}
