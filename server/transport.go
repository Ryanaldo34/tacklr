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

	conn := &Conn{Writer: w, RPC: bridge}
	env := ProtocolEnv{Registry: s.Registry, Conn: conn}
	proto := s.primary()

	type readResult struct {
		line []byte
		err  error
	}
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
	dispatch := func(body []byte) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Keep Conn caps/RPC in sync if initialize updated bridge caps.
			conn.Caps = bridge.Caps
			if err := proto.HandleInbound(ctx, env, body); err != nil {
				slog.Debug("inbound handler", "error", err, "protocol", proto.Name())
			}
		}()
	}

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

// ServeHTTP starts an HTTP server mounting all protocol routes.
func (s *Server) ServeHTTP(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	mux := http.NewServeMux()
	for _, p := range s.Protocols {
		proto := p
		for _, route := range proto.HTTPRoutes() {
			r := route
			pattern := r.Method + " " + r.Pattern
			mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
				env := ProtocolEnv{Registry: s.Registry, Conn: &Conn{}}
				r.Handler(env, w, req)
			})
		}
	}

	hs := &http.Server{Addr: addr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		errCh <- hs.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultHTTPShutdown)
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

// HandleMessage dispatches one inbound body on the primary protocol.
// Used by tests and unary HTTP adapters.
func (s *Server) HandleMessage(ctx context.Context, body []byte, w MessageWriter) {
	conn := &Conn{Writer: w, RPC: s.Client}
	if s.Client != nil {
		conn.Caps = s.Client.Caps
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
				if route.Method == "POST" && route.Pattern == "/" {
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
			if route.Method == "POST" && route.Pattern == path {
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
			if route.Method == "POST" && route.Pattern == "/" {
				route.Handler(env, w, req)
				return
			}
		}
	}
	http.Error(w, "sse protocol not registered", http.StatusInternalServerError)
}
