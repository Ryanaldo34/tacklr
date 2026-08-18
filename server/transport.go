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

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

// Server serves a Registry over one or more wire Protocols.
type Server struct {
	Registry  *Registry
	Protocols []Protocol
	// Client is set for the active stdio connection (outbound Agent→Client RPC).
	// Prefer Conn.RPC inside protocol handlers; this field supports demux on stdio.
	Client *ClientBridge
	// Connections tracks ACP WebSocket (and future Streamable HTTP) connections.
	Connections *ConnectionRegistry
	// Security is protocol-neutral authentication and authorization supplied by the host.
	Security *tacklrsecurity.Service
	// HTTPAttempt translates request credentials at the HTTP transport edge.
	HTTPAttempt HTTPAttemptExtractor

	allowAnonymousNetwork   bool
	networkPolicyConfigured bool
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
	connections := NewConnectionRegistry()
	connections.onRemove = func(connection *Connection) {
		if r.vfsAuth == nil {
			return
		}
		for _, sessionID := range connection.sessionIDs() {
			r.vfsAuth.Clear(sessionID)
		}
	}
	return &Server{
		Registry:    r,
		Protocols:   protocols,
		Connections: connections,
	}
}

type stdioReadResult struct {
	line []byte
	err  error
}

// ServeStdio serves line-delimited JSON messages over in/out.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	reader := bufio.NewReader(in)
	w := &lineMessageWriter{w: out}
	bridge := NewClientBridge(w)
	s.Client = bridge
	defer func() { s.Client = nil }()

	proto := s.Protocols[0]
	localPrincipal, err := tacklrsecurity.NewPrincipal("stdio")
	if err != nil {
		panic(err)
	}
	localSecurity := tacklrsecurity.Context{
		Principal: localPrincipal,
		Binding:   tacklrsecurity.ChannelBinding{Kind: "stdio", ID: "process"},
	}

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
	// initialize runs synchronously so later methods on the same input see it.
	dispatch := func(body []byte) {
		run := func() {
			reqConn := &Conn{
				Writer:   w,
				RPC:      bridge,
				Security: &localSecurity,
			}
			reqEnv := ProtocolEnv{Registry: s.Registry, Conn: reqConn, Security: s.Security}
			if err := proto.HandleInbound(ctx, reqEnv, body); err != nil {
				slog.Debug("inbound handler", "error", err, "protocol", proto.Name())
			}
		}
		if peek, err := peekJSONRPC(body); err == nil && peek.Method == "initialize" {
			run()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}

	return runStdioLoop(ctx, readCh, bridge, dispatch, &wg, bridge.Close)
}

// runStdioLoop is the ServeStdio select loop. Extracted so the readCh-closed
// path can be tested without racing the real reader goroutine.
func runStdioLoop(
	ctx context.Context,
	readCh <-chan stdioReadResult,
	bridge *ClientBridge,
	dispatch func([]byte),
	wg *sync.WaitGroup,
	onClose context.CancelFunc,
) error {
	if onClose == nil {
		onClose = func() {}
	}
	for {
		select {
		case <-ctx.Done():
			onClose()
			return ctx.Err()
		case rr, ok := <-readCh:
			if !ok {
				// Reader exited without a final result (typically parent cancel).
				onClose()
				return ctx.Err()
			}
			if rr.err != nil {
				if errors.Is(rr.err, io.EOF) {
					if trimmed := bytes.TrimRight(rr.line, "\n\r"); len(trimmed) > 0 {
						if !bridge.TryCompleteResponse(trimmed) {
							dispatch(trimmed)
						}
					}
					onClose()
					wg.Wait()
					return nil
				}
				onClose()
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
				securityContext, status := s.networkContext(req.Context(), req, r.AllowUnauthenticated)
				if status != 0 {
					http.Error(w, http.StatusText(status), status)
					return
				}
				env := ProtocolEnv{
					Registry:    s.Registry,
					Conn:        &Conn{Security: securityContext},
					Security:    s.Security,
					Connections: s.Connections,
				}
				r.Handler(env, w, req)
			})
		}
	}
	return mux
}

// ServeHTTP starts an HTTP server mounting all protocol routes.
func (s *Server) ServeHTTP(ctx context.Context, addr string) error {
	if !s.networkPolicyConfigured {
		return ErrNetworkSecurityPolicyRequired
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
	env := ProtocolEnv{Registry: s.Registry, Conn: conn}
	if err := s.Protocols[0].HandleInbound(ctx, env, body); err != nil {
		slog.Debug("HandleMessage", "error", err)
	}
}
