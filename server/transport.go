package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const (
	defaultHTTPShutdown = 5 * time.Second
	// maxFrame is the largest frame a WebSocket client may send.
	maxFrame = 32 << 20
)

// Server serves a session.Client to the protocols' clients. On net/http,
// ServeHTTP (or Serve, or HTTPMux) accepts each connection as a WebSocket.
// A platform that holds the socket itself calls Connect, then
// Connection.Serve on its socket.
type Server struct {
	Client session.Client
	Agent  tacklr.AgentOptions
	// Workspace says the runtime mounts a workspace for the sessions
	// (worker.Config.Workspace), so clients may bind backends to it (ACP
	// vfs/bind). False refuses binds.
	Workspace bool
	// Protocols are the wire protocols the server serves, each at its
	// Path.
	Protocols []Protocol
	// Security is protocol-neutral authentication and authorization supplied by the host.
	Security *Service
	// HTTPAttempt translates request credentials at the HTTP transport edge.
	HTTPAttempt HTTPAttemptExtractor

	networkPolicyConfigured bool
}

// NewServer serves rt over one or more Protocols. The ACP built-in is
// acp.New.
func NewServer(rt session.Client, agent tacklr.AgentOptions, protocols ...Protocol) *Server {
	if rt == nil || len(protocols) == 0 {
		panic("server: Runtime and at least one Protocol are required")
	}
	return &Server{Client: rt, Agent: agent, Protocols: protocols}
}

// Connect admits r as a connection of the protocol at r's path. A
// credential on r (HTTPAttempt) must be one the host accepts; a request
// with none opens an anonymous connection, which a protocol can
// authenticate later. A refusal is the HTTP status to answer r with.
func (s *Server) Connect(r *http.Request) (*Connection, int) {
	for _, p := range s.Protocols {
		if p.Path() != r.URL.Path {
			continue
		}
		caller, status := s.networkContext(r.Context(), r)
		if status != 0 {
			return nil, status
		}
		return &Connection{proto: p, trace: telemetry.Inject(telemetry.FromHeaders(context.Background(), r.Header)), env: Env{
			Client:    s.Client,
			Agent:     s.Agent,
			Workspace: s.Workspace,
			Security:  s.Security,
			Caller:    caller,
		}}, 0
	}
	return nil, http.StatusNotFound
}

// HTTPMux serves each protocol's connections at its Path, as WebSockets.
// Used by ServeHTTP and tests.
func (s *Server) HTTPMux() *http.ServeMux {
	mux := http.NewServeMux()
	for _, p := range s.Protocols {
		mux.HandleFunc("GET "+p.Path(), s.serveWebSocket)
	}
	return mux
}

// serveWebSocket accepts r as a WebSocket and serves it until it closes.
func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "WebSocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	c, status := s.Connect(r)
	if status != 0 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	// Accept answers a bad upgrade itself.
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// A prompt with an image or a document is larger than the library's
	// 32 KiB default; Cloudflare takes messages up to 32 MiB.
	ws.SetReadLimit(maxFrame)
	c.Serve(r.Context(), webSocket{ws})
	_ = ws.Close(websocket.StatusNormalClosure, "")
}

// webSocket is a Socket over a WebSocket of text messages.
type webSocket struct{ c *websocket.Conn }

func (s webSocket) Read(ctx context.Context) ([]byte, error) {
	_, data, err := s.c.Read(ctx)
	return data, err
}

func (s webSocket) Write(ctx context.Context, frame []byte) error {
	return s.c.Write(ctx, websocket.MessageText, frame)
}

// ServeHTTP listens on addr and serves all protocol routes there (Serve).
// A server with no network policy does not listen.
func (s *Server) ServeHTTP(ctx context.Context, addr string) error {
	if err := s.Policy(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve serves all protocol routes on ln until ctx ends, then shuts the
// server down and returns ctx's error. It closes ln.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if err := s.Policy(); err != nil {
		_ = ln.Close()
		return err
	}
	hs := &http.Server{Handler: s.HTTPMux(), ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		errCh <- hs.Serve(ln)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultHTTPShutdown)
		defer cancel()
		_ = hs.Shutdown(shutdownCtx)
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		// Only Shutdown closes hs, so ln failed to accept.
		return err
	}
}
