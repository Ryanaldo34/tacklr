package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

const defaultHTTPShutdown = 5 * time.Second

// Server serves a durable.Runtime over HTTP, with WebSocket when the request upgrades.
// Protocols is the ordered list of wire implementations (ACP and/or host protocols).
type Server struct {
	Runtime   durable.Runtime
	Catalog   durable.Catalog
	Protocols []Protocol
	// Connections tracks ACP WebSocket connections.
	// Custom protocols may ignore it.
	Connections *ConnectionRegistry
	// Security is protocol-neutral authentication and authorization supplied by the host.
	Security *tacklrsecurity.Service
	// HTTPAttempt translates request credentials at the HTTP transport edge.
	HTTPAttempt HTTPAttemptExtractor

	allowAnonymousNetwork   bool
	networkPolicyConfigured bool
}

// NewServer wraps a Runtime and one or more Protocols. ACP is NewACPProtocol;
// pass additional implementations to mount their HTTPRoutes on the same mux.
func NewServer(rt durable.Runtime, cat durable.Catalog, protocols ...Protocol) *Server {
	if rt == nil || len(protocols) == 0 {
		panic("server: Runtime and at least one Protocol are required")
	}
	return &Server{
		Runtime:     rt,
		Catalog:     cat,
		Protocols:   protocols,
		Connections: NewConnectionRegistry(),
	}
}

func (s *Server) env(conn *Conn) ProtocolEnv {
	return ProtocolEnv{Runtime: s.Runtime, Catalog: s.Catalog, Conn: conn, Security: s.Security, Connections: s.Connections}
}

// HTTPMux mounts every Protocol's HTTP routes. Used by ServeHTTP and tests.
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
				env := s.env(&Conn{Security: securityContext})
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
