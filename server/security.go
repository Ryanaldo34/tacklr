package server

import (
	"context"
	"errors"
	"net/http"
)

// ErrNetworkSecurityPolicyRequired prevents accidental anonymous listeners.
var ErrNetworkSecurityPolicyRequired = errors.New("server: configure security or explicitly allow anonymous network access")

// HTTPAttemptExtractor translates transport credential evidence into the
// protocol-neutral security model. It is an edge adapter, not part of the core.
type HTTPAttemptExtractor func(*http.Request) (Attempt, bool)

// WithSecurity installs host authentication and optional HTTP credential
// extraction. Protocol-native flows such as ACP authenticate can use service
// without an extractor.
func (s *Server) WithSecurity(service *Service, extract HTTPAttemptExtractor) *Server {
	if service == nil {
		panic("server: security Service is required")
	}
	s.Security = service
	s.HTTPAttempt = extract
	s.networkPolicyConfigured = true
	return s
}

// Policy is ErrNetworkSecurityPolicyRequired until the host chose a
// network policy (WithSecurity or AllowAnonymousNetwork). A host that
// serves the server on a platform's own listener checks it before it
// serves.
func (s *Server) Policy() error {
	if !s.networkPolicyConfigured {
		return ErrNetworkSecurityPolicyRequired
	}
	return nil
}

// AllowAnonymousNetwork explicitly enables unauthenticated network serving.
// It is intended for local development and trusted private environments.
func (s *Server) AllowAnonymousNetwork() *Server {
	s.Security = nil
	s.HTTPAttempt = nil
	s.networkPolicyConfigured = true
	return s
}

// networkContext is the security context of a request that opens a
// connection: its principal when it carries a credential the host accepts
// (else the status to refuse it with), an anonymous principal on a server
// with no security, and the zero Context for a request with no credential,
// which a protocol can authenticate later.
func (s *Server) networkContext(ctx context.Context, r *http.Request) (Context, int) {
	if s.Security == nil {
		principal, _ := NewPrincipal("anonymous")
		return Context{Principal: principal, Binding: ChannelBinding{Kind: "network"}}, 0
	}
	if s.HTTPAttempt != nil {
		if attempt, ok := s.HTTPAttempt(r); ok {
			securityContext, err := s.Security.Authenticate(ctx, attempt)
			if err != nil {
				return Context{}, http.StatusUnauthorized
			}
			return securityContext, 0
		}
	}
	return Context{}, 0
}
