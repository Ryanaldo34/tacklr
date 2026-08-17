package server

import (
	"context"
	"errors"
	"net/http"

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

// ErrNetworkSecurityPolicyRequired prevents accidental anonymous listeners.
var ErrNetworkSecurityPolicyRequired = errors.New("server: configure security or explicitly allow anonymous network access")

// HTTPAttemptExtractor translates transport credential evidence into the
// protocol-neutral security model. It is an edge adapter, not part of the core.
type HTTPAttemptExtractor func(*http.Request) (tacklrsecurity.Attempt, bool)

// WithSecurity installs host authentication and optional HTTP credential
// extraction. Protocol-native flows such as ACP authenticate can use service
// without an extractor.
func (s *Server) WithSecurity(service *tacklrsecurity.Service, extract HTTPAttemptExtractor) *Server {
	if s == nil {
		panic("server: nil Server")
	}
	if service == nil {
		panic("server: security Service is required")
	}
	s.Security = service
	s.HTTPAttempt = extract
	s.allowAnonymousNetwork = false
	s.networkPolicyConfigured = true
	return s
}

// AllowAnonymousNetwork explicitly enables unauthenticated network serving.
// It is intended for local development and trusted private environments.
func (s *Server) AllowAnonymousNetwork() *Server {
	if s == nil {
		panic("server: nil Server")
	}
	s.Security = nil
	s.HTTPAttempt = nil
	s.allowAnonymousNetwork = true
	s.networkPolicyConfigured = true
	return s
}

func (s *Server) networkContext(ctx context.Context, r *http.Request, allowUnauthenticated bool) (*tacklrsecurity.Context, int) {
	if s.Security != nil {
		if s.HTTPAttempt != nil {
			if attempt, ok := s.HTTPAttempt(r); ok {
				securityContext, err := s.Security.Authenticate(ctx, attempt)
				if err != nil {
					return nil, http.StatusUnauthorized
				}
				return &securityContext, 0
			}
		}
		if allowUnauthenticated {
			return new(tacklrsecurity.Context), 0
		}
		return nil, http.StatusUnauthorized
	}
	if s.allowAnonymousNetwork || !s.networkPolicyConfigured {
		principal, err := tacklrsecurity.NewPrincipal("anonymous")
		if err != nil {
			panic(err)
		}
		return &tacklrsecurity.Context{
			Principal: principal,
			Binding: tacklrsecurity.ChannelBinding{
				Kind: "network",
			},
		}, 0
	}
	return nil, http.StatusServiceUnavailable
}
