// Package security defines protocol-neutral authentication and authorization
// capabilities for Tacklr servers. server.Protocol implementations translate
// their wire formats into Attempt and Operation values; this package never
// interprets ACP, JSON-RPC, or HTTP types.
package security

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrAuthenticationRequired means no authenticated principal is available.
	ErrAuthenticationRequired = errors.New("authentication required")
	// ErrAuthenticationFailed means supplied credentials were rejected.
	ErrAuthenticationFailed = errors.New("authentication failed")
	// ErrAuthorizationDenied means the principal cannot perform an operation.
	ErrAuthorizationDenied = errors.New("authorization denied")
)

// Principal is the stable, opaque identity returned by a host Authenticator.
// Subject must be stable across reconnects when durable sessions can be loaded.
type Principal struct {
	Subject string
}

// NewPrincipal constructs a Principal and rejects an empty subject.
func NewPrincipal(subject string) (Principal, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return Principal{}, fmt.Errorf("%w: principal subject is required", ErrAuthenticationFailed)
	}
	return Principal{Subject: subject}, nil
}

// Valid reports whether the principal has a stable subject.
func (p Principal) Valid() bool {
	return strings.TrimSpace(p.Subject) != ""
}

// Secret is ephemeral credential material. Its String and GoString methods are
// deliberately redacted so ordinary structured logging cannot reveal it.
type Secret struct {
	value []byte
}

// NewSecret copies credential material into a redaction-safe value.
func NewSecret(value []byte) Secret {
	return Secret{value: append([]byte(nil), value...)}
}

// Bytes returns a copy for a host Authenticator.
func (s Secret) Bytes() []byte {
	return append([]byte(nil), s.value...)
}

// Empty reports whether no credential material is present.
func (s Secret) Empty() bool {
	return len(s.value) == 0
}

func (Secret) String() string   { return "[REDACTED]" }
func (Secret) GoString() string { return "[REDACTED]" }

// ChannelBinding identifies the logical channel on which authentication
// occurred. Values are transport-neutral and contain no credential material.
type ChannelBinding struct {
	Kind string
	ID   string
}

// Attempt is the canonical input to a host Authenticator.
type Attempt struct {
	Scheme     string
	Credential Secret
	Binding    ChannelBinding
}

// Authenticator verifies an authentication attempt supplied by a protocol or
// transport adapter.
type Authenticator interface {
	Authenticate(context.Context, Attempt) (Principal, error)
}

// Operation is a protocol-neutral authorization request.
type Operation struct {
	Action   string
	Resource string
}

// Authorizer decides whether a principal can perform an operation.
type Authorizer interface {
	Authorize(context.Context, Principal, Operation) error
}

// Context is the authenticated state propagated through server connections.
type Context struct {
	Principal Principal
	Binding   ChannelBinding
}

// Authenticated reports whether this context contains a valid principal.
func (c Context) Authenticated() bool {
	return c.Principal.Valid()
}

// Service coordinates host-provided authentication and authorization without
// knowing how any protocol represents credentials or errors.
type Service struct {
	Authenticator Authenticator
	Authorizer    Authorizer
}

// Authenticate delegates to the host and validates its returned invariant.
func (s *Service) Authenticate(ctx context.Context, attempt Attempt) (Context, error) {
	if s == nil || s.Authenticator == nil {
		return Context{}, ErrAuthenticationRequired
	}
	principal, err := s.Authenticator.Authenticate(ctx, attempt)
	if err != nil {
		return Context{}, fmt.Errorf("%w: %w", ErrAuthenticationFailed, err)
	}
	if !principal.Valid() {
		return Context{}, fmt.Errorf("%w: authenticator returned an empty principal", ErrAuthenticationFailed)
	}
	return Context{Principal: principal, Binding: attempt.Binding}, nil
}

// Authorize requires an authenticated principal and delegates to the optional
// host policy. No Authorizer means every authenticated principal is allowed.
func (s *Service) Authorize(ctx context.Context, securityContext Context, operation Operation) error {
	if !securityContext.Authenticated() {
		return ErrAuthenticationRequired
	}
	if strings.TrimSpace(operation.Action) == "" {
		panic("security: authorization operation action is required")
	}
	if s == nil || s.Authorizer == nil {
		return nil
	}
	if err := s.Authorizer.Authorize(ctx, securityContext.Principal, operation); err != nil {
		return fmt.Errorf("%w: %w", ErrAuthorizationDenied, err)
	}
	return nil
}
