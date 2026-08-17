package security_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/security"
)

type authenticatorFunc func(context.Context, security.Attempt) (security.Principal, error)

func (f authenticatorFunc) Authenticate(ctx context.Context, attempt security.Attempt) (security.Principal, error) {
	return f(ctx, attempt)
}

type authorizerFunc func(context.Context, security.Principal, security.Operation) error

func (f authorizerFunc) Authorize(ctx context.Context, principal security.Principal, operation security.Operation) error {
	return f(ctx, principal, operation)
}

func TestService_authenticatesRedactsAndAuthorizes(t *testing.T) {
	// Arrange
	secret := security.NewSecret([]byte("credential"))
	service := &security.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt security.Attempt) (security.Principal, error) {
			if string(attempt.Credential.Bytes()) != "credential" {
				t.Fatalf("credential = %q", attempt.Credential.Bytes())
			}
			return security.NewPrincipal("alice")
		}),
		Authorizer: authorizerFunc(func(_ context.Context, principal security.Principal, operation security.Operation) error {
			if principal.Subject != "alice" || operation.Action != "session.load" {
				t.Fatalf("authorization = %#v %#v", principal, operation)
			}
			return nil
		}),
	}

	// Act
	securityContext, err := service.Authenticate(t.Context(), security.Attempt{
		Scheme:     "test",
		Credential: secret,
		Binding:    security.ChannelBinding{Kind: "test", ID: "connection"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = service.Authorize(t.Context(), securityContext, security.Operation{Action: "session.load", Resource: "session"})

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if got := secret.String(); got != "[REDACTED]" {
		t.Fatalf("secret string = %q", got)
	}
}

func TestService_rejectsInvalidAuthenticationAndAuthorization(t *testing.T) {
	// Arrange
	service := &security.Service{
		Authenticator: authenticatorFunc(func(context.Context, security.Attempt) (security.Principal, error) {
			return security.Principal{}, errors.New("bad credential")
		}),
	}

	// Act
	_, authErr := service.Authenticate(t.Context(), security.Attempt{Scheme: "test"})
	authorizationErr := service.Authorize(t.Context(), security.Context{}, security.Operation{Action: "session.load"})

	// Assert
	if !errors.Is(authErr, security.ErrAuthenticationFailed) {
		t.Fatalf("authenticate error = %v", authErr)
	}
	if !errors.Is(authorizationErr, security.ErrAuthenticationRequired) {
		t.Fatalf("authorize error = %v", authorizationErr)
	}
}
