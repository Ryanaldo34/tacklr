package server_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/server"
)

type authenticatorFunc func(context.Context, server.Attempt) (server.Principal, error)

func (f authenticatorFunc) Authenticate(ctx context.Context, attempt server.Attempt) (server.Principal, error) {
	return f(ctx, attempt)
}

type authorizerFunc func(context.Context, server.Principal, server.Operation) error

func (f authorizerFunc) Authorize(ctx context.Context, principal server.Principal, operation server.Operation) error {
	return f(ctx, principal, operation)
}

func TestService_authenticatesRedactsAndAuthorizes(t *testing.T) {
	// Arrange
	secret := server.NewSecret([]byte("credential"))
	service := &server.Service{
		Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if string(attempt.Credential.Bytes()) != "credential" {
				t.Fatalf("credential = %q", attempt.Credential.Bytes())
			}
			return server.NewPrincipal("alice")
		}),
		Authorizer: authorizerFunc(func(_ context.Context, principal server.Principal, operation server.Operation) error {
			if principal.Subject != "alice" || operation.Action != "session.load" {
				t.Fatalf("authorization = %#v %#v", principal, operation)
			}
			return nil
		}),
	}

	// Act
	securityContext, err := service.Authenticate(t.Context(), server.Attempt{
		Scheme:     "test",
		Credential: secret,
		Binding:    server.ChannelBinding{Kind: "test", ID: "connection"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = service.Authorize(t.Context(), securityContext, server.Operation{Action: "session.load", Resource: "session"})

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if got := secret.String(); got != "[REDACTED]" {
		t.Fatalf("secret string = %q", got)
	}
}

func TestNewPrincipal_rejectsEmptySubject(t *testing.T) {
	// Act
	_, err := server.NewPrincipal("   ")

	// Assert
	if !errors.Is(err, server.ErrAuthenticationFailed) {
		t.Fatalf("error = %v", err)
	}
}

func TestSecret_emptyAndRedaction(t *testing.T) {
	// Arrange
	empty := server.Secret{}
	secret := server.NewSecret([]byte("credential"))

	// Assert
	if !empty.Empty() {
		t.Fatal("empty secret reported as non-empty")
	}
	if secret.Empty() {
		t.Fatal("non-empty secret reported as empty")
	}
	if got := secret.String(); got != "[REDACTED]" {
		t.Fatalf("string = %q", got)
	}
	if got := secret.GoString(); got != "[REDACTED]" {
		t.Fatalf("go string = %q", got)
	}
}

func TestService_nilAuthenticatorAndEmptyPrincipal(t *testing.T) {
	// Arrange
	var service *server.Service

	// Act
	_, nilErr := service.Authenticate(t.Context(), server.Attempt{Scheme: "test"})
	_, emptyPrincipalErr := (&server.Service{
		Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) {
			return server.Principal{}, nil
		}),
	}).Authenticate(t.Context(), server.Attempt{Scheme: "test"})

	// Assert
	if !errors.Is(nilErr, server.ErrAuthenticationRequired) {
		t.Fatalf("nil service error = %v", nilErr)
	}
	if !errors.Is(emptyPrincipalErr, server.ErrAuthenticationFailed) {
		t.Fatalf("empty principal error = %v", emptyPrincipalErr)
	}
}

func TestService_authorizeRequiresAction(t *testing.T) {
	// Arrange
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	service := &server.Service{}

	// Act
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for empty action")
		}
	}()
	_ = service.Authorize(t.Context(), server.Context{Principal: alice}, server.Operation{})
}

func TestService_authorizerDenies(t *testing.T) {
	// Arrange
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	service := &server.Service{
		Authorizer: authorizerFunc(func(context.Context, server.Principal, server.Operation) error {
			return errors.New("denied")
		}),
	}

	// Act
	err = service.Authorize(t.Context(), server.Context{Principal: alice}, server.Operation{Action: "session.load"})

	// Assert
	if !errors.Is(err, server.ErrAuthorizationDenied) {
		t.Fatalf("authorize error = %v", err)
	}
}

func TestService_rejectsInvalidAuthenticationAndAuthorization(t *testing.T) {
	// Arrange
	service := &server.Service{
		Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) {
			return server.Principal{}, errors.New("bad credential")
		}),
	}

	// Act
	_, authErr := service.Authenticate(t.Context(), server.Attempt{Scheme: "test"})
	authorizationErr := service.Authorize(t.Context(), server.Context{}, server.Operation{Action: "session.load"})

	// Assert
	if !errors.Is(authErr, server.ErrAuthenticationFailed) {
		t.Fatalf("authenticate error = %v", authErr)
	}
	if !errors.Is(authorizationErr, server.ErrAuthenticationRequired) {
		t.Fatalf("authorize error = %v", authorizationErr)
	}
}
