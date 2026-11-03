package server_test

import (
	"context"
	"errors"
	"fmt"
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

// TestService_authenticate covers each Authenticate outcome: a credential
// becomes the authenticator's principal, and a missing service, a failing
// authenticator, an empty principal or an empty subject each fail.
func TestService_authenticate(t *testing.T) {
	authenticator := func(p server.Principal, err error) *server.Service {
		return &server.Service{Authenticator: authenticatorFunc(func(context.Context, server.Attempt) (server.Principal, error) { return p, err })}
	}
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	_, emptySubject := server.NewPrincipal("   ")
	secret := server.NewSecret([]byte("credential"))
	var seen string
	ok := &server.Service{Authenticator: authenticatorFunc(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
		seen = string(attempt.Credential.Bytes())
		return alice, nil
	})}

	got, okErr := ok.Authenticate(t.Context(), server.Attempt{Scheme: "test", Credential: secret, Binding: server.ChannelBinding{Kind: "test", ID: "connection"}})
	_, nilErr := (*server.Service)(nil).Authenticate(t.Context(), server.Attempt{Scheme: "test"})
	_, badErr := authenticator(server.Principal{}, errors.New("bad credential")).Authenticate(t.Context(), server.Attempt{Scheme: "test"})
	_, emptyErr := authenticator(server.Principal{}, nil).Authenticate(t.Context(), server.Attempt{Scheme: "test"})

	if okErr != nil || got.Principal.Subject != "alice" || seen != "credential" {
		t.Fatalf("authenticate = %+v %v (saw %q), want alice from the credential", got, okErr, seen)
	}
	if !errors.Is(nilErr, server.ErrAuthenticationRequired) {
		t.Fatalf("no service = %v", nilErr)
	}
	for name, err := range map[string]error{"a failing authenticator": badErr, "an empty principal": emptyErr, "an empty subject": emptySubject} {
		if !errors.Is(err, server.ErrAuthenticationFailed) {
			t.Errorf("%s = %v, want authentication failed", name, err)
		}
	}
}

// TestService_authorize covers each Authorize outcome: allowed, denied by
// the authorizer, refused without a principal, and a panic for an operation
// with no action (a programming error).
func TestService_authorize(t *testing.T) {
	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	var asked server.Operation
	allow := &server.Service{Authorizer: authorizerFunc(func(_ context.Context, _ server.Principal, op server.Operation) error { asked = op; return nil })}
	deny := &server.Service{Authorizer: authorizerFunc(func(context.Context, server.Principal, server.Operation) error { return errors.New("denied") })}
	load := server.Operation{Action: "session.load", Resource: "session"}

	allowed := allow.Authorize(t.Context(), server.Context{Principal: alice}, load)
	denied := deny.Authorize(t.Context(), server.Context{Principal: alice}, load)
	anonymous := allow.Authorize(t.Context(), server.Context{}, load)

	if allowed != nil || asked != load {
		t.Fatalf("allowed = %v for %+v", allowed, asked)
	}
	if !errors.Is(denied, server.ErrAuthorizationDenied) || !errors.Is(anonymous, server.ErrAuthenticationRequired) {
		t.Fatalf("denied = %v, anonymous = %v", denied, anonymous)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("an operation with no action was authorized")
		}
	}()
	_ = allow.Authorize(t.Context(), server.Context{Principal: alice}, server.Operation{})
}

// TestSecret_redactsAndReportsEmpty: a secret never prints its bytes.
func TestSecret_redactsAndReportsEmpty(t *testing.T) {
	secret := server.NewSecret([]byte("credential"))

	if !(server.Secret{}).Empty() || secret.Empty() {
		t.Fatal("Empty is wrong")
	}
	if secret.String() != "[REDACTED]" || secret.GoString() != "[REDACTED]" || fmt.Sprintf("%v %#v", secret, secret) != "[REDACTED] [REDACTED]" {
		t.Fatalf("secret printed as %q / %q", secret.String(), secret.GoString())
	}
}
