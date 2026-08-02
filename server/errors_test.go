package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
)

func TestIsClientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"validation", clientErrorf(ErrInvalidRequest, "agent_id is required"), true},
		{"agent not found", clientErrorf(ErrAgentNotFound, "agent not found"), true},
		{"store not configured", clientErrorf(ErrSessionStoreNotConfigured, "store missing"), true},
		{"method not found", clientErrorf(ErrMethodNotFound, "method not found"), true},
		{"wrapped client", fmt.Errorf("load agent: %w", clientErrorf(ErrAgentNotFound, "missing")), true},
		{"session not found", fmt.Errorf("load: %w", stores.ErrSessionNotFound), true},
		{"interrupt not found", fmt.Errorf("return: %w", interrupt.ErrInterruptNotFound), true},
		{"invalid payload", fmt.Errorf("return: %w", interrupt.ErrInvalidPayload), true},
		{"internal", fmt.Errorf("something broke"), false},
		{"nil", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsClientError(tc.err); got != tc.want {
				t.Errorf("IsClientError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestPublicError(t *testing.T) {
	client := clientErrorf(ErrAgentNotFound, "agent %q not found", "x")
	if got := PublicError(client); !errors.Is(got, ErrAgentNotFound) {
		t.Errorf("PublicError(client) = %v, want unwrap ErrAgentNotFound", got)
	}
	if got := PublicError(fmt.Errorf("db down")); !errors.Is(got, ErrInternal) {
		t.Errorf("PublicError(internal) = %v, want ErrInternal", got)
	}
	if got := PublicError(nil); !errors.Is(got, ErrInternal) {
		t.Errorf("PublicError(nil) = %v, want ErrInternal", got)
	}
}

func TestJSONRPCErrorCode(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{clientErrorf(ErrInvalidRequest, "bad"), jsonRPCCodeInvalidRequest},
		{clientErrorf(ErrMethodNotFound, "nope"), jsonRPCCodeMethodNotFound},
		{clientErrorf(ErrAgentNotFound, "missing"), jsonRPCCodeApplication},
		{ErrRequestCancelled, jsonRPCCodeCancelled},
		{fmt.Errorf("boom"), jsonRPCCodeInternal},
		{nil, jsonRPCCodeInternal},
	}
	for _, tc := range cases {
		if got := JSONRPCErrorCode(tc.err); got != tc.want {
			t.Errorf("JSONRPCErrorCode(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestClientError_errorsIs(t *testing.T) {
	err := fmt.Errorf("load agent: %w", clientErrorf(ErrAgentNotFound, "agent %q not found", "x"))
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatal("expected errors.Is to match ErrAgentNotFound through wrap")
	}
}
