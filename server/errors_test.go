package server

import (
	"fmt"
	"testing"

	"github.com/ryanaldo34/tacklr/control"
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
		{"session not found", fmt.Errorf("load: %w", stores.ErrSessionNotFound), true},
		{"interrupt not found", fmt.Errorf("return: %w", control.ErrInterruptNotFound), true},
		{"invalid payload", fmt.Errorf("return: %w", control.ErrInvalidPayload), true},
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
