package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func TestStopReasonFromError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		want   string
		wantOK bool
	}{
		{"nil", nil, "", false},
		{"refused", tacklr.ErrModelRefused, stopReasonRefusal, true},
		{"refused wrapped", tacklr.WrapStopReason(tacklr.ErrModelRefused, errors.New("filter")), stopReasonRefusal, true},
		{"max tokens", tacklr.ErrMaxTokens, stopReasonMaxTokens, true},
		{"max turn", tacklr.ErrMaxTurnRequests, stopReasonMaxTurnRequests, true},
		{"canceled", context.Canceled, stopReasonCancelled, true},
		{"cancel wrap", fmt.Errorf("run: context cancelled: %w", context.Canceled), stopReasonCancelled, true},
		{"unmapped", errors.New("boom"), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stopReasonFromError(tt.err)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("stopReasonFromError(%v) = (%q, %v), want (%q, %v)", tt.err, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
