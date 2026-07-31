package tacklr

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrModelRefused,
		ErrMaxTokens,
		ErrMaxTurnRequests,
		ErrApiKeyNotSet,
		ErrModelNotSet,
		ErrUnknownModel,
		ErrToolNotFound,
		ErrToolTimeout,
		ErrToolPermissionDenied,
		ErrWorkerNotFound,
		ErrWorkerNoOutput,
		ErrWorkerIncomplete,
		ErrWorkerNoModel,
		ErrEmptyWorkerTask,
		ErrWorkerParkMissing,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			if errors.Is(a, b) {
				t.Errorf("sentinel[%d] (%v) should not be errors.Is sentinel[%d] (%v)", i, a, j, b)
			}
			if errors.Is(b, a) {
				t.Errorf("sentinel[%d] (%v) should not be errors.Is sentinel[%d] (%v)", j, b, i, a)
			}
		}
	}
}

func TestWrapStopReason_preservesIs(t *testing.T) {
	cause := fmt.Errorf("content_filter")
	err := WrapStopReason(ErrModelRefused, cause)
	if !errors.Is(err, ErrModelRefused) {
		t.Fatalf("errors.Is ErrModelRefused = false for %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is cause = false for %v", err)
	}
	if !errors.Is(WrapStopReason(ErrMaxTokens, nil), ErrMaxTokens) {
		t.Fatal("nil cause should return kind")
	}
	// nil kind preserves the cause as-is.
	if !errors.Is(WrapStopReason(nil, cause), cause) {
		t.Fatal("nil kind should return cause")
	}
}

func TestSentinelWrapping(t *testing.T) {
	sentinels := []error{
		ErrModelRefused,
		ErrMaxTokens,
		ErrMaxTurnRequests,
		ErrApiKeyNotSet,
		ErrModelNotSet,
		ErrUnknownModel,
		ErrToolNotFound,
		ErrToolTimeout,
		ErrToolPermissionDenied,
		ErrWorkerNotFound,
		ErrWorkerNoOutput,
		ErrWorkerIncomplete,
		ErrWorkerNoModel,
		ErrEmptyWorkerTask,
		ErrWorkerParkMissing,
	}
	for _, sentinel := range sentinels {
		wrapped := fmt.Errorf("context: %w", sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Errorf("wrapped sentinel %v should be errors.Is %v", wrapped, sentinel)
		}
	}
}
