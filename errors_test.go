package tacklr

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrModelRefused,
		ErrApiKeyNotSet,
		ErrModelNotSet,
		ErrUnknownModel,
		ErrToolNotFound,
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

func TestSentinelWrapping(t *testing.T) {
	sentinels := []error{
		ErrModelRefused,
		ErrApiKeyNotSet,
		ErrModelNotSet,
		ErrUnknownModel,
		ErrToolNotFound,
	}
	for _, sentinel := range sentinels {
		wrapped := fmt.Errorf("context: %w", sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Errorf("wrapped sentinel %v should be errors.Is %v", wrapped, sentinel)
		}
	}
}
