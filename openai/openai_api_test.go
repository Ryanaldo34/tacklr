package openai

import (
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

func TestClassifyAPIStatus_nilError(t *testing.T) {
	if err := classifyAPIStatus(nil, ""); err != nil {
		t.Fatalf("nil api error: %v", err)
	}
}

func TestClassifyIncompleteReason_allOutcomes(t *testing.T) {
	if err := classifyIncompleteReason(""); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := classifyIncompleteReason("max_output_tokens"); !errors.Is(err, session.ErrMaxTokens) {
		t.Fatalf("max_output_tokens: %v", err)
	}
	if err := classifyIncompleteReason("max_tokens"); !errors.Is(err, session.ErrMaxTokens) {
		t.Fatalf("max_tokens: %v", err)
	}
	if err := classifyIncompleteReason("content_filter"); !errors.Is(err, session.ErrModelRefused) {
		t.Fatalf("content_filter: %v", err)
	}
	err := classifyIncompleteReason("other")
	if err == nil || errors.Is(err, session.ErrMaxTokens) {
		t.Fatalf("other: %v", err)
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("err = %v", err)
	}
}
