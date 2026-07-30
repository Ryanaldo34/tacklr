package inference

import (
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func TestClassifyProviderFailure_refusal(t *testing.T) {
	body := []byte(`{"error":{"message":"Your request was blocked by content_filter","code":"content_filter","type":"invalid_request_error"}}`)
	err := ClassifyProviderFailure(400, body)
	if !errors.Is(err, tacklr.ErrModelRefused) {
		t.Fatalf("err = %v, want ErrModelRefused", err)
	}
	var api *APIStatusError
	if !errors.As(err, &api) {
		t.Fatal("expected APIStatusError in chain")
	}
	if api.Status != 400 {
		t.Errorf("status = %d", api.Status)
	}
}

func TestClassifyProviderFailure_maxTokens(t *testing.T) {
	body := []byte(`{"error":{"message":"This model's maximum context length is 128000 tokens","code":"context_length_exceeded"}}`)
	err := ClassifyProviderFailure(400, body)
	if !errors.Is(err, tacklr.ErrMaxTokens) {
		t.Fatalf("err = %v, want ErrMaxTokens", err)
	}
}

func TestClassifyProviderFailure_unmapped(t *testing.T) {
	body := []byte(`{"error":{"message":"internal overload","code":"server_error"}}`)
	err := ClassifyProviderFailure(500, body)
	if errors.Is(err, tacklr.ErrModelRefused) || errors.Is(err, tacklr.ErrMaxTokens) {
		t.Fatalf("should not map overload: %v", err)
	}
	var api *APIStatusError
	if !errors.As(err, &api) || api.Status != 500 {
		t.Fatalf("err = %v", err)
	}
}

func TestClassifyIncompleteReason(t *testing.T) {
	err := ClassifyIncompleteReason("max_output_tokens")
	if !errors.Is(err, tacklr.ErrMaxTokens) {
		t.Fatalf("err = %v", err)
	}
	err = ClassifyIncompleteReason("content_filter")
	if !errors.Is(err, tacklr.ErrModelRefused) {
		t.Fatalf("err = %v", err)
	}
	err = ClassifyIncompleteReason("other")
	if err == nil || errors.Is(err, tacklr.ErrMaxTokens) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("err = %v", err)
	}
}
