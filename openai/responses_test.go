package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
)

func TestOpenAIResponseStrategy_NormalFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/responses" {
			t.Errorf("path = %s, want /responses", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer sk-test" {
			t.Errorf("auth = %q", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"resp_1","object":"response","status":"completed","output":[]}`))
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}

	events := make(chan tacklr.LLMResponseChunk, 10)
	_, err := s.Handle(context.Background(), []byte(`{}`), events)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case chunk := <-events:
		if !chunk.IsComplete {
			t.Error("expected final IsComplete chunk")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for chunk")
	}
}

func TestOpenAIResponseStrategy_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"resp_1","object":"response","status":"completed","output":[]}`))
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}

	events := make(chan tacklr.LLMResponseChunk, 10)
	_, err := s.Handle(context.Background(), []byte(`{}`), events)
	if err != nil {
		t.Fatal(err)
	}
	// The final IsComplete chunk should arrive
	select {
	case chunk := <-events:
		if !chunk.IsComplete {
			t.Error("expected final IsComplete chunk")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for final chunk")
	}
}

func TestOpenAIResponseStrategy_EarlyReturnOnCancelledContext(t *testing.T) {
	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    "http://does-not-matter",
		HttpClient: http.DefaultClient,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	events := make(chan tacklr.LLMResponseChunk, 1)
	_, err := s.Handle(ctx, []byte(`{}`), events)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestOpenAIResponseStrategy_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		w.Write([]byte(`{"error":{"message":"bad request"}}`))
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}

	events := make(chan tacklr.LLMResponseChunk, 1)
	_, err := s.Handle(context.Background(), []byte(`{}`), events)
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIStatusError, got %T: %v", err, err)
	}
	if apiErr.Status != 400 {
		t.Errorf("status = %d, want 400", apiErr.Status)
	}
}

func TestOpenAIResponseStrategy_CancellationDuringHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	events := make(chan tacklr.LLMResponseChunk, 1)
	_, err := s.Handle(ctx, []byte(`{}`), events)
	if err == nil {
		t.Fatal("expected error from cancellation during HTTP request")
	}
}

func TestResponseStrategy_APIErrorWithCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_err",
			"object": "response",
			"status": "failed",
			"error": map[string]any{
				"code":    "rate_limit_exceeded",
				"message": "Too many requests",
				"type":    "rate_limit_error",
			},
			"output": []any{},
		})
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}
	events := make(chan tacklr.LLMResponseChunk, 10)
	_, err := s.Handle(context.Background(), nil, events)
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIStatusError, got %T: %v", err, err)
	}
	if apiErr.Code != "rate_limit_exceeded" {
		t.Errorf("code = %q, want 'rate_limit_exceeded'", apiErr.Code)
	}
	if !strings.Contains(apiErr.Body, "Too many requests") {
		t.Errorf("body = %q, want contains 'Too many requests'", apiErr.Body)
	}
}

func TestResponseStrategy_ReasoningEmission(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "reasoning",
					"id":   "rs_test",
					"content": []map[string]any{
						{"type": "reasoning_text", "text": "Let me think about this..."},
					},
				},
				{
					"type":   "message",
					"id":     "msg_test",
					"role":   "assistant",
					"status": "completed",
					"content": []map[string]any{
						{"type": "output_text", "text": "The answer is 42", "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}
	events := make(chan tacklr.LLMResponseChunk, 10)
	if _, err := s.Handle(context.Background(), []byte(`{}`), events); err != nil {
		t.Fatal(err)
	}

	var chunks []tacklr.LLMResponseChunk
	for chunk := range events {
		chunks = append(chunks, chunk)
	}

	var reasoningChunk *tacklr.LLMResponseChunk
	for i := range chunks {
		if chunks[i].Type == tacklr.StreamEventReasoning {
			reasoningChunk = &chunks[i]
			break
		}
	}
	if reasoningChunk == nil {
		t.Fatal("no StreamEventReasoning chunk emitted")
	}
	if reasoningChunk.Content != "Let me think about this..." {
		t.Errorf("reasoning content = %q", reasoningChunk.Content)
	}
	if reasoningChunk.MessageId != "rs_test" {
		t.Errorf("reasoning MessageId = %q, want 'rs_test'", reasoningChunk.MessageId)
	}
}

func TestResponseStrategy_TextAliasContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{
				{
					"type":   "message",
					"id":     "msg_test",
					"role":   "assistant",
					"status": "completed",
					"content": []map[string]any{
						{"type": "text", "text": "plain text alias"},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAINoStreamResponseStrategy{
		ApiKey:     "sk-test",
		BaseURL:    server.URL,
		HttpClient: server.Client(),
	}
	events := make(chan tacklr.LLMResponseChunk, 10)
	if _, err := s.Handle(context.Background(), []byte(`{}`), events); err != nil {
		t.Fatal(err)
	}

	var content string
	for chunk := range events {
		if chunk.Type == tacklr.StreamEventMessage {
			content += chunk.Content
		}
	}
	if content != "plain text alias" {
		t.Errorf("content = %q, want 'plain text alias'", content)
	}
}

func TestExtractErrorMessage_fallbackToRawBody(t *testing.T) {
	// Non-JSON body
	got := extractErrorMessage([]byte("oops not json"))
	if got != "oops not json" {
		t.Errorf("got %q, want raw body", got)
	}
	// Valid JSON but no error.message field
	got = extractErrorMessage([]byte(`{"error":{}}`))
	if got != `{"error":{}}` {
		t.Errorf("got %q, want raw body when message empty", got)
	}
	// Valid error message
	got = extractErrorMessage([]byte(`{"error":{"message":"real error"}}`))
	if got != "real error" {
		t.Errorf("got %q, want 'real error'", got)
	}
}
