package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

	events := make(chan LLMResponseChunk, 10)
	err := s.Handle(context.Background(), []byte(`{}`), events)
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

	events := make(chan LLMResponseChunk, 10)
	err := s.Handle(context.Background(), []byte(`{}`), events)
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

	events := make(chan LLMResponseChunk, 1)
	err := s.Handle(ctx, []byte(`{}`), events)
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

	events := make(chan LLMResponseChunk, 1)
	err := s.Handle(context.Background(), []byte(`{}`), events)
	if err == nil {
		t.Fatal("expected HTTP error")
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

	events := make(chan LLMResponseChunk, 1)
	err := s.Handle(ctx, []byte(`{}`), events)
	if err == nil {
		t.Fatal("expected error from cancellation during HTTP request")
	}
}
