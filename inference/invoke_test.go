package inference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
)

func TestInvoke_streamsMessageAndMapsDeveloperToSystem(t *testing.T) {
	var sawBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if err := json.Unmarshal(raw, &sawBody); err != nil {
			t.Errorf("unmarshal body: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`data: {"type":"response.output_item.added","item":{"id":"msg_1","type":"message"}}`,
			`data: {"type":"response.output_text.delta","item_id":"msg_1","delta":"hello"}`,
			`data: {"type":"response.output_item.done","item":{"id":"msg_1","type":"message"}}`,
			`data: {"type":"response.completed","response":{"status":"completed"}}`,
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("test-key").
		WithModel("test-model").
		WithURL(srv.URL)
	s.SetSystemPrompt("sys instructions")

	msgs := []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "user ask"},
		{Role: tacklr.RoleDeveloper, Content: "handoff notes"},
	}
	tools := []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{
			Name: "echo",
			Handler: func(ctx context.Context) (string, error) {
				return "ok", nil
			},
		}),
	}

	ch, err := s.Invoke(context.Background(), msgs, tools, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var text strings.Builder
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			t.Fatalf("stream error: %s", chunk.Content)
		}
		if chunk.Type == tacklr.StreamEventMessage && chunk.Content != "" {
			text.WriteString(chunk.Content)
		}
	}
	if text.String() != "hello" {
		t.Fatalf("streamed = %q, want hello", text.String())
	}

	if sawBody == nil {
		t.Fatal("request body not captured")
	}
	if sawBody["model"] != "test-model" {
		t.Errorf("model = %v", sawBody["model"])
	}
	if sawBody["instructions"] != "sys instructions" {
		t.Errorf("instructions = %v", sawBody["instructions"])
	}
	if sawBody["stream"] != true {
		t.Errorf("stream = %v, want true", sawBody["stream"])
	}

	// Developer messages must wire as system so Foundry treats handoff as instruction.
	inputRaw, _ := json.Marshal(sawBody["input"])
	var input []map[string]any
	if err := json.Unmarshal(inputRaw, &input); err != nil {
		t.Fatalf("input: %v body=%s", err, inputRaw)
	}
	var roles []string
	for _, item := range input {
		if role, ok := item["role"].(string); ok {
			roles = append(roles, role)
		}
	}
	if len(roles) < 2 || roles[0] != "user" || roles[1] != "system" {
		t.Fatalf("input roles = %v, want [user system] (developer→system)", roles)
	}

	toolsRaw, _ := json.Marshal(sawBody["tools"])
	if !strings.Contains(string(toolsRaw), "echo") {
		t.Errorf("tools JSON missing echo: %s", toolsRaw)
	}
}

func TestInvoke_truncatedStreamDoesNotCommitPartialAssistant(t *testing.T) {
	// Arrange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","item_id":"msg","delta":"partial"}`+"\n")
	}))
	t.Cleanup(server.Close)
	strategy := NewOpenAIInferenceStrategy(server.Client()).
		WithApiKey("test-key").
		WithModel("gpt-5.4").
		WithURL(server.URL)
	agent, err := tacklr.NewAgent(t.Context(), tacklr.AgentOptions{
		Config: tacklr.Config{MaxWindowSize: 8192},
		Model:  strategy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agent.Close)

	// Act
	events, err := agent.Run(t.Context(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for event := range events {
		if event.Type == tacklr.StreamEventError {
			streamErr = event.Error
		}
	}

	// Assert
	if !errors.Is(streamErr, ErrIncompleteStream) {
		t.Fatalf("stream error = %v", streamErr)
	}
	for _, message := range agent.Messages() {
		if message.Role == tacklr.RoleAssistant && strings.Contains(message.Content, "partial") {
			t.Fatalf("partial assistant committed: %#v", agent.Messages())
		}
	}
}

func TestCountTokens_usesAPIWhenAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses/input_tokens" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"response.input_tokens","input_tokens":42}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	n, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "hello world"},
	}, nil)
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n != 42 {
		t.Fatalf("tokens = %d, want 42", n)
	}
}

func TestCountTokens_404WithoutLocalFallback_returnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	_, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "count these tokens please"},
	}, nil)
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Fatalf("want 404 API error, got %v", err)
	}
}

func TestCountTokens_fallsBackToTiktokenOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL).
		WithLocalTokenFallback()

	n, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "count these tokens please"},
	}, nil)
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive tiktoken fallback count, got %d", n)
	}
}

func TestInvoke_contextCancel_stopsStream(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		// Slow infinite stream until client disconnects.
		for i := 0; i < 1000; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","item_id":"m","delta":"x"}`+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Invoke(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}, nil, "")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start")
	}
	// Read at least one chunk then cancel.
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no chunks")
	}
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed — success
			}
		case <-deadline:
			t.Fatal("stream did not end after cancel")
		}
	}
}

func TestInvoke_apiError_emitsErrorChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"model overloaded"}}`)
	}))
	t.Cleanup(srv.Close)

	s := NewOpenAIInferenceStrategy(srv.Client()).
		WithApiKey("k").
		WithModel("m").
		WithURL(srv.URL)

	ch, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "hi"},
	}, nil, "")
	if err != nil {
		t.Fatalf("Invoke sync err: %v", err)
	}
	var saw string
	for chunk := range ch {
		if chunk.Type == tacklr.StreamEventError {
			saw = chunk.Content
		}
	}
	if !strings.Contains(saw, "model overloaded") {
		t.Fatalf("error content = %q, want model overloaded", saw)
	}
}
