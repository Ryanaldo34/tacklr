package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func drain(ch <-chan LLMResponseChunk) {
	for range ch {
	}
}

// invokeCollect calls Invoke with a channel, drains the chunks into a Response,
// and applies structured-output parsing if the strategy has one configured.
func invokeCollect(s *OpenAIInferenceStrategy, ctx context.Context, msgs []*Message, tools []*Tool) (*Response, error) {
	events, err := s.Invoke(ctx, msgs, tools)
	if err != nil {
		return nil, err
	}

	var resp Response
	var currentMsg *Message
	for chunk := range events {
		switch chunk.Type {
		case StreamEventMessage:
			if currentMsg == nil || currentMsg.Role != RoleAssistant {
				currentMsg = &Message{Role: RoleAssistant}
				resp.Messages = append(resp.Messages, currentMsg)
			}
			currentMsg.Content += chunk.Content
		case StreamEventFunctionCall:
			if currentMsg == nil || currentMsg.Role != RoleAssistant {
				currentMsg = &Message{Role: RoleAssistant}
				resp.Messages = append(resp.Messages, currentMsg)
			}
			currentMsg.ToolCalls = append(currentMsg.ToolCalls, chunk.ToolCalls...)
		}
		if chunk.IsComplete {
			resp.Status = StatusCompleted
		}
	}

	if s.structuredOutputType != nil && len(resp.Messages) > 0 {
		lastMsg := resp.Messages[len(resp.Messages)-1]
		if lastMsg.Content != "" {
			ptr := reflect.New(s.structuredOutputType)
			if json.Unmarshal([]byte(lastMsg.Content), ptr.Interface()) == nil {
				lastMsg.StructuredOutput = ptr.Elem().Interface()
			}
		}
	}

	return &resp, nil
}

func TestOpenAIInferenceStrategy_WithApiKeyAndModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)

	chained := s.WithApiKey("sk-test").WithModel("gpt-4o")

	if _, ok := chained.(*OpenAIInferenceStrategy); !ok {
		t.Fatal("expected *OpenAIInferenceStrategy from chained call")
	}

	if s.apiKey != "sk-test" {
		t.Errorf("apiKey = %q, want %q", s.apiKey, "sk-test")
	}
	if s.model != "gpt-4o" {
		t.Errorf("model = %q, want %q", s.model, "gpt-4o")
	}
}

func TestOpenAIInferenceStrategy_MissingApiKey(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithModel("gpt-4o").(*OpenAIInferenceStrategy)
	_, err := s.Invoke(context.Background(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "API key") {
		t.Fatalf("expected API key error, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_MissingModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithApiKey("sk-test").(*OpenAIInferenceStrategy)
	_, err := s.Invoke(context.Background(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "model") {
		t.Fatalf("expected model error, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_InvokeSimpleText(t *testing.T) {
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
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q", ct)
		}

		var reqBody struct {
			Model        string          `json:"model"`
			Input        json.RawMessage `json:"input"`
			Instructions *string         `json:"instructions,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Fatal(err)
		}
		if reqBody.Model != "gpt-4o" {
			t.Errorf("model = %q", reqBody.Model)
		}
		if reqBody.Instructions != nil {
			t.Errorf("unexpected instructions: %s", *reqBody.Instructions)
		}

		var input []json.RawMessage
		if err := json.Unmarshal(reqBody.Input, &input); err != nil {
			t.Fatal(err)
		}
		if len(input) != 1 {
			t.Fatalf("expected 1 input item, got %d", len(input))
		}

		var msg struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}
		json.Unmarshal(input[0], &msg)
		if msg.Role != "user" || msg.Content != "Hello!" {
			t.Errorf("input = %+v", msg)
		}

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
						{"type": "output_text", "text": "Hi back!", "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "Hello!"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(resp.Messages))
	}
	if resp.Messages[0].Role != RoleAssistant || resp.Messages[0].Content != "Hi back!" {
		t.Errorf("message = %+v", resp.Messages[0])
	}
}

func TestOpenAIInferenceStrategy_InvokeWithToolCall(t *testing.T) {
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
						{"type": "output_text", "text": "Let me check...", "annotations": []any{}},
					},
				},
				{
					"type":      "function_call",
					"id":        "tc_test",
					"call_id":   "call_1",
					"name":      "get_weather",
					"arguments": `{"location":"NYC"}`,
					"status":    "completed",
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "What's the weather?"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged)", len(resp.Messages))
	}
	if resp.Messages[0].Role != RoleAssistant {
		t.Errorf("role = %q", resp.Messages[0].Role)
	}
	if resp.Messages[0].Content != "Let me check..." {
		t.Errorf("content = %q", resp.Messages[0].Content)
	}
	if len(resp.Messages[0].ToolCalls) != 1 {
		t.Fatalf("tool calls = %d", len(resp.Messages[0].ToolCalls))
	}
	if resp.Messages[0].ToolCalls[0].Name != "get_weather" {
		t.Errorf("tool name = %q", resp.Messages[0].ToolCalls[0].Name)
	}
}

func TestOpenAIInferenceStrategy_InvokeWithMultipleToolCalls(t *testing.T) {
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
						{"type": "output_text", "text": "Let me check...", "annotations": []any{}},
					},
				},
				{
					"type":      "function_call",
					"id":        "fc_1",
					"call_id":   "call_1",
					"name":      "get_weather",
					"arguments": `{"location":"NYC"}`,
					"status":    "completed",
				},
				{
					"type":      "function_call",
					"id":        "fc_2",
					"call_id":   "call_2",
					"name":      "send_email",
					"arguments": `{"to":"bob@email.com","body":"Hi"}`,
					"status":    "completed",
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "Do two things"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged)", len(resp.Messages))
	}
	if resp.Messages[0].Role != RoleAssistant {
		t.Errorf("role = %q", resp.Messages[0].Role)
	}
	if resp.Messages[0].Content != "Let me check..." {
		t.Errorf("content = %q", resp.Messages[0].Content)
	}
	if len(resp.Messages[0].ToolCalls) != 2 {
		t.Fatalf("got %d tool calls, want 2", len(resp.Messages[0].ToolCalls))
	}
	if resp.Messages[0].ToolCalls[0].Name != "get_weather" {
		t.Errorf("tool[0] name = %q", resp.Messages[0].ToolCalls[0].Name)
	}
	if resp.Messages[0].ToolCalls[1].Name != "send_email" {
		t.Errorf("tool[1] name = %q", resp.Messages[0].ToolCalls[1].Name)
	}
}

func TestOpenAIInferenceStrategy_InvokeWithToolCallsOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{
				{
					"type":      "function_call",
					"id":        "fc_a",
					"call_id":   "call_a",
					"name":      "tool_a",
					"arguments": `{}`,
					"status":    "completed",
				},
				{
					"type":      "function_call",
					"id":        "fc_b",
					"call_id":   "call_b",
					"name":      "tool_b",
					"arguments": `{}`,
					"status":    "completed",
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "Run tools"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged tool calls)", len(resp.Messages))
	}
	if resp.Messages[0].Role != RoleAssistant {
		t.Errorf("role = %q", resp.Messages[0].Role)
	}
	if resp.Messages[0].Content != "" {
		t.Errorf("expected empty content, got %q", resp.Messages[0].Content)
	}
	if len(resp.Messages[0].ToolCalls) != 2 {
		t.Fatalf("got %d tool calls, want 2", len(resp.Messages[0].ToolCalls))
	}
	if resp.Messages[0].ToolCalls[0].Name != "tool_a" {
		t.Errorf("tool[0] name = %q", resp.Messages[0].ToolCalls[0].Name)
	}
	if resp.Messages[0].ToolCalls[1].Name != "tool_b" {
		t.Errorf("tool[1] name = %q", resp.Messages[0].ToolCalls[1].Name)
	}
}

func TestOpenAIInferenceStrategy_InvokeWithToolsParam(t *testing.T) {
	var capturedTools json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Tools json.RawMessage `json:"tools"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedTools = reqBody.Tools

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	tools := []*Tool{
		{
			Name:        "get_weather",
			Description: "Get the weather",
			Handler: func(args struct {
				Location string `json:"location"`
			}) (string, error) {
				return `{"temp":72}`, nil
			},
		},
	}
	tools[0].Validate()

	_, err := s.Invoke(context.Background(), []*Message{{Role: RoleUser, Content: "Hi"}}, tools)
	if err != nil {
		t.Fatal(err)
	}

	if len(capturedTools) == 0 {
		t.Fatal("tools field was empty in request")
	}

	var parsed []map[string]any
	if err := json.Unmarshal(capturedTools, &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(parsed))
	}
	if parsed[0]["type"] != "function" {
		t.Errorf("tool type = %v", parsed[0]["type"])
	}
}

func TestOpenAIInferenceStrategy_InvokeSystemPromptAsInstructions(t *testing.T) {
	var capturedInstr *string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Instructions *string `json:"instructions"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedInstr = reqBody.Instructions

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:       "sk-test",
		model:        "gpt-4o",
		httpClient:   server.Client(),
		baseURL:      server.URL,
		instructions: "You are a helpful assistant.",
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "Hi"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if capturedInstr == nil {
		t.Fatal("instructions not set in request")
	}
	if *capturedInstr != "You are a helpful assistant." {
		t.Errorf("instructions = %q", *capturedInstr)
	}
}

func TestOpenAIInferenceStrategy_InvokeToolResultToFunctionCallOutput(t *testing.T) {
	var capturedInput []json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Input json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		json.Unmarshal(reqBody.Input, &capturedInput)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "What's the weather?"},
		{Role: RoleTool, ToolCallID: "call_1", Content: `{"temp":72}`},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(capturedInput) != 2 {
		t.Fatalf("expected 2 input items, got %d", len(capturedInput))
	}

	var item2 struct {
		Type   string `json:"type"`
		CallID string `json:"call_id"`
		Output string `json:"output"`
	}
	json.Unmarshal(capturedInput[1], &item2)
	if item2.Type != "function_call_output" {
		t.Errorf("item2 type = %q", item2.Type)
	}
	if item2.CallID != "call_1" {
		t.Errorf("item2 call_id = %q", item2.CallID)
	}
	if item2.Output != `{"temp":72}` {
		t.Errorf("item2 output = %q", item2.Output)
	}
}

func TestOpenAIInferenceStrategy_APIErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Invalid model: gpt-4o-fake",
				"type":    "invalid_request_error",
				"code":    "model_not_found",
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o-fake",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	_, err := s.Invoke(context.Background(), []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "Invalid model") {
		t.Errorf("error = %v", err)
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error missing status code: %v", err)
	}
}

func TestOpenAIInferenceStrategy_MaxContextWindow(t *testing.T) {
	tests := []struct {
		model   string
		want    int
		wantErr bool
	}{
		// GPT-5 frontier models (1M context)
		{"gpt-5.5", 1000000, false},
		{"gpt-5.4", 1000000, false},
		{"gpt-5.4-mini", 400000, false},
		{"gpt-5.4-nano", 400000, false},
		// o-series reasoning models
		{"o1-preview", 200000, false},
		{"o1-pro", 200000, false},
		{"o3", 200000, false},
		{"o3-mini", 200000, false},
		{"o3-pro", 200000, false},
		{"o3-deep-research", 200000, false},
		{"o4-mini", 200000, false},
		// Unknown
		{"unknown-model", 0, true},
	}

	for _, tc := range tests {
		s := &OpenAIInferenceStrategy{model: tc.model}
		got, err := s.MaxContextWindow()
		if tc.wantErr {
			if err == nil {
				t.Errorf("model %q: expected error, got nil", tc.model)
			}
			continue
		}
		if err != nil {
			t.Errorf("model %q: %v", tc.model, err)
			continue
		}
		if got != tc.want {
			t.Errorf("model %q: got %d, want %d", tc.model, got, tc.want)
		}
	}
}

func TestOpenAIInferenceStrategy_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: http.DefaultClient,
		baseURL:    "http://does-not-matter",
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    "http://does-not-matter",
			HttpClient: http.DefaultClient,
		},
	}

	_, err := s.Invoke(ctx, []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestOpenAIInferenceStrategy_UnknownOutputItemSkipped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "reasoning",
					"text": "thinking...",
				},
				{
					"type":   "message",
					"id":     "msg_test",
					"role":   "assistant",
					"status": "completed",
					"content": []map[string]any{
						{"type": "output_text", "text": "Final answer", "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{{Role: RoleUser, Content: "Think step by step"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (reasoning skipped)", len(resp.Messages))
	}
	if resp.Messages[0].Content != "Final answer" {
		t.Errorf("content = %q", resp.Messages[0].Content)
	}
}

func TestCompressContextWindow(t *testing.T) {
	s := &OpenAIInferenceStrategy{}
	if err := s.CompressContextWindow(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewOpenAIInferenceStrategy(t *testing.T) {
	client := &http.Client{Timeout: 5}
	s := NewOpenAIInferenceStrategy(client)
	if s.httpClient != client {
		t.Error("httpClient mismatch")
	}
	if s.baseURL != "https://api.openai.com/v1" {
		t.Errorf("baseURL = %q", s.baseURL)
	}
}

func TestOpenAIInferenceStrategy_WithURL(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	s.WithURL("https://custom.example.com/v2")
	if s.baseURL != "https://custom.example.com/v2" {
		t.Errorf("baseURL = %q", s.baseURL)
	}
}

func TestOpenAIInferenceStrategy_WithReasoningLevel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	s.WithReasoningLevel("medium")
	if s.reasoning != "medium" {
		t.Errorf("reasoning = %q, want %q", s.reasoning, "medium")
	}
}

func TestOpenAIInferenceStrategy_InvokeWithReasoningLevel(t *testing.T) {
	var capturedReasoning *string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Reasoning *struct {
				Effort string `json:"effort,omitempty"`
			} `json:"reasoning,omitempty"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		if reqBody.Reasoning != nil {
			capturedReasoning = &reqBody.Reasoning.Effort
		}

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
						{"type": "output_text", "text": "Hi", "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		reasoning:  "high",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "Hello!"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if capturedReasoning == nil {
		t.Fatal("reasoning was not set in request")
	}
	if *capturedReasoning != "high" {
		t.Errorf("reasoning effort = %q, want %q", *capturedReasoning, "high")
	}
}

func TestOpenAIInferenceStrategy_InvokeWithNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{
				{
					"type":      "function_call",
					"id":        "tc_test",
					"call_id":   "call_1",
					"name":      "get_customer",
					"namespace": "crm",
					"arguments": `{"customer_id":"123"}`,
					"status":    "completed",
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "Find customer 123"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(resp.Messages))
	}
	if len(resp.Messages[0].ToolCalls) != 1 {
		t.Fatalf("got %d tool calls, want 1", len(resp.Messages[0].ToolCalls))
	}
	if resp.Messages[0].ToolCalls[0].Name != "get_customer" {
		t.Errorf("name = %q", resp.Messages[0].ToolCalls[0].Name)
	}
	if resp.Messages[0].ToolCalls[0].Namespace != "crm" {
		t.Errorf("namespace = %q", resp.Messages[0].ToolCalls[0].Namespace)
	}
	if resp.Messages[0].ToolCalls[0].CallID != "call_1" {
		t.Errorf("call_id = %q", resp.Messages[0].ToolCalls[0].CallID)
	}
}

func TestOpenAIInferenceStrategy_InvokeWithNamespacedToolsDefinition(t *testing.T) {
	var capturedTools json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Tools json.RawMessage `json:"tools"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedTools = reqBody.Tools

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":     "resp_test",
			"object": "response",
			"status": "completed",
			"output": []map[string]any{},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	tools := []*Tool{
		{Name: "standalone_tool", Handler: zeroArgsStringHandler},
		{Name: "get_customer", Namespace: "crm", Handler: zeroArgsStringHandler},
	}
	for _, t := range tools {
		t.Validate()
	}

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "Find customer"},
	}, tools)
	if err != nil {
		t.Fatal(err)
	}

	var parsed []map[string]any
	if err := json.Unmarshal(capturedTools, &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 2 {
		t.Fatalf("expected 2 tool definitions, got %d", len(parsed))
	}
	if parsed[0]["type"] != "function" {
		t.Errorf("first item type = %v", parsed[0]["type"])
	}
	if parsed[1]["type"] != "namespace" {
		t.Errorf("second item type = %v", parsed[1]["type"])
	}
	nsTools, ok := parsed[1]["tools"].([]any)
	if !ok {
		t.Fatalf("namespace.tools type = %T", parsed[1]["tools"])
	}
	if len(nsTools) != 1 {
		t.Fatalf("expected 1 tool in namespace, got %d", len(nsTools))
	}
}

func TestOpenAIInferenceStrategy_CountTokens_SimpleMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/responses/input_tokens" {
			t.Errorf("path = %s, want /responses/input_tokens", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer sk-test" {
			t.Errorf("auth = %q", auth)
		}

		var reqBody struct {
			Model        string          `json:"model"`
			Input        json.RawMessage `json:"input"`
			Instructions *string         `json:"instructions,omitempty"`
			Tools        json.RawMessage `json:"tools,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Fatal(err)
		}
		if reqBody.Model != "gpt-5.5" {
			t.Errorf("model = %q", reqBody.Model)
		}

		var input []json.RawMessage
		if err := json.Unmarshal(reqBody.Input, &input); err != nil {
			t.Fatal(err)
		}
		if len(input) != 1 {
			t.Fatalf("expected 1 input item, got %d", len(input))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"input_tokens": 14,
			"object":       "response.input_tokens",
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	count, err := s.CountTokens(context.Background(), []*Message{
		{Role: RoleUser, Content: "Hello!"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 14 {
		t.Errorf("count = %d, want 14", count)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_WithInstructions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Instructions *string `json:"instructions"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		if reqBody.Instructions == nil || *reqBody.Instructions != "You are helpful." {
			t.Errorf("instructions = %v", reqBody.Instructions)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"input_tokens": 28,
			"object":       "response.input_tokens",
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:       "sk-test",
		model:        "gpt-5.5",
		httpClient:   server.Client(),
		baseURL:      server.URL,
		instructions: "You are helpful.",
	}

	count, err := s.CountTokens(context.Background(), []*Message{
		{Role: RoleUser, Content: "Hi"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 28 {
		t.Errorf("count = %d, want 28", count)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_WithTools(t *testing.T) {
	var capturedTools json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Tools json.RawMessage `json:"tools"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedTools = reqBody.Tools

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"input_tokens": 42,
			"object":       "response.input_tokens",
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	tools := []*Tool{
		{
			Name:        "get_weather",
			Description: "Get the current weather",
			Handler: func(args struct {
				Location string `json:"location"`
			}) (string, error) {
				return "sunny", nil
			},
		},
	}
	tools[0].Validate()

	count, err := s.CountTokens(context.Background(), []*Message{
		{Role: RoleUser, Content: "Weather in NYC?"},
	}, tools)
	if err != nil {
		t.Fatal(err)
	}
	if count != 42 {
		t.Errorf("count = %d, want 42", count)
	}
	if len(capturedTools) == 0 {
		t.Fatal("tools field was empty in request")
	}

	var parsed []map[string]any
	if err := json.Unmarshal(capturedTools, &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(parsed))
	}
}

func TestOpenAIInferenceStrategy_CountTokens_MissingApiKey(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithModel("gpt-5.5").(*OpenAIInferenceStrategy)
	_, err := s.CountTokens(context.Background(), []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "API key") {
		t.Fatalf("expected API key error, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_MissingModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithApiKey("sk-test").(*OpenAIInferenceStrategy)
	_, err := s.CountTokens(context.Background(), []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "model") {
		t.Fatalf("expected model error, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "invalid request",
				"type":    "invalid_request_error",
				"code":    "invalid_request",
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	_, err := s.CountTokens(context.Background(), []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "invalid request") {
		t.Errorf("error = %v", err)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_FallbackTiktoken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}

	count, err := s.CountTokens(context.Background(), []*Message{
		{Role: RoleUser, Content: "Hello, how are you?"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count <= 0 {
		t.Errorf("expected positive token count from tiktoken fallback, got %d", count)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: http.DefaultClient,
		baseURL:    "http://does-not-matter",
	}

	_, err := s.CountTokens(ctx, []*Message{{Role: RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestInterfaceCompliance(t *testing.T) {
	var _ InferenceStrategy = (*OpenAIInferenceStrategy)(nil)
}

func TestOpenAIInferenceStrategy_WithStructuredOutput(t *testing.T) {
	type MySchema struct {
		Name string `json:"name" desc:"The name"`
		Age  int    `json:"age"`
	}

	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	chained := s.WithStructuredOutput(MySchema{})

	if _, ok := chained.(*OpenAIInferenceStrategy); !ok {
		t.Fatal("expected *OpenAIInferenceStrategy from chained call")
	}

	if s.structuredOutputName != "MySchema" {
		t.Errorf("structuredOutputName = %q, want %q", s.structuredOutputName, "MySchema")
	}
	if s.structuredOutputSchema == nil {
		t.Fatal("structuredOutputSchema is nil")
	}
	if s.structuredOutputType == nil {
		t.Fatal("structuredOutputType is nil")
	}
}

func TestOpenAIInferenceStrategy_InvokeWithStructuredOutput(t *testing.T) {
	type MySchema struct {
		Name string `json:"name"`
	}

	var capturedText *textFormat
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Text *textFormat `json:"text,omitempty"`
		}
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedText = reqBody.Text

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
						{"type": "output_text", "text": `{"name":"test"}`, "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}
	s.WithStructuredOutput(MySchema{})

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "Get data"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if capturedText == nil {
		t.Fatal("text field was not set in request")
	}
	if capturedText.Format == nil {
		t.Fatal("text.format was not set in request")
	}
	if capturedText.Format.Type != "json_schema" {
		t.Errorf("format type = %q, want %q", capturedText.Format.Type, "json_schema")
	}
	if capturedText.Format.Name != "MySchema" {
		t.Errorf("format name = %q, want %q", capturedText.Format.Name, "MySchema")
	}
	if capturedText.Format.Schema == nil {
		t.Error("format schema is nil")
	}
	if capturedText.Format.Schema["type"] != "object" {
		t.Errorf("schema type = %v", capturedText.Format.Schema["type"])
	}
}

func TestOpenAIInferenceStrategy_InvokeStructuredOutputParsed(t *testing.T) {
	type MySchema struct {
		Name string `json:"name"`
	}

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
						{"type": "output_text", "text": `{"name":"Alice"}`, "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}
	s.WithStructuredOutput(MySchema{})

	resp, err := invokeCollect(s, context.Background(), []*Message{
		{Role: RoleUser, Content: "Get data"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(resp.Messages))
	}
	if resp.Messages[0].Content != `{"name":"Alice"}` {
		t.Errorf("content = %q", resp.Messages[0].Content)
	}
	parsed, ok := resp.Messages[0].StructuredOutput.(MySchema)
	if !ok {
		t.Fatalf("StructuredOutput type = %T, want MySchema", resp.Messages[0].StructuredOutput)
	}
	if parsed.Name != "Alice" {
		t.Errorf("parsed.Name = %q", parsed.Name)
	}
}

func TestOpenAIInferenceStrategy_InvokeStructuredOutputRefusal(t *testing.T) {
	type MySchema struct {
		Name string `json:"name"`
	}

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
						{
							"type":    "refusal",
							"refusal": "I'm sorry, I cannot assist with that request.",
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		responseHandler: &OpenAINoStreamResponseStrategy{
			ApiKey:     "sk-test",
			BaseURL:    server.URL,
			HttpClient: server.Client(),
		},
	}
	s.WithStructuredOutput(MySchema{})

	_, err := s.Invoke(context.Background(), []*Message{
		{Role: RoleUser, Content: "Help with something bad"},
	}, nil)
	if err == nil {
		t.Fatal("expected error from refusal")
	}
	if !strings.Contains(err.Error(), "model refused") {
		t.Errorf("error = %v, want refused error", err)
	}
	if !strings.Contains(err.Error(), "I'm sorry") {
		t.Errorf("error should contain refusal message: %v", err)
	}
}
