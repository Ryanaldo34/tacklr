package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func drain(ch <-chan tacklr.LLMResponseChunk) {
	for range ch {
	}
}

// invokeCollect calls Invoke with a channel, drains the chunks into a Response,
// and applies structured-output parsing if the strategy has one configured.
func invokeCollect(s *OpenAIInferenceStrategy, ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (*tacklr.Response, error) {
	events, err := s.Invoke(ctx, msgs, tools)
	if err != nil {
		return nil, err
	}

	var resp tacklr.Response
	var currentMsg *tacklr.Message
	for chunk := range events {
		switch chunk.Type {
		case tacklr.StreamEventMessage:
			if currentMsg == nil || currentMsg.Role != tacklr.RoleAssistant {
				currentMsg = &tacklr.Message{Role: tacklr.RoleAssistant}
				resp.Messages = append(resp.Messages, currentMsg)
			}
			currentMsg.Content += chunk.Content
		case tacklr.StreamEventFunctionCall:
			if currentMsg == nil || currentMsg.Role != tacklr.RoleAssistant {
				currentMsg = &tacklr.Message{Role: tacklr.RoleAssistant}
				resp.Messages = append(resp.Messages, currentMsg)
			}
			currentMsg.ToolCalls = append(currentMsg.ToolCalls, chunk.ToolCalls...)
		}
		if chunk.IsComplete {
			resp.Status = tacklr.StatusCompleted
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

func zeroArgsStringHandler() (string, error) { return "hello", nil }

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
	if err == nil || !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("expected ErrApiKeyNotSet, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_MissingModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithApiKey("sk-test").(*OpenAIInferenceStrategy)
	_, err := s.Invoke(context.Background(), nil, nil)
	if err == nil || !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("expected ErrModelNotSet, got: %v", err)
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hello!"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(resp.Messages))
	}
	if resp.Messages[0].Role != tacklr.RoleAssistant || resp.Messages[0].Content != "Hi back!" {
		t.Errorf("message = %+v", resp.Messages[0])
	}
}

func TestOpenAIInferenceStrategy_InvokeWithReasoningContext(t *testing.T) {
	var capturedInput []json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody struct {
			Input []json.RawMessage `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		capturedInput = reqBody.Input

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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hello!"},
		{Role: tacklr.RoleReasoning, Content: "Let me think...", MessageID: "rs_1"},
		{Role: tacklr.RoleAssistant, Content: "The answer is 42", MessageID: "msg_1"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 || resp.Messages[0].Content != "Hi back!" {
		t.Errorf("unexpected response: %+v", resp.Messages)
	}

	// Reasoning items are kept in the context window but are not sent back to
	// the model because the Responses API does not accept them as input.
	if len(capturedInput) != 2 {
		t.Fatalf("expected 2 input items (user + assistant), got %d: %s", len(capturedInput), capturedInput)
	}

	var userItem struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(capturedInput[0], &userItem); err != nil {
		t.Fatalf("unmarshal user item: %v", err)
	}
	if userItem.Role != "user" || userItem.Content != "Hello!" {
		t.Errorf("user item = %+v", userItem)
	}

	var assistantItem struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(capturedInput[1], &assistantItem); err != nil {
		t.Fatalf("unmarshal assistant item: %v", err)
	}
	if assistantItem.Role != "assistant" || assistantItem.Content != "The answer is 42" {
		t.Errorf("assistant item = %+v", assistantItem)
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "What's the weather?"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged)", len(resp.Messages))
	}
	if resp.Messages[0].Role != tacklr.RoleAssistant {
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Do two things"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged)", len(resp.Messages))
	}
	if resp.Messages[0].Role != tacklr.RoleAssistant {
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Run tools"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 (merged tool calls)", len(resp.Messages))
	}
	if resp.Messages[0].Role != tacklr.RoleAssistant {
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

	tools := []*tacklr.Tool{
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, tools)
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hi"},
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "What's the weather?"},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: `{"temp":72}`},
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

func TestOpenAIInferenceStrategy_InvokeReplaysAssistantToolCalls(t *testing.T) {
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "What's the weather?"},
		{
			Role:    tacklr.RoleAssistant,
			Content: "Let me check...",
			ToolCalls: []tacklr.ToolCall{
				{ID: "fc_1", CallID: "call_1", Name: "get_weather", Namespace: "weather", Arguments: `{"location":"NYC"}`},
				{ID: "fc_2", CallID: "call_2", Name: "get_weather", Arguments: `{"location":"LA"}`},
			},
		},
		{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: `{"temp":72}`},
		{Role: tacklr.RoleTool, ToolCallID: "call_2", Content: `{"temp":85}`},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(capturedInput) != 6 {
		t.Fatalf("expected 6 input items, got %d", len(capturedInput))
	}

	var assistantMsg easyInputRequest
	if err := json.Unmarshal(capturedInput[1], &assistantMsg); err != nil {
		t.Fatalf("unmarshal assistant: %v", err)
	}
	if assistantMsg.Role != "assistant" || assistantMsg.Content != "Let me check..." {
		t.Errorf("assistant = %+v", assistantMsg)
	}

	var fc1 functionCallInputRequest
	if err := json.Unmarshal(capturedInput[2], &fc1); err != nil {
		t.Fatalf("unmarshal fc1: %v", err)
	}
	if fc1.Type != "function_call" || fc1.ID != "" || fc1.CallID != "call_1" || fc1.Name != "get_weather" || fc1.Arguments != `{"location":"NYC"}` {
		t.Errorf("fc1 = %+v", fc1)
	}

	var fc2 functionCallInputRequest
	if err := json.Unmarshal(capturedInput[3], &fc2); err != nil {
		t.Fatalf("unmarshal fc2: %v", err)
	}
	if fc2.Type != "function_call" || fc2.ID != "" || fc2.CallID != "call_2" || fc2.Name != "get_weather" || fc2.Arguments != `{"location":"LA"}` {
		t.Errorf("fc2 = %+v", fc2)
	}

	var out1 functionCallOutputRequest
	if err := json.Unmarshal(capturedInput[4], &out1); err != nil {
		t.Fatalf("unmarshal output1: %v", err)
	}
	if out1.Type != "function_call_output" || out1.CallID != "call_1" || out1.Output != `{"temp":72}` {
		t.Errorf("output1 = %+v", out1)
	}

	var out2 functionCallOutputRequest
	if err := json.Unmarshal(capturedInput[5], &out2); err != nil {
		t.Fatalf("unmarshal output2: %v", err)
	}
	if out2.Type != "function_call_output" || out2.CallID != "call_2" || out2.Output != `{"temp":85}` {
		t.Errorf("output2 = %+v", out2)
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIStatusError, got %T: %v", err, err)
	}
	if apiErr.Status != 400 {
		t.Errorf("status = %d, want 400", apiErr.Status)
	}
	if !strings.Contains(apiErr.Body, "Invalid model") {
		t.Errorf("body = %q, want contains 'Invalid model'", apiErr.Body)
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
		// Prefix fallbacks (not in modelContextLimits map)
		{"gpt-5-custom", 1000000, false},
		{"o4-mini", 200000, false},
		{"o1-new", 200000, false},
		// Unknown
		{"unknown-model", 0, true},
	}

	for _, tc := range tests {
		s := &OpenAIInferenceStrategy{model: tc.model}
		got, err := s.MaxContextWindow()
		if tc.wantErr {
			if err == nil {
				t.Errorf("model %q: expected error, got nil", tc.model)
				continue
			}
			if !errors.Is(err, tacklr.ErrUnknownModel) {
				t.Errorf("model %q: expected ErrUnknownModel in chain, got: %v", tc.model, err)
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

	_, err := s.Invoke(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Think step by step"}}, nil)
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hello!"},
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Find customer 123"},
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

	tools := []*tacklr.Tool{
		{Name: "standalone_tool", Handler: zeroArgsStringHandler},
		{Name: "get_customer", Namespace: "crm", Handler: zeroArgsStringHandler},
	}
	for _, t := range tools {
		t.Validate()
	}

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Find customer"},
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
	if parsed[0]["name"] != "standalone_tool" {
		t.Errorf("first item name = %v", parsed[0]["name"])
	}
	if parsed[1]["type"] != "function" {
		t.Errorf("second item type = %v", parsed[1]["type"])
	}
	if parsed[1]["name"] != "crm.get_customer" {
		t.Errorf("second item name = %v, want crm.get_customer", parsed[1]["name"])
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

	count, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hello!"},
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

	count, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hi"},
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

	tools := []*tacklr.Tool{
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

	count, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Weather in NYC?"},
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
	_, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
	if err == nil || !errors.Is(err, tacklr.ErrApiKeyNotSet) {
		t.Fatalf("expected ErrApiKeyNotSet, got: %v", err)
	}
}

func TestOpenAIInferenceStrategy_CountTokens_MissingModel(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient).WithApiKey("sk-test").(*OpenAIInferenceStrategy)
	_, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
	if err == nil || !errors.Is(err, tacklr.ErrModelNotSet) {
		t.Fatalf("expected ErrModelNotSet, got: %v", err)
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

	_, err := s.CountTokens(context.Background(), []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *APIStatusError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIStatusError, got %T: %v", err, err)
	}
	if apiErr.Status != 400 {
		t.Errorf("status = %d, want 400", apiErr.Status)
	}
	if !strings.Contains(apiErr.Body, "invalid request") {
		t.Errorf("body = %q, want contains 'invalid request'", apiErr.Body)
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

	count, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hello, how are you?"},
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

	_, err := s.CountTokens(ctx, []*tacklr.Message{{Role: tacklr.RoleUser, Content: "Hi"}}, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Get data"},
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

	resp, err := invokeCollect(s, context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Get data"},
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

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Help with something bad"},
	}, nil)
	if err == nil {
		t.Fatal("expected error from refusal")
	}
	if !errors.Is(err, tacklr.ErrModelRefused) {
		t.Errorf("error = %v, want ErrModelRefused", err)
	}
	if !strings.Contains(err.Error(), "I'm sorry") {
		t.Errorf("error should contain refusal message: %v", err)
	}
}

func TestEnsureResponseHandler_lazyInit(t *testing.T) {
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
						{"type": "output_text", "text": "hi", "annotations": []any{}},
					},
				},
			},
		})
	}))
	defer server.Close()

	// Construct WITHOUT setting responseHandler — lazy init should fire on Invoke
	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-4o",
		httpClient: server.Client(),
		baseURL:    server.URL,
		// responseHandler intentionally nil
	}

	_, err := s.Invoke(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hi"},
	}, nil)
	if err != nil {
		t.Fatalf("lazy init failed: %v", err)
	}
	if s.responseHandler == nil {
		t.Fatal("responseHandler still nil after Invoke — lazy init did not fire")
	}
}

func TestWithStructuredOutput_nilResets(t *testing.T) {
	s := NewOpenAIInferenceStrategy(http.DefaultClient)
	s.WithStructuredOutput(struct {
		Name string `json:"name"`
	}{})
	if s.structuredOutputSchema == nil {
		t.Fatal("schema should be set after WithStructuredOutput(struct)")
	}
	s.WithStructuredOutput(nil)
	if s.structuredOutputSchema != nil {
		t.Error("schema should be nil after WithStructuredOutput(nil)")
	}
	if s.structuredOutputName != "" {
		t.Errorf("name = %q, want empty", s.structuredOutputName)
	}
	if s.structuredOutputType != nil {
		t.Error("type should be nil after reset")
	}
}

func TestCountTokens_NonJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json at all"))
	}))
	defer server.Close()

	s := &OpenAIInferenceStrategy{
		apiKey:     "sk-test",
		model:      "gpt-5.5",
		httpClient: server.Client(),
		baseURL:    server.URL,
	}
	_, err := s.CountTokens(context.Background(), []*tacklr.Message{
		{Role: tacklr.RoleUser, Content: "Hi"},
	}, nil)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
	if !strings.Contains(err.Error(), "unmarshal response") {
		t.Errorf("error = %v, want 'unmarshal response' in chain", err)
	}
}
