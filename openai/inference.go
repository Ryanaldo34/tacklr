package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"

	"github.com/pkoukk/tiktoken-go"

	"github.com/ryanaldo34/tacklr"
)

// OpenAIInferenceStrategy implements tacklr.InferenceStrategy for the OpenAI Responses API.
type OpenAIInferenceStrategy struct {
	instructions string
	apiKey       string
	model        string
	reasoning    string
	httpClient   *http.Client
	baseURL      string
	doStreaming  bool

	structuredOutputSchema map[string]any
	structuredOutputName   string
	structuredOutputType   reflect.Type
	responseHandler        ResponseStrategy
}

func (s *OpenAIInferenceStrategy) SetSystemPrompt(prompt string) {
	s.instructions = prompt
}

// NewOpenAIInferenceStrategy creates a new strategy with the given HTTP client.
func NewOpenAIInferenceStrategy(client *http.Client) *OpenAIInferenceStrategy {
	s := &OpenAIInferenceStrategy{
		httpClient: client,
		baseURL:    "https://api.openai.com/v1",
	}
	return s
}

func (s *OpenAIInferenceStrategy) ensureResponseHandler() {
	if s.responseHandler != nil {
		return
	}
	s.responseHandler = &OpenAINoStreamResponseStrategy{
		ApiKey:     s.apiKey,
		BaseURL:    s.baseURL,
		HttpClient: s.httpClient,
	}
}

func (s *OpenAIInferenceStrategy) WithApiKey(key string) tacklr.InferenceStrategy {
	s.apiKey = key
	return s
}

func (s *OpenAIInferenceStrategy) WithModel(model string) tacklr.InferenceStrategy {
	s.model = model
	return s
}

func (s *OpenAIInferenceStrategy) WithURL(url string) tacklr.InferenceStrategy {
	s.baseURL = url
	return s
}

func (s *OpenAIInferenceStrategy) WithReasoningLevel(level string) tacklr.InferenceStrategy {
	s.reasoning = level
	return s
}

func (s *OpenAIInferenceStrategy) WithStructuredOutput(v any) tacklr.InferenceStrategy {
	if v == nil {
		s.structuredOutputSchema = nil
		s.structuredOutputName = ""
		s.structuredOutputType = nil
		return s
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	schema, err := tacklr.TypeToJSONSchema(reflect.New(t).Elem().Interface())
	if err != nil {
		slog.Warn("structured output schema build failed; structured output disabled", "type", t.Name(), "error", err)
		s.structuredOutputSchema = nil
		s.structuredOutputName = ""
		s.structuredOutputType = nil
		return s
	}
	s.structuredOutputSchema = schema
	s.structuredOutputName = t.Name()
	s.structuredOutputType = t
	return s
}

func (s *OpenAIInferenceStrategy) WithResponseStrategy(strategy string) tacklr.InferenceStrategy {
	switch strategy {
	case "standard":
		s.doStreaming = false
		s.responseHandler = &OpenAINoStreamResponseStrategy{
			ApiKey:     s.apiKey,
			BaseURL:    s.baseURL,
			HttpClient: s.httpClient,
		}
	default:
		s.doStreaming = false
		s.responseHandler = &OpenAINoStreamResponseStrategy{
			ApiKey:     s.apiKey,
			BaseURL:    s.baseURL,
			HttpClient: s.httpClient,
		}
	}
	return s
}

func (s *OpenAIInferenceStrategy) CompressContextWindow() error {
	return nil
}

func (s *OpenAIInferenceStrategy) MaxContextWindow() (int, error) {
	if limit, ok := modelContextLimits[s.model]; ok {
		return limit, nil
	}
	prefixes := []string{
		"o1-",
		"o3-",
		"o4-",
		"gpt-5",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(s.model, prefix) {
			if strings.HasPrefix(s.model, "o1") || strings.HasPrefix(s.model, "o3") || strings.HasPrefix(s.model, "o4") {
				return 200000, nil
			}
			if strings.HasPrefix(s.model, "gpt-5") {
				return 1000000, nil
			}
		}
	}
	return 0, fmt.Errorf("max context window: unknown model %q: %w", s.model, tacklr.ErrUnknownModel)
}

func (s *OpenAIInferenceStrategy) CountTokens(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
	if s.apiKey == "" {
		return 0, fmt.Errorf("count tokens: %w", tacklr.ErrApiKeyNotSet)
	}
	if s.model == "" {
		return 0, fmt.Errorf("count tokens: %w", tacklr.ErrModelNotSet)
	}

	items, err := marshalMessagesToInput(messages)
	if err != nil {
		return 0, fmt.Errorf("marshal messages: %w", err)
	}

	inputJSON, err := json.Marshal(items)
	if err != nil {
		return 0, fmt.Errorf("marshal input items: %w", err)
	}

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr, err := tacklr.ToolsAsJson(tools)
		if err != nil {
			return 0, fmt.Errorf("serialize tools: %w", err)
		}
		toolsJSON = json.RawMessage(toolsStr)
	}

	reqBody := countTokensRequest{
		Model: s.model,
		Input: inputJSON,
		Tools: toolsJSON,
	}

	if s.instructions != "" {
		reqBody.Instructions = &s.instructions
	}

	if s.structuredOutputSchema != nil {
		reqBody.Text = &textFormat{
			Format: &jsonSchemaFormat{
				Type:   "json_schema",
				Name:   s.structuredOutputName,
				Schema: s.structuredOutputSchema,
				Strict: true,
			},
		}
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/responses/input_tokens", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

	httpResp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("http request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode != 200 {
		// Some OpenAI providers don't have the token counting endpoint, so we fall back to tiktoken-go
		if httpResp.StatusCode == 404 {
			// Use the encoding for gpt 5 series models
			tke, err := tiktoken.GetEncoding("o200k_base")
			if err != nil {
				return 0, fmt.Errorf("tiktoken count tokens: %w", err)
			}
			contents := make([]string, 0, len(messages))
			for _, msg := range messages {
				contents = append(contents, msg.Content)
			}
			tokens := strings.Join(contents, "\n")
			tokenCount := len(tke.Encode(tokens, nil, nil))
			return tokenCount, nil
		}
		return 0, &APIStatusError{Status: httpResp.StatusCode, Body: extractErrorMessage(respBody)}
	}

	var countResp struct {
		InputTokens int    `json:"input_tokens"`
		Object      string `json:"object"`
	}
	if err := json.Unmarshal(respBody, &countResp); err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}

	return countResp.InputTokens, nil
}

func (s *OpenAIInferenceStrategy) Invoke(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool) (chan tacklr.LLMResponseChunk, error) {
	if s.apiKey == "" {
		return nil, fmt.Errorf("invoke: %w", tacklr.ErrApiKeyNotSet)
	}
	if s.model == "" {
		return nil, fmt.Errorf("invoke: %w", tacklr.ErrModelNotSet)
	}

	s.ensureResponseHandler()

	items, err := marshalMessagesToInput(messages)
	if err != nil {
		return nil, fmt.Errorf("marshal messages: %w", err)
	}

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr, err := tacklr.ToolsAsJson(tools)
		if err != nil {
			return nil, fmt.Errorf("serialize tools: %w", err)
		}
		toolsJSON = json.RawMessage(toolsStr)
	}

	inputJSON, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("marshal input items: %w", err)
	}

	reqBody := responsesRequest{
		Model:  s.model,
		Input:  inputJSON,
		Tools:  toolsJSON,
		Stream: s.doStreaming,
	}

	if s.instructions != "" {
		reqBody.Instructions = &s.instructions
	}

	if s.reasoning != "" {
		reqBody.Reasoning = &reasoningDetail{Effort: s.reasoning}
	}

	if s.structuredOutputSchema != nil {
		reqBody.Text = &textFormat{
			Format: &jsonSchemaFormat{
				Type:   "json_schema",
				Name:   s.structuredOutputName,
				Schema: s.structuredOutputSchema,
				Strict: true,
			},
		}
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	events := make(chan tacklr.LLMResponseChunk, 10)
	if s.doStreaming {
		go func() {
			if _, err := s.responseHandler.Handle(ctx, body, events); err != nil {
				slog.Warn("response handler failed", "error", err)
			}
		}()
	} else {
		if _, err := s.responseHandler.Handle(ctx, body, events); err != nil {
			return nil, fmt.Errorf("response handler: %w", err)
		}
	}
	return events, nil
}

// --- Mapping logic ---

func marshalMessagesToInput(messages []*tacklr.Message) ([]json.RawMessage, error) {
	var items []json.RawMessage

	for _, msg := range messages {
		switch msg.Role {
		case tacklr.RoleTool:
			item := functionCallOutputRequest{
				Type:   "function_call_output",
				CallID: msg.ToolCallID,
				Output: msg.Content,
			}
			b, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("marshal tool message: %w", err)
			}
			items = append(items, b)

		case tacklr.RoleUser:
			item := easyInputRequest{
				Role:    string(msg.Role),
				Content: msg.Content,
			}
			b, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("marshal user message: %w", err)
			}
			items = append(items, b)

		case tacklr.RoleAssistant:
			// Emit assistant text content first, then function_call items,
			// matching the order the Responses API returns them (message then
			// function_call). Reversing this can cause 500 errors on Azure.
			if msg.Content != "" {
				item := easyInputRequest{
					Role:    string(msg.Role),
					Content: msg.Content,
				}
				b, err := json.Marshal(item)
				if err != nil {
					return nil, fmt.Errorf("marshal assistant message: %w", err)
				}
				items = append(items, b)
			}
			for _, tc := range msg.ToolCalls {
				fc := functionCallInputRequest{
					Type:      "function_call",
					CallID:    tc.CallID,
					Name:      tc.Name,
					Arguments: tc.Arguments,
				}
				b, err := json.Marshal(fc)
				if err != nil {
					return nil, fmt.Errorf("marshal function_call input: %w", err)
				}
				items = append(items, b)
			}
		}
	}

	return items, nil
}
