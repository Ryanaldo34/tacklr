package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/pkoukk/tiktoken-go"
)

// OpenAIInferenceStrategy implements InferenceStrategy for the OpenAI Responses API.
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

func (s *OpenAIInferenceStrategy) WithApiKey(key string) InferenceStrategy {
	s.apiKey = key
	return s
}

func (s *OpenAIInferenceStrategy) WithModel(model string) InferenceStrategy {
	s.model = model
	return s
}

func (s *OpenAIInferenceStrategy) WithURL(url string) InferenceStrategy {
	s.baseURL = url
	return s
}

func (s *OpenAIInferenceStrategy) WithReasoningLevel(level string) InferenceStrategy {
	s.reasoning = level
	return s
}

func (s *OpenAIInferenceStrategy) WithStructuredOutput(v any) InferenceStrategy {
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
	schema, err := TypeToJSONSchema(reflect.New(t).Elem().Interface())
	if err != nil {
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

func (s *OpenAIInferenceStrategy) WithResponseStrategy(strategy string) InferenceStrategy {
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

var modelContextLimits = map[string]int{
	"gpt-5.5":          1000000,
	"gpt-5.4":          1000000,
	"gpt-5.4-mini":     400000,
	"gpt-5.4-nano":     400000,
	"o1-preview":       200000,
	"o1-pro":           200000,
	"o3":               200000,
	"o3-mini":          200000,
	"o3-pro":           200000,
	"o3-deep-research": 200000,
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
	return 0, fmt.Errorf("unknown model %q; cannot determine context window size", s.model)
}

func (s *OpenAIInferenceStrategy) CountTokens(ctx context.Context, messages []*Message, tools []*Tool) (int, error) {
	if s.apiKey == "" {
		return 0, fmt.Errorf("OpenAI API key not set; call WithApiKey")
	}
	if s.model == "" {
		return 0, fmt.Errorf("model not set; call WithModel")
	}

	items := marshalMessagesToInput(messages)

	inputJSON, err := json.Marshal(items)
	if err != nil {
		return 0, fmt.Errorf("marshal input items: %w", err)
	}

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr, err := ToolsAsJson(tools)
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
		return 0, fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, extractErrorMessage(respBody))
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

type countTokensRequest struct {
	Model        string          `json:"model"`
	Input        json.RawMessage `json:"input"`
	Instructions *string         `json:"instructions,omitempty"`
	Tools        json.RawMessage `json:"tools,omitempty"`
	Text         *textFormat     `json:"text,omitempty"`
}

func (s *OpenAIInferenceStrategy) Invoke(ctx context.Context, messages []*Message, tools []*Tool) (chan LLMResponseChunk, error) {
	if s.apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key not set; call WithApiKey")
	}
	if s.model == "" {
		return nil, fmt.Errorf("model not set; call WithModel")
	}

	s.ensureResponseHandler()

	items := marshalMessagesToInput(messages)

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr, err := ToolsAsJson(tools)
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

	events := make(chan LLMResponseChunk, 10)
	if s.doStreaming {
		go func() {
			s.responseHandler.Handle(ctx, body, events)
		}()
	} else {
		if err := s.responseHandler.Handle(ctx, body, events); err != nil {
			return nil, err
		}
	}
	return events, nil
}

// --- Request/Response wire types (unexported) ---

type responsesRequest struct {
	Model        string           `json:"model"`
	Input        json.RawMessage  `json:"input"`
	Instructions *string          `json:"instructions,omitempty"`
	Tools        json.RawMessage  `json:"tools,omitempty"`
	Stream       bool             `json:"stream,omitempty"`
	Reasoning    *reasoningDetail `json:"reasoning,omitempty"`
	Text         *textFormat      `json:"text,omitempty"`
}

type textFormat struct {
	Format *jsonSchemaFormat `json:"format,omitempty"`
}

type jsonSchemaFormat struct {
	Type   string         `json:"type"`
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict bool           `json:"strict"`
}

type reasoningDetail struct {
	Effort string `json:"effort,omitempty"`
}

type incompleteDetail struct {
	Reason string `json:"reason"`
}

type responsesResponse struct {
	ID                string            `json:"id"`
	Object            string            `json:"object"`
	Status            string            `json:"status"`
	Output            []json.RawMessage `json:"output"`
	Error             *apiErrorDetail   `json:"error,omitempty"`
	IncompleteDetails *incompleteDetail `json:"incomplete_details,omitempty"`
}

type apiErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

type easyInputRequest struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type functionCallOutputRequest struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

// --- Mapping logic ---

func marshalMessagesToInput(messages []*Message) []json.RawMessage {
	var items []json.RawMessage

	for _, msg := range messages {
		switch msg.Role {
		case RoleTool:
			item := functionCallOutputRequest{
				Type:   "function_call_output",
				CallID: msg.ToolCallID,
				Output: msg.Content,
			}
			b, _ := json.Marshal(item)
			items = append(items, b)

		case RoleUser:
			item := easyInputRequest{
				Role:    string(msg.Role),
				Content: msg.Content,
			}
			b, _ := json.Marshal(item)
			items = append(items, b)

		case RoleAssistant:
			// Skip assistant messages that are purely tool-call carriers
			if msg.Content == "" && len(msg.ToolCalls) > 0 {
				continue
			}
			item := easyInputRequest{
				Role:    string(msg.Role),
				Content: msg.Content,
			}
			b, _ := json.Marshal(item)
			items = append(items, b)
		}
	}

	return items
}

func parseOutputItem(raw json.RawMessage) (*Message, error) {
	var typeHolder struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &typeHolder); err != nil {
		return nil, fmt.Errorf("parse output item type: %w", err)
	}

	switch typeHolder.Type {
	case "message":
		return parseOutputMessage(raw)
	case "function_call":
		return parseFunctionCall(raw)
	default:
		return nil, nil
	}
}

func parseOutputMessage(raw json.RawMessage) (*Message, error) {
	var rawMsg struct {
		ID      string          `json:"id"`
		Role    string          `json:"role"`
		Status  string          `json:"status"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &rawMsg); err != nil {
		return nil, fmt.Errorf("parse message: %w", err)
	}

	var contentParts []struct {
		Type    string `json:"type"`
		Text    string `json:"text,omitempty"`
		Refusal string `json:"refusal,omitempty"`
	}
	text := ""
	if err := json.Unmarshal(rawMsg.Content, &contentParts); err == nil {
		for _, cp := range contentParts {
			if cp.Type == ContentTypeRefusal {
				return nil, fmt.Errorf("model refused: %s", cp.Refusal)
			}
			if cp.Type == ContentTypeOutputText || cp.Type == "text" {
				text += cp.Text
			}
		}
	}

	return &Message{
		Role:    MessageRole(rawMsg.Role),
		Content: text,
	}, nil
}

func parseFunctionCall(raw json.RawMessage) (*Message, error) {
	var fc struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Arguments string `json:"arguments"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &fc); err != nil {
		return nil, fmt.Errorf("parse function_call: %w", err)
	}

	return &Message{
		Role: RoleAssistant,
		ToolCalls: []ToolCall{{
			ID:        fc.ID,
			Type:      "function_call",
			CallID:    fc.CallID,
			Name:      fc.Name,
			Namespace: fc.Namespace,
			Arguments: fc.Arguments,
			Status:    fc.Status,
		}},
	}, nil
}

// extractErrorMessage attempts to parse the OpenAI error payload from a non-200 response body.
func extractErrorMessage(body []byte) string {
	var errResp struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		return errResp.Error.Message
	}
	return string(body)
}
