package inference

import (
	"bufio"
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

type OpenAIInferenceStrategy struct {
	instructions string
	apiKey       string
	model        string
	reasoning    string
	httpClient   *http.Client
	baseURL      string

	structuredOutputSchema map[string]any
	structuredOutputName   string
	structuredOutputType   reflect.Type
}

func (s *OpenAIInferenceStrategy) SetSystemPrompt(prompt string) {
	s.instructions = prompt
}

func NewOpenAIInferenceStrategy(client *http.Client) *OpenAIInferenceStrategy {
	return &OpenAIInferenceStrategy{
		httpClient: client,
		baseURL:    "https://api.openai.com/v1",
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
		if httpResp.StatusCode == 404 {
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
		Stream: true,
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

		go func() {
		defer close(events)

		httpReq, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/responses", bytes.NewReader(body))
		if err != nil {
			slog.Error("create request", "error", err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

		httpResp, err := s.httpClient.Do(httpReq)
		if err != nil {
			slog.Error("http request", "error", err)
			return
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != 200 {
			respBody, _ := io.ReadAll(httpResp.Body)
			slog.Error("non-200 response", "status", httpResp.StatusCode, "body", extractErrorMessage(respBody))
			return
		}

		s.parseSSEResponse(httpResp.Body, events)
	}()

	return events, nil
}

func (s *OpenAIInferenceStrategy) parseSSEResponse(body io.Reader, events chan<- tacklr.LLMResponseChunk) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	var currentItemID string
	for scanner.Scan() {
		line := scanner.Text()

		const prefix = "data: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		data := line[len(prefix):]
		if data == "[DONE]" {
			break
		}

		var evt struct {
			Type  string          `json:"type"`
			Delta string          `json:"delta"`
			Item  json.RawMessage `json:"item"`
		}
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}

		switch evt.Type {
		case "response.output_item.added":
			var item struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(evt.Item, &item) == nil {
				currentItemID = item.ID
			}
		case "response.output_text.delta":
			if evt.Delta != "" {
				events <- tacklr.LLMResponseChunk{
					Type:       tacklr.StreamEventMessage,
					MessageId:  currentItemID,
					Content:    evt.Delta,
					IsComplete: false,
				}
			}
		case "response.output_item.done":
			if evt.Item != nil {
				s.emitOutputItemComplete(evt.Item, events)
			}
		}
	}

	events <- tacklr.LLMResponseChunk{IsComplete: true}
}

func (s *OpenAIInferenceStrategy) emitOutputItemComplete(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
	var typeHolder struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &typeHolder); err != nil {
		return
	}

	switch typeHolder.Type {
	case "message":
		var msg struct {
			ID string `json:"id"`
		}
		json.Unmarshal(raw, &msg)
		events <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventMessage,
			MessageId:  msg.ID,
			IsComplete: true,
		}
	case "function_call":
		s.emitFunctionCallChunk(raw, events)
	case "reasoning":
		s.emitReasoningChunk(raw, events)
	}
}

func (s *OpenAIInferenceStrategy) emitFunctionCallChunk(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
	var fc struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Arguments string `json:"arguments"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &fc); err != nil {
		return
	}
	name := fc.Name
	namespace := fc.Namespace
	if namespace == "" && strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		namespace = parts[0]
		name = parts[1]
	}
	events <- tacklr.LLMResponseChunk{
		Type: tacklr.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{{
			ID:        fc.ID,
			Type:      "function_call",
			CallID:    fc.CallID,
			Name:      name,
			Namespace: namespace,
			Arguments: fc.Arguments,
			Status:    fc.Status,
		}},
		IsComplete: fc.Status == "completed",
	}
}

func (s *OpenAIInferenceStrategy) emitReasoningChunk(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
	var reasoning struct {
		ID      string `json:"id"`
		Summary []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"summary,omitempty"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"content,omitempty"`
	}
	if err := json.Unmarshal(raw, &reasoning); err != nil {
		return
	}
	var text string
	for _, item := range reasoning.Content {
		if item.Type == "reasoning_text" {
			text += item.Text
		}
	}
	if text != "" {
		events <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventReasoning,
			MessageId:  reasoning.ID,
			Content:    text,
			IsComplete: true,
		}
	}
}

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
