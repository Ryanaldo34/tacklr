package inference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"

	"github.com/pkoukk/tiktoken-go"
	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

type OpenAIInferenceStrategy struct {
	instructions string
	apiKey       string
	model        string
	// reasoning effort: "low" | "medium" | "high" (provider-specific).
	reasoning string
	// reasoningSummary: "auto" | "concise" | "detailed". Empty omits the field.
	// Required for Azure OpenAI/Foundry to stream response.reasoning_summary_text.delta.
	reasoningSummary string
	// maxOutputTokens is sent as max_output_tokens when > 0 (Azure/OpenAI Responses).
	maxOutputTokens int
	httpClient      *http.Client
	baseURL         string

	structuredOutputSchema map[string]any
	structuredOutputName   string
	structuredOutputType   reflect.Type
	// localTokenFallback uses tiktoken when the provider has no input_tokens endpoint.
	localTokenFallback bool
}

var (
	// ErrIncompleteStream means a provider closed without a terminal response event.
	ErrIncompleteStream = errors.New("incomplete provider stream")
	// ErrMalformedStream means a provider emitted invalid SSE event JSON.
	ErrMalformedStream = errors.New("malformed provider stream")
)

func (s *OpenAIInferenceStrategy) SetSystemPrompt(prompt string) {
	s.instructions = prompt
}

func NewOpenAIInferenceStrategy(client *http.Client) *OpenAIInferenceStrategy {
	if client == nil {
		client = http.DefaultClient
	}
	return &OpenAIInferenceStrategy{
		httpClient: client,
		baseURL:    "https://api.openai.com/v1",
	}
}

func (s *OpenAIInferenceStrategy) WithApiKey(key string) *OpenAIInferenceStrategy {
	s.apiKey = key
	return s
}

func (s *OpenAIInferenceStrategy) WithModel(model string) *OpenAIInferenceStrategy {
	s.model = model
	return s
}

// ModelTelemetryIdentity implements the optional harness hook so model spans
// get GenAI provider/model attrs without exporting raw config fields.
func (s *OpenAIInferenceStrategy) ModelTelemetryIdentity() telemetry.ModelIdentity {
	if s == nil {
		return telemetry.ModelIdentity{}
	}
	return telemetry.NewModelIdentity(s.model, s.baseURL)
}

func (s *OpenAIInferenceStrategy) WithURL(url string) *OpenAIInferenceStrategy {
	s.baseURL = url
	return s
}

// WithLocalTokenFallback counts tokens with tiktoken when the provider returns
// 404/400/422 for /responses/input_tokens. Off by default.
func (s *OpenAIInferenceStrategy) WithLocalTokenFallback() *OpenAIInferenceStrategy {
	s.localTokenFallback = true
	return s
}

func (s *OpenAIInferenceStrategy) WithReasoningLevel(level string) *OpenAIInferenceStrategy {
	s.reasoning = level
	// Default summary so Azure OpenAI / OpenAI stream thought deltas as
	// response.reasoning_summary_text.delta (mapped to StreamEventReasoning).
	if level != "" && s.reasoningSummary == "" {
		s.reasoningSummary = "auto"
	}
	return s
}

// WithReasoningSummary sets reasoning.summary on Responses API requests
// ("auto", "concise", "detailed"). Empty clears it.
func (s *OpenAIInferenceStrategy) WithReasoningSummary(summary string) *OpenAIInferenceStrategy {
	s.reasoningSummary = summary
	return s
}

// WithMaxOutputTokens sets Responses API max_output_tokens (0 omits the field).
// Raise this for Azure reasoning models after large tool results (e.g. web_search)
// so streams do not end as bare response.incomplete.
func (s *OpenAIInferenceStrategy) WithMaxOutputTokens(n int) *OpenAIInferenceStrategy {
	if n < 0 {
		n = 0
	}
	s.maxOutputTokens = n
	return s
}

func (s *OpenAIInferenceStrategy) WithStructuredOutput(v any) *OpenAIInferenceStrategy {
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
	// TypeToJSONSchema only fails for nil types; value types always succeed.
	schema, _ := tacklr.TypeToJSONSchema(reflect.New(t).Elem().Interface())
	s.structuredOutputSchema = schema
	s.structuredOutputName = t.Name()
	s.structuredOutputType = t
	return s
}

// SupportsMIME implements tacklr.InferenceStrategy for the selected model id.
func (s *OpenAIInferenceStrategy) SupportsMIME(mimeType string) bool {
	if s == nil {
		return streaming.IsTextMIME(mimeType)
	}
	return modelSupportsMIME(s.model, mimeType)
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

// getEncoding is tiktoken.GetEncoding; overridden in tests for the rare failure path.
var getEncoding = tiktoken.GetEncoding

func (s *OpenAIInferenceStrategy) CountTokens(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
	if s.apiKey == "" {
		return 0, fmt.Errorf("count tokens: %w", tacklr.ErrApiKeyNotSet)
	}
	if s.model == "" {
		return 0, fmt.Errorf("count tokens: %w", tacklr.ErrModelNotSet)
	}

	items := marshalMessagesToInput(messages)
	inputJSON, err := json.Marshal(items)
	if err != nil {
		return 0, fmt.Errorf("marshal token-count input: %w", err)
	}

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr := tacklr.ToolsAsJson(tools)
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
		return 0, fmt.Errorf("marshal token-count request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.baseURL, "/")+"/responses/input_tokens", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build token-count request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

	httpResp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("token-count request failed: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		if s.localTokenFallback && (httpResp.StatusCode == http.StatusNotFound || httpResp.StatusCode == http.StatusBadRequest || httpResp.StatusCode == http.StatusUnprocessableEntity) {
			tke, err := getEncoding("o200k_base")
			if err != nil {
				return 0, fmt.Errorf("tiktoken count tokens: %w", err)
			}
			contents := make([]string, 0, len(messages))
			for _, msg := range messages {
				contents = append(contents, msg.Content)
			}
			return len(tke.Encode(strings.Join(contents, "\n"), nil, nil)), nil
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

func (s *OpenAIInferenceStrategy) Invoke(ctx context.Context, messages []*tacklr.Message, tools []*tacklr.Tool, systemPrompt string) (chan tacklr.LLMResponseChunk, error) {
	if s.apiKey == "" {
		return nil, fmt.Errorf("invoke: %w", tacklr.ErrApiKeyNotSet)
	}
	if s.model == "" {
		return nil, fmt.Errorf("invoke: %w", tacklr.ErrModelNotSet)
	}

	items := marshalMessagesToInput(messages)

	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsStr := tacklr.ToolsAsJson(tools)
		toolsJSON = json.RawMessage(toolsStr)
	}

	inputJSON, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("marshal model input: %w", err)
	}

	reqBody := responsesRequest{
		Model:   s.model,
		Input:   inputJSON,
		Tools:   toolsJSON,
		Stream:  true,
		Include: []string{"reasoning.encrypted_content"},
	}
	if s.maxOutputTokens > 0 {
		reqBody.MaxOutputTokens = s.maxOutputTokens
	}

	prompt := systemPrompt
	if prompt == "" {
		prompt = s.instructions
	}
	if prompt != "" {
		reqBody.Instructions = &prompt
	}

	if s.reasoning != "" || s.reasoningSummary != "" {
		reqBody.Reasoning = &reasoningDetail{
			Effort:  s.reasoning,
			Summary: s.reasoningSummary,
		}
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
		return nil, fmt.Errorf("marshal model request: %w", err)
	}
	inputSummary := summarizeInputItems(items)

	events := make(chan tacklr.LLMResponseChunk, 10)

	// sendChunk delivers a chunk unless ctx is already cancelled (avoids blocking
	// the HTTP goroutine after session/cancel when the consumer has stopped).
	sendChunk := func(chunk tacklr.LLMResponseChunk) {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
		case events <- chunk:
		}
	}
	sendErr := func(err error) {
		if err == nil {
			return
		}
		sendChunk(tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventError,
			Content:    err.Error(),
			Error:      err,
			IsComplete: true,
		})
	}

	go func() {
		defer close(events)

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.baseURL, "/")+"/responses", bytes.NewReader(body))
		if err != nil {
			err = fmt.Errorf("build provider request: %w", err)
			slog.ErrorContext(ctx, "failed to build model provider request", "error", err)
			sendErr(err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

		httpResp, err := s.httpClient.Do(httpReq)
		if err != nil {
			err = fmt.Errorf("model provider request failed: %w", err)
			slog.ErrorContext(ctx, "model provider request failed", "error", err)
			// Surface transport failures as stream errors (no silent channel close).
			sendErr(err)
			return
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK {
			respBody, readErr := io.ReadAll(httpResp.Body)
			if readErr != nil {
				slog.WarnContext(ctx, "could not read provider error body", "error", readErr, "http_status", httpResp.StatusCode)
			}
			classified := classifyProviderFailure(httpResp.StatusCode, respBody)
			emitProviderFailed(ctx, classified, httpResp.StatusCode, inputSummary, string(respBody))
			sendErr(classified)
			return
		}

		s.parseSSEResponse(ctx, httpResp.Body, events, inputSummary)
	}()

	return events, nil
}

func (s *OpenAIInferenceStrategy) parseSSEResponse(ctx context.Context, body io.Reader, events chan<- tacklr.LLMResponseChunk, inputSummary string) {
	// Match main-branch classification: output_text is always a message chunk.
	// DeepSeek on Foundry streams thinking inside output_text (e.g. <think> tags);
	// reclassifying those deltas as StreamEventReasoning broke ACP clients that
	// already handled thinking correctly from agent_message_chunk on main.
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	var currentItemID string
	terminal := false
	emitFailure := func(err error, detail string) {
		emitProviderFailed(ctx, err, http.StatusOK, inputSummary, detail)
		events <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventError,
			Content:    err.Error(),
			Error:      err,
			IsComplete: true,
		}
	}
	// item IDs that already streamed reasoning_*_text.delta — avoid replaying
	// the full summary on output_item.done (would duplicate ACP thought chunks).
	reasoningStreamed := make(map[string]struct{})
	for scanner.Scan() {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		line := scanner.Text()

		const prefix = "data: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		data := line[len(prefix):]
		if data == "[DONE]" {
			if !terminal {
				emitFailure(ErrIncompleteStream, "provider sent [DONE] before a terminal response event")
			}
			return
		}

		var evt struct {
			Type   string          `json:"type"`
			Delta  string          `json:"delta"`
			Item   json.RawMessage `json:"item"`
			ItemID string          `json:"item_id"`
			Error  *apiErrorDetail `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			emitFailure(fmt.Errorf("%w: %w", ErrMalformedStream, err), "")
			return
		}

		msgID := currentItemID
		if evt.ItemID != "" {
			msgID = evt.ItemID
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
					MessageId:  msgID,
					Content:    evt.Delta,
					IsComplete: false,
				}
			}
		case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
			// Explicit reasoning channels (OpenAI/Azure o-series style) → thought.
			// Keep summary as additive; do not invent reclassification of output_text.
			if evt.Delta != "" {
				if msgID != "" {
					reasoningStreamed[msgID] = struct{}{}
				}
				events <- tacklr.LLMResponseChunk{
					Type:       tacklr.StreamEventReasoning,
					MessageId:  msgID,
					Content:    evt.Delta,
					IsComplete: false,
				}
			}
		case "response.output_item.done":
			if evt.Item != nil {
				s.emitOutputItemComplete(evt.Item, events, reasoningStreamed)
			}
		case "error":
			// Azure mid-stream errors often use type=error with HTTP 200.
			_, classified := classifyTerminalSSE("error", data)
			if classified == nil {
				classified = &APIStatusError{Status: 200, Body: "stream error event"}
			}
			emitProviderFailed(ctx, classified, 200, inputSummary, data)
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Content:    classified.Error(),
				Error:      classified,
				IsComplete: true,
			}
			return
		case "response.incomplete", "response.failed", "response.completed":
			// Terminal incomplete/failed, or completed-with-bad-status (some Azure builds).
			if terminal, classified := classifyTerminalSSE(evt.Type, data); terminal {
				emitProviderFailed(ctx, classified, 200, inputSummary, data)
				events <- tacklr.LLMResponseChunk{
					Type:       tacklr.StreamEventError,
					Content:    classified.Error(),
					Error:      classified,
					IsComplete: true,
				}
				return
			}
			// Successful response.completed: surface token usage for model spans/metrics.
			if evt.Type == "response.completed" {
				terminal = true
				if u, ok := parseResponseUsage(data); ok {
					events <- tacklr.LLMResponseChunk{
						Type:            tacklr.StreamEventComplete,
						IsComplete:      true,
						InputTokens:     u.Input,
						OutputTokens:    u.Output,
						ReasoningTokens: u.Reasoning,
					}
				}
			}
		}
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		emitFailure(fmt.Errorf("%w: %w", ErrIncompleteStream, err), "")
		return
	}
	if !terminal {
		emitFailure(ErrIncompleteStream, "provider stream closed before a terminal response event")
	}
}

// responseUsage is the token counts we care about from Responses API usage.
type responseUsage struct {
	Input     int
	Output    int
	Reasoning int
}

// parseResponseUsage extracts usage from a response.completed SSE payload.
func parseResponseUsage(data string) (responseUsage, bool) {
	var payload struct {
		Response struct {
			Usage *struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
				// Nested details (OpenAI / Azure Responses).
				OutputTokensDetails *struct {
					ReasoningTokens int `json:"reasoning_tokens"`
				} `json:"output_tokens_details"`
				// Flat alternate some gateways emit.
				ReasoningTokens int `json:"reasoning_tokens"`
			} `json:"usage"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil || payload.Response.Usage == nil {
		return responseUsage{}, false
	}
	u := payload.Response.Usage
	out := responseUsage{
		Input:  u.InputTokens,
		Output: u.OutputTokens,
	}
	if u.OutputTokensDetails != nil && u.OutputTokensDetails.ReasoningTokens > 0 {
		out.Reasoning = u.OutputTokensDetails.ReasoningTokens
	} else if u.ReasoningTokens > 0 {
		out.Reasoning = u.ReasoningTokens
	}
	if out.Input == 0 && out.Output == 0 && out.Reasoning == 0 {
		return responseUsage{}, false
	}
	return out, true
}

// classifyTerminalSSE maps Responses SSE terminal events to a harness error.
// Returns terminal=false for response.completed with a successful status.
func classifyTerminalSSE(evtType, data string) (terminal bool, err error) {
	var payload struct {
		Response struct {
			Status            string            `json:"status"`
			IncompleteDetails *incompleteDetail `json:"incomplete_details"`
			Error             *apiErrorDetail   `json:"error"`
			// Azure occasionally nests error fields without full apiErrorDetail shape.
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"response"`
		IncompleteDetails *incompleteDetail `json:"incomplete_details"`
		Error             *apiErrorDetail   `json:"error"`
		// Azure sometimes puts reason as a bare string.
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal([]byte(data), &payload)

	status := strings.TrimSpace(payload.Response.Status)
	if evtType == "response.completed" {
		// Successful completion — not an error path.
		if status == "" || status == "completed" {
			return false, nil
		}
		if status != "incomplete" && status != "failed" && status != "cancelled" {
			return false, nil
		}
	}

	detail := payload.Response.IncompleteDetails
	if detail == nil {
		detail = payload.IncompleteDetails
	}
	reason := ""
	if detail != nil {
		reason = strings.TrimSpace(detail.Reason)
	}
	if reason == "" {
		reason = strings.TrimSpace(payload.Reason)
	}

	if reason != "" {
		if classified := classifyIncompleteReason(reason); classified != nil {
			return true, classified
		}
	}

	apiErr := &APIStatusError{Status: 200, Body: ""}
	if payload.Response.Error != nil {
		apiErr.Body = payload.Response.Error.Message
		apiErr.Code = payload.Response.Error.Code
		if payload.Response.Error.Type != "" && apiErr.Body == "" {
			apiErr.Body = payload.Response.Error.Type
		}
	} else if payload.Error != nil {
		apiErr.Body = payload.Error.Message
		apiErr.Code = payload.Error.Code
	} else if payload.Response.ErrorMessage != "" {
		apiErr.Body = payload.Response.ErrorMessage
		apiErr.Code = payload.Response.ErrorCode
	}
	// Human-readable body when the provider sent no message.
	if strings.TrimSpace(apiErr.Body) == "" {
		var parts []string
		parts = append(parts, "stream ended without a usable response")
		if status != "" {
			parts = append(parts, "status="+status)
		}
		if reason != "" {
			parts = append(parts, "reason="+reason)
		}
		if evtType != "" {
			parts = append(parts, "event="+evtType)
		}
		apiErr.Body = strings.Join(parts, "; ")
	}
	classified := classifyAPIStatus(apiErr, "")
	// Known stop-reason sentinels are already actionable; skip noisy raw dumps.
	if errors.Is(classified, tacklr.ErrModelRefused) || errors.Is(classified, tacklr.ErrMaxTokens) {
		return true, classified
	}
	slog.Error("model stream ended with a provider error",
		"event", evtType,
		"response_status", status,
		"reason", reason,
		"error_code", apiErr.Code,
		"error", classified,
		"response_excerpt", truncateForLog(data, 400),
	)
	return true, classified
}

func truncateForLog(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// emitProviderFailed records a span-correlated OTel log event and a clear slog line.
func emitProviderFailed(ctx context.Context, err error, httpStatus int, inputItems, body string) {
	code := ""
	if err != nil {
		var api *APIStatusError
		if errors.As(err, &api) && api != nil {
			if httpStatus == 0 {
				httpStatus = api.Status
			}
			code = api.Code
		}
	}
	attrs := []log.KeyValue{
		log.String(telemetry.EventAttrInputItems, inputItems),
		log.String(telemetry.AttrErrorClass, telemetry.ClassifyErrorClass(err, httpStatus)),
	}
	if httpStatus > 0 {
		attrs = append(attrs, log.Int(telemetry.AttrHTTPStatus, httpStatus))
	}
	if code != "" {
		attrs = append(attrs, log.String(telemetry.AttrErrorCode, code))
	}
	telemetry.EmitEventSeverity(ctx, telemetry.EventProviderFailed, log.SeverityError, attrs...)
}

func (s *OpenAIInferenceStrategy) emitOutputItemComplete(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk, reasoningStreamed map[string]struct{}) {
	var typeHolder struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &typeHolder); err != nil {
		return
	}

	switch typeHolder.Type {
	case "message":
		var msg struct {
			ID      string `json:"id"`
			Status  string `json:"status"`
			Content []struct {
				Type    string `json:"type"`
				Refusal string `json:"refusal"`
				Text    string `json:"text"`
			} `json:"content"`
		}
		_ = json.Unmarshal(raw, &msg)
		// Refusal-only completed message → terminal stop reason, not end_turn.
		if isRefusalMessage(msg.Content) {
			text := refusalText(msg.Content)
			err := tacklr.WrapStopReason(tacklr.ErrModelRefused, fmt.Errorf("%s", text))
			events <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				MessageId:  msg.ID,
				Content:    err.Error(),
				Error:      err,
				IsComplete: true,
			}
			return
		}
		events <- tacklr.LLMResponseChunk{
			Type:       tacklr.StreamEventMessage,
			MessageId:  msg.ID,
			IsComplete: true,
		}
	case "function_call":
		s.emitFunctionCallChunk(raw, events)
	case "reasoning":
		s.emitReasoningChunk(raw, events, reasoningStreamed)
	}
}

func isRefusalMessage(parts []struct {
	Type    string `json:"type"`
	Refusal string `json:"refusal"`
	Text    string `json:"text"`
}) bool {
	if len(parts) == 0 {
		return false
	}
	sawRefusal := false
	for _, p := range parts {
		switch p.Type {
		case "refusal":
			sawRefusal = true
		case "output_text":
			if strings.TrimSpace(p.Text) != "" {
				return false
			}
		}
	}
	return sawRefusal
}

func refusalText(parts []struct {
	Type    string `json:"type"`
	Refusal string `json:"refusal"`
	Text    string `json:"text"`
}) string {
	for _, p := range parts {
		if p.Type == "refusal" && p.Refusal != "" {
			return p.Refusal
		}
	}
	return "model refused"
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
	// llama.cpp (and some local servers) only set call_id; OpenAI often sets both.
	// Normalize so ACP toolCallId / harness CurrentToolCallID are never empty when
	// either field is present.
	id := fc.ID
	callID := fc.CallID
	if id == "" {
		id = callID
	}
	if callID == "" {
		callID = id
	}
	events <- tacklr.LLMResponseChunk{
		Type: tacklr.StreamEventFunctionCall,
		ToolCalls: []tacklr.ToolCall{{
			ID:        id,
			Type:      "function_call",
			CallID:    callID,
			Name:      name,
			Namespace: namespace,
			Arguments: fc.Arguments,
			Status:    fc.Status,
		}},
		IsComplete: fc.Status == "completed",
	}
}

func (s *OpenAIInferenceStrategy) emitReasoningChunk(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk, reasoningStreamed map[string]struct{}) {
	var reasoning struct {
		ID               string `json:"id"`
		EncryptedContent string `json:"encrypted_content"`
		Summary          []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"summary"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &reasoning); err != nil {
		return
	}
	// Prefer client-visible summary; fall back to plain reasoning_text when present.
	var text strings.Builder
	for _, p := range reasoning.Summary {
		if p.Text == "" {
			continue
		}
		if p.Type == "" || p.Type == "summary_text" {
			text.WriteString(p.Text)
		}
	}
	if text.Len() == 0 {
		for _, p := range reasoning.Content {
			if p.Text == "" {
				continue
			}
			if p.Type == "" || p.Type == "reasoning_text" {
				text.WriteString(p.Text)
			}
		}
	}
	// If deltas already went to ACP as thought chunks, only signal completion
	// (empty Content) so Zed does not render the full summary a second time.
	content := ""
	if text.Len() > 0 {
		if _, streamed := reasoningStreamed[reasoning.ID]; !streamed {
			content = text.String()
		}
	}
	events <- tacklr.LLMResponseChunk{
		Type:             tacklr.StreamEventReasoning,
		MessageId:        reasoning.ID,
		Content:          content,
		EncryptedContent: reasoning.EncryptedContent,
		IsComplete:       true,
	}
}

func marshalMessagesToInput(messages []*tacklr.Message) []json.RawMessage {
	// Responses API requires each function_call to be immediately followed by its
	// function_call_output (same call_id). Parallel tool batches must interleave
	// call→output, not emit all calls then all outputs.
	paired := make(map[*tacklr.Message]bool)
	var items []json.RawMessage

	appendJSON := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			panic(fmt.Sprintf("inference: marshal internally constructed model input: %v", err))
		}
		items = append(items, b)
	}

	takeToolOutput := func(tc tacklr.ToolCall) *tacklr.Message {
		wire, key := tc.WireID(), tc.Key()
		if wire == "" && key == "" {
			return nil
		}
		var byKey *tacklr.Message
		for _, m := range messages {
			if m == nil || m.Role != tacklr.RoleTool || paired[m] {
				continue
			}
			// Prefer the Responses call_id. A leftover result keyed by the
			// provider item id (fc_…) must not steal a later call_ result.
			if wire != "" && m.ToolCallID == wire {
				paired[m] = true
				return m
			}
			if byKey == nil && key != "" && m.ToolCallID == key {
				byKey = m
			}
		}
		if byKey != nil {
			paired[byKey] = true
			return byKey
		}
		return nil
	}

	appendFunctionCall := func(tc tacklr.ToolCall) {
		callID := tc.WireID()
		name := tc.Name
		if tc.Namespace != "" && name != "" && !strings.Contains(name, ".") {
			name = tc.Namespace + "." + name
		}
		args := tc.Arguments
		if args == "" {
			args = "{}"
		}
		status := string(tacklr.StatusCompleted)
		if tc.Status == string(tacklr.StatusInProgress) || tc.Status == string(tacklr.StatusIncomplete) {
			status = tc.Status
		}
		appendJSON(functionCallInputRequest{
			Type:      "function_call",
			CallID:    callID,
			Name:      name,
			Arguments: args,
			Status:    status,
		})
		if out := takeToolOutput(tc); out != nil {
			appendJSON(functionCallOutputRequest{
				Type:   "function_call_output",
				CallID: callID,
				Output: out.Content,
				Status: string(tacklr.StatusCompleted),
			})
		}
	}

	for _, msg := range messages {
		if msg == nil {
			continue
		}
		switch msg.Role {
		case tacklr.RoleTool:
			if paired[msg] {
				continue
			}
			// Unmatched function_call_output is an invalid_request_error
			// ("No tool call found for function call output"). Drop it.

		case tacklr.RoleUser, tacklr.RoleSystem:
			appendJSON(marshalRoleContent(string(msg.Role), msg))

		case tacklr.RoleDeveloper:
			// Wire as system so models treat handoff/plan as instructions, not a
			// conversational turn to answer (Foundry/DeepSeek was echoing
			// developer-role handoff text into agent_message_chunk).
			appendJSON(marshalRoleContent(string(tacklr.RoleSystem), msg))

		case tacklr.RoleReasoning:
			// Responses multi-turn: pass prior reasoning items back with the tool
			// turn. `summary` is required (empty array when none). Do not send
			// status — that is an output field and Azure rejects it on input.
			// Harness Message.Content holds streamed thought; map to summary_text.
			//
			// `id` without encrypted_content is a provider store lookup. OpenAI
			// ZDR and Azure (store=false) then 400 "Item with id 'rs_…' not found".
			// Only send id when we have the ciphertext from include=.
			item := reasoningInputRequest{
				Type:    "reasoning",
				Summary: []reasoningSummaryPart{},
			}
			if enc := strings.TrimSpace(msg.EncryptedContent); enc != "" {
				item.ID = msg.MessageID
				item.EncryptedContent = enc
			}
			if text := strings.TrimSpace(msg.Content); text != "" {
				item.Summary = []reasoningSummaryPart{{
					Type: "summary_text",
					Text: text,
				}}
			}
			appendJSON(item)

		case tacklr.RoleAssistant:
			if msg.Content != "" || len(msg.ContentParts) > 0 {
				appendJSON(marshalRoleContent(string(msg.Role), msg))
			}
			for _, tc := range msg.ToolCalls {
				appendFunctionCall(tc)
			}
		}
	}

	return items
}

// marshalRoleContent uses string content when there are no multimodal parts;
// otherwise Responses array content (input_text / input_image / input_file).
func marshalRoleContent(role string, msg *tacklr.Message) any {
	if msg == nil {
		return easyInputRequest{Role: role, Content: ""}
	}
	if len(msg.ContentParts) == 0 {
		return easyInputRequest{Role: role, Content: msg.Content}
	}
	parts := make([]any, 0, len(msg.ContentParts)+1)
	if t := strings.TrimSpace(msg.Content); t != "" {
		parts = append(parts, inputTextPart{Type: "input_text", Text: t})
	}
	for _, p := range msg.ContentParts {
		switch p.Type {
		case tacklr.ContentTypeInputImage:
			if p.ImageURL == nil || p.ImageURL.URL == "" {
				continue
			}
			parts = append(parts, inputImagePart{
				Type:     "input_image",
				ImageURL: p.ImageURL.URL,
				Detail:   p.ImageURL.Detail,
			})
		case tacklr.ContentTypeInputFile:
			if p.FileData == nil || p.FileData.Data == "" {
				continue
			}
			fd := p.FileData
			filename := fd.Filename
			if filename == "" {
				filename = "document.pdf"
			}
			fileData := fd.Data
			if !strings.HasPrefix(fileData, "data:") {
				fileData = streaming.DataURL(fd.MIMEType, fileData)
			}
			parts = append(parts, inputFilePart{
				Type:     "input_file",
				Filename: filename,
				FileData: fileData,
				FileID:   fd.FileID,
			})
		}
	}
	if len(parts) == 0 {
		return easyInputRequest{Role: role, Content: msg.Content}
	}
	return multiInputRequest{Role: role, Content: parts}
}

// summarizeInputItems is a short, safe log line of request input shape (types,
// call_ids) for diagnosing provider terminal failures.
func summarizeInputItems(items []json.RawMessage) string {
	if len(items) == 0 {
		return "empty"
	}
	parts := make([]string, 0, len(items))
	for i, raw := range items {
		var head struct {
			Type   string `json:"type"`
			Role   string `json:"role"`
			CallID string `json:"call_id"`
			Status string `json:"status"`
			Name   string `json:"name"`
			ID     string `json:"id"`
		}
		_ = json.Unmarshal(raw, &head)
		kind := head.Type
		if kind == "" {
			kind = "message:" + head.Role
		}
		extra := ""
		if head.CallID != "" {
			extra += " call_id=" + head.CallID
		}
		if head.Name != "" {
			extra += " name=" + head.Name
		}
		if head.Status != "" {
			extra += " status=" + head.Status
		}
		if head.ID != "" {
			extra += " id=" + head.ID
		}
		parts = append(parts, fmt.Sprintf("%d:%s%s", i, kind, extra))
		if i >= 24 {
			parts = append(parts, fmt.Sprintf("…+%d more", len(items)-i-1))
			break
		}
	}
	return strings.Join(parts, "; ")
}
