package openai

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
	"strings"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Model holds only model and endpoint settings; each
// request carries its own prompt and cache key, so one is safe to share.
type Model struct {
	apiKey string
	model  string
	// reasoning effort: "low" | "medium" | "high" (provider-specific).
	reasoning string
	// reasoningSummary: "auto" | "concise" | "detailed". Empty omits the field.
	// Required for Azure OpenAI/Foundry to stream response.reasoning_summary_text.delta.
	reasoningSummary string
	httpClient       *http.Client
	baseURL          string
	// promptCacheFor overrides cache-profile resolution when non-nil.
	// NewOpenRouter pins OpenRouter's key-only profile here.
	promptCacheFor func(model, baseURL, key string) promptCache
}

var (
	// ErrIncompleteStream means a provider closed without a terminal response event.
	ErrIncompleteStream = errors.New("incomplete provider stream")
	// ErrMalformedStream means a provider emitted invalid SSE event JSON.
	ErrMalformedStream = errors.New("malformed provider stream")

	_ tacklr.Model = (*Model)(nil)
)

// promptCache resolves the cache profile for this strategy's model and
// endpoint and the request's cache key (prompt_cache_key, and
// x-grok-conv-id on xAI). A strategy may pin its own profile.
func (s *Model) promptCache(key string) promptCache {
	key = strings.TrimSpace(key)
	if s.promptCacheFor != nil {
		return s.promptCacheFor(s.model, s.baseURL, key)
	}
	return newPromptCache(s.model, s.baseURL, key)
}

func New(client *http.Client) *Model {
	if client == nil {
		client = http.DefaultClient
	}
	return &Model{
		httpClient: client,
		baseURL:    "https://api.openai.com/v1",
	}
}

func (s *Model) WithApiKey(key string) *Model {
	s.apiKey = key
	return s
}

func (s *Model) WithModel(model string) *Model {
	s.model = model
	return s
}

// ModelTelemetryIdentity names the provider and model for model spans.
func (s *Model) ModelTelemetryIdentity() telemetry.ModelIdentity {
	return telemetry.NewModelIdentity(s.model, s.baseURL)
}

func (s *Model) WithURL(url string) *Model {
	s.baseURL = url
	return s
}

func (s *Model) WithReasoningLevel(level string) *Model {
	s.reasoning = level
	// Default summary so Azure OpenAI / OpenAI stream thought deltas as
	// response.reasoning_summary_text.delta (mapped to session.EventReasoning).
	if level != "" && s.reasoningSummary == "" {
		s.reasoningSummary = "auto"
	}
	return s
}

// WithReasoningSummary sets reasoning.summary on Responses API requests
// ("auto", "concise", "detailed"). Empty clears it.
func (s *Model) WithReasoningSummary(summary string) *Model {
	s.reasoningSummary = summary
	return s
}

// SupportsMIME implements tacklr.Model for the selected model id.
func (s *Model) SupportsMIME(mimeType string) bool {
	return modelSupportsMIME(s.model, mimeType)
}

func (s *Model) MaxContextWindow() (int, error) {
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

func (s *Model) CountTokens(ctx context.Context, req tacklr.ModelRequest) (int, error) {
	if s.apiKey == "" {
		return 0, tacklr.ErrApiKeyNotSet
	}
	if s.model == "" {
		return 0, tacklr.ErrModelNotSet
	}

	cache := s.promptCache(req.CacheKey)
	_, inputJSON, toolsJSON := requestInput(req, cache)
	// It encodes: its fields are JSON that json.Marshal built.
	body, _ := json.Marshal(countTokensRequest{
		Model: s.model,
		Input: inputJSON,
		Tools: toolsJSON,
	})

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.baseURL, "/")+"/responses/input_tokens", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build token-count request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.apiKey)

	httpResp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return 0, tacklr.Network(fmt.Errorf("token-count request failed: %w", err))
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return 0, tacklr.Network(fmt.Errorf("read response: %w", err))
	}

	if httpResp.StatusCode != http.StatusOK {
		err := classifyAPIStatus(&APIStatusError{Status: httpResp.StatusCode, Body: extractErrorMessage(respBody)}, "")
		switch httpResp.StatusCode {
		case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
			// A provider without the endpoint (Azure OpenAI, local servers):
			// the harness sizes requests from the usage it reports.
			return 0, fmt.Errorf("openai: the provider at %s has no /responses/input_tokens endpoint: %w: %w", s.baseURL, tacklr.ErrCannotCount, err)
		}
		return 0, err
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

func (s *Model) Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error) {
	if s.apiKey == "" {
		return nil, tacklr.ErrApiKeyNotSet
	}
	if s.model == "" {
		return nil, tacklr.ErrModelNotSet
	}

	cache := s.promptCache(req.CacheKey)
	items, inputJSON, toolsJSON := requestInput(req, cache)

	reqBody := responsesRequest{
		Model:   s.model,
		Input:   inputJSON,
		Tools:   toolsJSON,
		Stream:  true,
		Include: []string{"reasoning.encrypted_content"},
	}
	if tacklr.ToolChoiceNone(ctx) {
		reqBody.ToolChoice = "none"
	}
	cache.apply(&reqBody)

	if s.reasoning != "" || s.reasoningSummary != "" {
		reqBody.Reasoning = &reasoningDetail{
			Effort:  s.reasoning,
			Summary: s.reasoningSummary,
		}
	}

	// It encodes: its raw fields are JSON that json.Marshal built.
	body, _ := json.Marshal(reqBody)
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
		sendChunk(tacklr.LLMResponseChunk{
			Type:       session.EventError,
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
		cache.headers(httpReq.Header)

		httpResp, err := s.httpClient.Do(httpReq)
		if err != nil {
			err = tacklr.Network(fmt.Errorf("model provider request failed: %w", err))
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

// requestInput is req's Responses input items, the items as one JSON array,
// and its tools as JSON (nil when it has none). Each item is JSON that
// json.Marshal built, so the array always encodes.
func requestInput(req tacklr.ModelRequest, cache promptCache) (items []json.RawMessage, input, tools json.RawMessage) {
	items = marshalMessagesToInput(req.Messages, req.SystemPrompt, cache.breakpoints())
	input, _ = json.Marshal(items)
	if len(req.Tools) > 0 {
		tools = json.RawMessage(tacklr.ToolsAsJson(req.Tools))
	}
	return items, input, tools
}

func (s *Model) parseSSEResponse(ctx context.Context, body io.Reader, events chan<- tacklr.LLMResponseChunk, inputSummary string) {
	// Match main-branch classification: output_text is always a message chunk.
	// DeepSeek on Foundry streams thinking inside output_text (e.g. <think> tags);
	// reclassifying those deltas as session.EventReasoning broke ACP clients that
	// already handled thinking correctly from agent_message_chunk on main.
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	var currentItemID string
	terminal := false
	emitFailure := func(err error, detail string) {
		emitProviderFailed(ctx, err, http.StatusOK, inputSummary, detail)
		events <- tacklr.LLMResponseChunk{
			Type:       session.EventError,
			Content:    err.Error(),
			Error:      err,
			IsComplete: true,
		}
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
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
					Type:       session.EventMessage,
					MessageId:  msgID,
					Content:    evt.Delta,
					IsComplete: false,
				}
			}
		case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
			// Explicit reasoning channels (OpenAI/Azure o-series style) → thought.
			// Keep summary as additive; do not invent reclassification of output_text.
			if evt.Delta != "" {
				events <- tacklr.LLMResponseChunk{
					Type:       session.EventReasoning,
					MessageId:  msgID,
					Content:    evt.Delta,
					IsComplete: false,
				}
			}
		case "response.output_item.done":
			if evt.Item != nil {
				s.emitOutputItemComplete(evt.Item, events)
			}
		case "error":
			// Azure mid-stream errors often use type=error with HTTP 200.
			// An error event is always terminal and always classified.
			_, classified := classifyTerminalSSE("error", data)
			emitProviderFailed(ctx, classified, 200, inputSummary, data)
			events <- tacklr.LLMResponseChunk{
				Type:       session.EventError,
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
					Type:       session.EventError,
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
						Type:             session.EventComplete,
						IsComplete:       true,
						InputTokens:      u.Input,
						OutputTokens:     u.Output,
						ReasoningTokens:  u.Reasoning,
						CachedTokens:     u.Cached,
						CacheWriteTokens: u.CacheWrite,
					}
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return
	}
	if err := scanner.Err(); err != nil {
		emitFailure(tacklr.Network(fmt.Errorf("%w: %w", ErrIncompleteStream, err)), "")
		return
	}
	if !terminal {
		emitFailure(tacklr.Network(ErrIncompleteStream), "provider stream closed before a terminal response event")
	}
}

// responseUsage is the token counts we care about from Responses API usage.
type responseUsage struct {
	Input      int
	Output     int
	Reasoning  int
	Cached     int
	CacheWrite int
}

// parseResponseUsage extracts usage from a response.completed SSE payload.
func parseResponseUsage(data string) (responseUsage, bool) {
	var payload struct {
		Response struct {
			Usage *struct {
				InputTokens        int `json:"input_tokens"`
				OutputTokens       int `json:"output_tokens"`
				InputTokensDetails *struct {
					CachedTokens     int `json:"cached_tokens"`
					CacheWriteTokens int `json:"cache_write_tokens"`
				} `json:"input_tokens_details"`
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
	} else {
		out.Reasoning = u.ReasoningTokens
	}
	if u.InputTokensDetails != nil {
		out.Cached = u.InputTokensDetails.CachedTokens
		out.CacheWrite = u.InputTokensDetails.CacheWriteTokens
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
		return true, classifyIncompleteReason(reason)
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
		// A reason returned above, so the body names only the status.
		if status != "" {
			parts = append(parts, "status="+status)
		}
		if evtType != "" {
			parts = append(parts, "event="+evtType)
		}
		apiErr.Body = strings.Join(parts, "; ")
	}
	classified := classifyAPIStatus(apiErr, "")
	// Known stop-reason sentinels are already actionable; skip noisy raw dumps.
	if errors.Is(classified, session.ErrModelRefused) || errors.Is(classified, session.ErrMaxTokens) {
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
		if errors.As(err, &api) {
			code = api.Code
		}
	}
	attrs := []attribute.KeyValue{
		attribute.String(telemetry.EventAttrInputItems, inputItems),
		attribute.String(telemetry.AttrErrorClass, telemetry.ClassifyErrorClass(err, httpStatus)),
	}
	if httpStatus > 0 {
		attrs = append(attrs, attribute.Int(telemetry.AttrHTTPStatus, httpStatus))
	}
	if code != "" {
		attrs = append(attrs, attribute.String(telemetry.AttrErrorCode, code))
	}
	telemetry.EmitEventSeverity(ctx, telemetry.EventProviderFailed, log.SeverityError, attrs...)
}

func (s *Model) emitOutputItemComplete(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
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
			err := fmt.Errorf("%w: %s", session.ErrModelRefused, text)
			events <- tacklr.LLMResponseChunk{
				Type:       session.EventError,
				MessageId:  msg.ID,
				Content:    err.Error(),
				Error:      err,
				IsComplete: true,
			}
			return
		}
		var text strings.Builder
		for _, c := range msg.Content {
			if c.Type == "output_text" {
				text.WriteString(c.Text)
			}
		}
		events <- tacklr.LLMResponseChunk{
			Type:       session.EventMessage,
			MessageId:  msg.ID,
			Content:    text.String(),
			IsComplete: true,
		}
	case "function_call":
		s.emitFunctionCallChunk(raw, events)
	case "reasoning":
		s.emitReasoningChunk(raw, events)
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

func (s *Model) emitFunctionCallChunk(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
	var fc struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &fc); err != nil {
		return
	}
	namespace, name := "", fc.Name
	if ns, n, ok := strings.Cut(fc.Name, "__"); ok {
		namespace, name = ns, n
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
		Type: session.EventFunctionCall,
		ToolCalls: []message.ToolCall{{
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

func (s *Model) emitReasoningChunk(raw json.RawMessage, events chan<- tacklr.LLMResponseChunk) {
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
	events <- tacklr.LLMResponseChunk{
		Type:             session.EventReasoning,
		MessageId:        reasoning.ID,
		Content:          text.String(),
		EncryptedContent: reasoning.EncryptedContent,
		IsComplete:       true,
	}
}

func marshalMessagesToInput(messages []*message.Message, systemPrompt string, stampBreakpoints bool) []json.RawMessage {
	// Responses API requires each function_call to be immediately followed by its
	// function_call_output (same call_id). Parallel tool batches must interleave
	// call→output, not emit all calls then all outputs.
	paired := make(map[*message.Message]bool)
	var items []json.RawMessage
	lastToolOut := -1

	// v is an input type of this package, which always encodes.
	appendJSON := func(v any) {
		b, _ := json.Marshal(v)
		items = append(items, b)
	}

	if text := strings.TrimSpace(systemPrompt); text != "" {
		if stampBreakpoints {
			appendJSON(multiInputRequest{
				Role: string(message.RoleDeveloper),
				Content: []any{inputTextPart{
					Type: "input_text", Text: text,
					PromptCacheBreakpoint: &promptCacheBreakpoint{Mode: "explicit"},
				}},
			})
		} else {
			appendJSON(easyInputRequest{Role: string(message.RoleDeveloper), Content: text})
		}
	}

	takeToolOutput := func(tc message.ToolCall) *message.Message {
		wire, key := tc.WireID(), tc.Key()
		var byKey *message.Message
		for _, m := range messages {
			if m == nil || m.Role != message.RoleTool || paired[m] {
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

	appendFunctionCall := func(tc message.ToolCall) {
		callID := tc.WireID()
		name := tc.Name
		if tc.Namespace != "" {
			name = tc.Namespace + "__" + tc.Name
		}
		args := tc.Arguments
		if args == "" {
			args = "{}"
		}
		status := string(message.StatusCompleted)
		if tc.Status == string(message.StatusInProgress) || tc.Status == string(message.StatusIncomplete) {
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
				Status: string(message.StatusCompleted),
			})
			lastToolOut = len(items) - 1
		}
	}

	for _, msg := range messages {
		if msg == nil {
			continue
		}
		switch msg.Role {
		case message.RoleTool:
			if paired[msg] {
				continue
			}
			// Unmatched function_call_output is an invalid_request_error
			// ("No tool call found for function call output"). Drop it.

		case message.RoleUser, message.RoleSystem:
			appendJSON(marshalRoleContent(string(msg.Role), msg))

		case message.RoleDeveloper:
			if stampBreakpoints && tacklr.IsPlanDocument(msg) {
				appendJSON(multiInputRequest{
					Role: string(message.RoleDeveloper),
					Content: []any{inputTextPart{
						Type: "input_text", Text: msg.Content,
						PromptCacheBreakpoint: &promptCacheBreakpoint{Mode: "explicit"},
					}},
				})
			} else {
				appendJSON(marshalRoleContent(string(message.RoleDeveloper), msg))
			}

		case message.RoleReasoning:
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

		case message.RoleAssistant:
			if msg.Content != "" || len(msg.ContentParts) > 0 {
				appendJSON(marshalRoleContent(string(msg.Role), msg))
			}
			for _, tc := range msg.ToolCalls {
				appendFunctionCall(tc)
			}
		}
	}

	if stampBreakpoints && lastToolOut >= 0 {
		var out functionCallOutputRequest
		if err := json.Unmarshal(items[lastToolOut], &out); err == nil {
			text, _ := out.Output.(string)
			out.Output = []inputTextPart{{
				Type: "input_text", Text: text,
				PromptCacheBreakpoint: &promptCacheBreakpoint{Mode: "explicit"},
			}}
			b, err := json.Marshal(out)
			if err == nil {
				items[lastToolOut] = b
			}
		}
	}

	return items
}

// marshalRoleContent uses string content when there are no multimodal parts;
// otherwise Responses array content (input_text / input_image / input_file).
func marshalRoleContent(role string, msg *message.Message) any {
	if len(msg.ContentParts) == 0 {
		return easyInputRequest{Role: role, Content: msg.Content}
	}
	parts := make([]any, 0, len(msg.ContentParts)+1)
	if t := strings.TrimSpace(msg.Content); t != "" {
		parts = append(parts, inputTextPart{Type: "input_text", Text: t})
	}
	for _, p := range msg.ContentParts {
		switch p.Type {
		case message.ContentTypeInputImage:
			if p.ImageURL == nil || p.ImageURL.URL == "" {
				continue
			}
			parts = append(parts, inputImagePart{
				Type:     "input_image",
				ImageURL: p.ImageURL.URL,
				Detail:   p.ImageURL.Detail,
			})
		case message.ContentTypeInputFile:
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
				fileData = message.DataURL(fd.MIMEType, fileData)
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

// openRouterBaseURL is OpenRouter's OpenAI-compatible base URL. The strategy
// appends the Responses API paths (/responses, /responses/input_tokens) to it.
const openRouterBaseURL = "https://openrouter.ai/api/v1"

// NewOpenRouter returns an OpenAI Responses strategy
// preconfigured for OpenRouter.
//
// OpenRouter validates prompt_cache_options.mode and rejects "implicit", so the
// strategy pins a key-only cache profile: prompt_cache_key is sent, while the
// GPT-5.6 cache options and content-block breakpoints stay off the wire.
func NewOpenRouter(client *http.Client) *Model {
	s := New(client).WithURL(openRouterBaseURL)
	s.promptCacheFor = func(_, _, key string) promptCache {
		return openRouterCache{key: key}
	}
	return s
}

// openRouterCache sends prompt_cache_key only. OpenRouter rejects the GPT-5.6
// prompt_cache_options payload, and breakpoints are part of that same policy.
type openRouterCache struct{ key string }

func (c openRouterCache) apply(req *responsesRequest) { req.PromptCacheKey = c.key }
func (openRouterCache) headers(http.Header)           {}
func (openRouterCache) breakpoints() bool             { return false }

// Model media capability is prefix match against the selected model id.
// Unknown models accept text only (safe default for ACP ads and prompt reject).

// Prefixes that imply vision / image input on OpenAI-compatible stacks.
var visionModelPrefixes = []string{
	"gpt-4o",
	"gpt-4.1",
	"gpt-4-turbo",
	"gpt-4-vision",
	"gpt-5",
	"o1",
	"o3",
	"o4",
	"computer-use",
}

// Model prefixes that accept PDF / document file input (Responses input_file).
var pdfModelPrefixes = []string{
	"gpt-4o",
	"gpt-4.1",
	"gpt-5",
	"o1",
	"o3",
	"o4",
}

func modelHasPrefix(model string, prefixes []string) bool {
	model = strings.TrimSpace(strings.ToLower(model))
	if model == "" {
		return false
	}
	for _, p := range prefixes {
		if strings.HasPrefix(model, p) {
			return true
		}
	}
	return false
}

// modelSupportsMIME is the OpenAI-strategy capability matrix for a model id.
func modelSupportsMIME(model, mimeType string) bool {
	mime := message.NormalizeMIME(mimeType)
	if message.IsTextMIME(mime) {
		return true
	}
	if strings.HasPrefix(mime, "image/") {
		return modelHasPrefix(model, visionModelPrefixes)
	}
	if mime == "application/pdf" {
		return modelHasPrefix(model, pdfModelPrefixes)
	}
	return false
}
