// Package anthropic is the tacklr.Model for Claude models on the Claude API
// (the Messages API). It streams each request, replays thinking blocks
// unchanged, counts tokens with the API's count endpoint, and caches the
// stable prefix of each request.
package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/provider"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const (
	defaultBaseURL = "https://api.anthropic.com/v1"
	// apiVersion is the anthropic-version every request sends.
	apiVersion = "2026-10-21"
	// defaultMaxTokens bounds a response when the host sets no limit. The
	// request streams, so a large limit does not time out.
	defaultMaxTokens = 64000
)

var (
	// ErrIncompleteStream is a stream that closed before message_stop.
	ErrIncompleteStream = provider.ErrIncompleteStream
	// ErrMalformedStream is a stream event that is not valid JSON.
	ErrMalformedStream = provider.ErrMalformedStream

	_ tacklr.Model = (*Model)(nil)
)

// APIStatusError is an HTTP error answer from the API. Find it in a
// wrapped chain with errors.As.
type APIStatusError = provider.APIStatusError

// Model holds only model and endpoint settings. Each request carries its
// own prompt, so one Model is safe to share across sessions.
type Model struct {
	httpClient    *http.Client
	baseURL       string
	apiKey        string
	model         string
	effort        string
	maxTokens     int
	contextWindow int
}

// New is a client over client, whose requests carry their trace context
// (W3C traceparent). A nil client is http.DefaultClient's settings.
func New(client *http.Client) *Model {
	c := http.Client{}
	if client != nil {
		c = *client
	}
	c.Transport = telemetry.Transport(c.Transport)
	return &Model{httpClient: &c, baseURL: defaultBaseURL}
}

// WithApiKey sets the API key, sent as x-api-key.
func (m *Model) WithApiKey(key string) *Model {
	m.apiKey = key
	return m
}

// WithModel sets the model id, such as claude-opus-5-5.
func (m *Model) WithModel(model string) *Model {
	m.model = model
	return m
}

// WithURL sets the API's base URL: a gateway in front of the Claude API,
// or another Messages API, such as Microsoft Foundry's
// https://<resource>.services.ai.azure.com/anthropic/v1. The default is
// https://api.anthropic.com/v1.
func (m *Model) WithURL(url string) *Model {
	m.baseURL = url
	return m
}

// WithEffort sets output_config.effort: low, medium, high, xhigh or max.
// Empty sends none, so the model's default applies. Defaults differ by
// model: claude-opus-5-5 defaults to medium.
func (m *Model) WithEffort(effort string) *Model {
	m.effort = effort
	return m
}

// WithMaxTokens sets max_tokens, the limit on each response. The default
// is 64,000.
func (m *Model) WithMaxTokens(n int) *Model {
	m.maxTokens = n
	return m
}

// WithContextWindow sets the model's context window in tokens, for a model
// id the client does not know, such as a Foundry deployment name.
func (m *Model) WithContextWindow(n int) *Model {
	m.contextWindow = n
	return m
}

// ModelTelemetryIdentity names the provider and model for model spans.
func (m *Model) ModelTelemetryIdentity() telemetry.ModelIdentity {
	return telemetry.ModelIdentity{Provider: telemetry.GenAIProviderAnthropic, Model: m.model}
}

// MaxContextWindow is the window WithContextWindow set, else the known
// window of the model id.
func (m *Model) MaxContextWindow() (int, error) {
	if m.contextWindow > 0 {
		return m.contextWindow, nil
	}
	if !isClaude(m.model) {
		return 0, fmt.Errorf("max context window: unknown model %q: %w", m.model, tacklr.ErrUnknownModel)
	}
	if hasPrefix(m.model, smallWindowModels) {
		return 200_000, nil
	}
	return 1_000_000, nil
}

// SupportsMIME reports the input types every Claude model accepts: text,
// JPEG, PNG, GIF and WebP images, and PDF documents. Any model id works,
// such as a Foundry deployment name: a Messages API serves Claude models.
func (m *Model) SupportsMIME(mimeType string) bool {
	switch mime := message.NormalizeMIME(mimeType); mime {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "application/pdf":
		return true
	default:
		return message.IsTextMIME(mime)
	}
}

// CountTokens counts req's input tokens with the count endpoint. A gateway
// without the endpoint (HTTP 404) fails with tacklr.ErrCannotCount.
func (m *Model) CountTokens(ctx context.Context, req tacklr.ModelRequest) (int, error) {
	if err := m.ready(); err != nil {
		return 0, err
	}
	body, _ := json.Marshal(m.request(ctx, req).counted())
	resp, err := provider.Post(ctx, m.httpClient, m.url("/messages/count_tokens"), m.header(), body)
	if err != nil {
		return 0, fmt.Errorf("count tokens: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, tacklr.Network(fmt.Errorf("read token count: %w", err))
	}
	if resp.StatusCode != http.StatusOK {
		failed := classifyStatus(resp.StatusCode, raw)
		if resp.StatusCode == http.StatusNotFound {
			return 0, fmt.Errorf("anthropic: the API at %s has no count_tokens endpoint: %w: %w", m.baseURL, tacklr.ErrCannotCount, failed)
		}
		return 0, failed
	}
	var count struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.Unmarshal(raw, &count); err != nil {
		return 0, fmt.Errorf("decode token count: %w", err)
	}
	return count.InputTokens, nil
}

// Invoke streams one response to req.
func (m *Model) Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error) {
	if err := m.ready(); err != nil {
		return nil, err
	}
	wire := m.request(ctx, req)
	body, _ := json.Marshal(wire)
	summary := wire.summary()
	chunks := provider.NewChunks(ctx)
	go func() {
		defer close(chunks.C)
		resp, err := provider.Post(ctx, m.httpClient, m.url("/messages"), m.header(), body)
		if err != nil {
			slog.ErrorContext(ctx, "model provider request failed", "error", err)
			chunks.Fail(err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			raw, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				slog.WarnContext(ctx, "could not read provider error body", "error", readErr, "http_status", resp.StatusCode)
			}
			failed := classifyStatus(resp.StatusCode, raw)
			provider.EmitFailed(ctx, failed, resp.StatusCode, summary)
			chunks.Fail(failed)
			return
		}
		(&stream{ctx: ctx, chunks: chunks, summary: summary}).read(resp.Body)
	}()
	return chunks.C, nil
}

func (m *Model) ready() error {
	if m.apiKey == "" {
		return tacklr.ErrApiKeyNotSet
	}
	if m.model == "" {
		return tacklr.ErrModelNotSet
	}
	return nil
}

func (m *Model) url(path string) string {
	return strings.TrimRight(m.baseURL, "/") + path
}

func (m *Model) header() http.Header {
	return http.Header{"X-Api-Key": {m.apiKey}, "Anthropic-Version": {apiVersion}}
}

// request is req on the wire, with this model's settings.
func (m *Model) request(ctx context.Context, req tacklr.ModelRequest) request {
	r := request{
		Model:        m.model,
		MaxTokens:    m.maxTokens,
		Stream:       true,
		System:       systemBlocks(req.SystemPrompt),
		Tools:        toolDefinitions(req.Tools),
		Messages:     turns(req.Messages),
		CacheControl: &ephemeral,
	}
	if r.MaxTokens <= 0 {
		r.MaxTokens = defaultMaxTokens
	}
	if tacklr.ToolChoiceNone(ctx) && len(r.Tools) > 0 {
		r.ToolChoice = &toolChoice{Type: "none"}
	}
	if !hasPrefix(m.model, noAdaptiveThinkingModels) {
		// Summarized, so the reasoning streams as text; the default omits it.
		r.Thinking = &thinking{Type: "adaptive", Display: "summarized"}
	}
	if m.effort != "" {
		r.OutputConfig = &outputConfig{Effort: m.effort}
	}
	return r
}

// smallWindowModels have a 200K-token context window. Later models have
// 1M.
var smallWindowModels = []string{"claude-3", "claude-haiku-4-5", "claude-sonnet-4-5", "claude-opus-4-5", "claude-opus-4-1", "claude-opus-4-0", "claude-sonnet-4-0", "claude-opus-4-2", "claude-sonnet-4-2"}

// noAdaptiveThinkingModels predate adaptive thinking. Requests to them
// send no thinking configuration.
var noAdaptiveThinkingModels = smallWindowModels

// modelName is the model id after a gateway's prefix (vendor/model).
func modelName(model string) string {
	return strings.ToLower(model[strings.LastIndex(model, "/")+1:])
}

func isClaude(model string) bool {
	return strings.HasPrefix(modelName(model), "claude-")
}

func hasPrefix(model string, prefixes []string) bool {
	name := modelName(model)
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}
