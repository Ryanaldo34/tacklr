package agentbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// OpenAIEmbedder implements brain.QueryEmbedder via POST {baseURL}/embeddings
// (OpenAI-compatible, including Azure OpenAI with base …/openai/v1).
type OpenAIEmbedder struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

// DefaultEmbedModel is used when OPENAI_EMBEDDING_MODEL / -embed-model is unset.
const DefaultEmbedModel = "text-embedding-3-small"

// Embed returns a dense vector for text. Empty text returns nil, nil (Put skips embed).
func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil
	}
	if e == nil {
		return nil, fmt.Errorf("agentbench: embedder is nil")
	}
	model := strings.TrimSpace(e.Model)
	if model == "" {
		model = DefaultEmbedModel
	}
	base := strings.TrimRight(strings.TrimSpace(e.BaseURL), "/")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	client := e.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	body, err := json.Marshal(map[string]any{
		"model": model,
		"input": text,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := strings.TrimSpace(e.APIKey); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("agentbench: embed request: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("agentbench: embed read: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("agentbench: embed HTTP %d: %s", res.StatusCode, truncate(string(raw), 300))
	}

	var parsed struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("agentbench: embed decode: %w", err)
	}
	if len(parsed.Data) == 0 || len(parsed.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("agentbench: embed response empty")
	}
	src := parsed.Data[0].Embedding
	out := make([]float32, len(src))
	for i, v := range src {
		out[i] = float32(v)
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
