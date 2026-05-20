package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RawAPIMessage struct {
	Message    []byte
	Error      error
	IsComplete bool
}

type LLMResponseChunk struct {
	TurnId     string
	MessageId  string
	ToolCalls  []ToolCall
	Type       StreamEventType
	Content    string
	IsComplete bool
}

type ResponseStrategy interface {
	Handle(ctx context.Context, payload []byte, events chan<- LLMResponseChunk) error
}

type OpenAINoStreamResponseStrategy struct {
	ApiKey     string
	BaseURL    string
	HttpClient *http.Client
}

func (s *OpenAINoStreamResponseStrategy) Handle(ctx context.Context, payload []byte, events chan<- LLMResponseChunk) error {
	defer close(events)

	if err := ctx.Err(); err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.BaseURL+"/responses", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.ApiKey)

	httpResp, err := s.HttpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if httpResp.StatusCode != 200 {
		return fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, extractErrorMessage(respBody))
	}

	// Parse the response body and emit chunks
	var apiResp struct {
		ID                string            `json:"id"`
		Object            string            `json:"object"`
		Status            string            `json:"status"`
		Output            []json.RawMessage `json:"output"`
		Error             *apiErrorDetail   `json:"error,omitempty"`
		IncompleteDetails *incompleteDetail `json:"incomplete_details,omitempty"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	if apiResp.Error != nil {
		return fmt.Errorf("API error: %s (code: %s)", apiResp.Error.Message, apiResp.Error.Code)
	}

	for _, raw := range apiResp.Output {
		var typeHolder struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &typeHolder); err != nil {
			return fmt.Errorf("parse output item type: %w", err)
		}

		switch typeHolder.Type {
		case "message":
			var rawMsg struct {
				ID      string          `json:"id"`
				Role    string          `json:"role"`
				Status  string          `json:"status"`
				Content json.RawMessage `json:"content"`
			}
			if err := json.Unmarshal(raw, &rawMsg); err != nil {
				return fmt.Errorf("parse message: %w", err)
			}

			var contentParts []struct {
				Type    string `json:"type"`
				Text    string `json:"text,omitempty"`
				Refusal string `json:"refusal,omitempty"`
			}
			if err := json.Unmarshal(rawMsg.Content, &contentParts); err == nil {
				for _, cp := range contentParts {
					if cp.Type == ContentTypeRefusal {
						return fmt.Errorf("model refused: %s", cp.Refusal)
					}
					if cp.Type == ContentTypeOutputText || cp.Type == "text" {
						events <- LLMResponseChunk{
							Type:       StreamEventMessage,
							MessageId:  rawMsg.ID,
							Content:    cp.Text,
							IsComplete: rawMsg.Status == "completed",
						}
					}
				}
			}
		case "function_call":
			var fc struct {
				ID        string `json:"id"`
				CallID    string `json:"call_id"`
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				Arguments string `json:"arguments"`
				Status    string `json:"status"`
			}
			if err := json.Unmarshal(raw, &fc); err != nil {
				return fmt.Errorf("parse function_call: %w", err)
			}
			events <- LLMResponseChunk{
				Type: StreamEventFunctionCall,
				ToolCalls: []ToolCall{{
					ID:        fc.ID,
					Type:      "function_call",
					CallID:    fc.CallID,
					Name:      fc.Name,
					Namespace: fc.Namespace,
					Arguments: fc.Arguments,
					Status:    fc.Status,
				}},
				IsComplete: fc.Status == "completed",
			}
		case "reasoning":
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
			if err := json.Unmarshal(raw, &reasoning); err == nil {
				var text string
				for _, item := range reasoning.Content {
					if item.Type == "reasoning_text" {
						text += item.Text
					}
				}
				if text != "" {
					events <- LLMResponseChunk{
						Type:      StreamEventReasoning,
						MessageId: reasoning.ID,
						Content:   text,
					}
				}
			}
		}
	}

	events <- LLMResponseChunk{IsComplete: true}
	return nil
}
