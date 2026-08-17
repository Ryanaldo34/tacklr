package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
)

// ACP stop reasons for session/prompt PromptResponse (spec Prompt Turn).
const (
	stopReasonEndTurn         = "end_turn"
	stopReasonMaxTokens       = "max_tokens"
	stopReasonMaxTurnRequests = "max_turn_requests"
	stopReasonRefusal         = "refusal"
	stopReasonCancelled       = "cancelled"
)

// stopReasonFromError maps harness terminal errors to ACP stopReason values.
// ok is false when the error is not a semantic turn stop (use JSON-RPC error).
func stopReasonFromError(err error) (reason string, ok bool) {
	if err == nil {
		return "", false
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, ErrRequestCancelled):
		return stopReasonCancelled, true
	case errors.Is(err, tacklr.ErrModelRefused):
		return stopReasonRefusal, true
	case errors.Is(err, tacklr.ErrMaxTokens):
		return stopReasonMaxTokens, true
	case errors.Is(err, tacklr.ErrMaxTurnRequests):
		return stopReasonMaxTurnRequests, true
	default:
		return "", false
	}
}

func acpPromptResult(stopReason string) map[string]string {
	return map[string]string{"stopReason": stopReason}
}

// acpRequest is the JSON-RPC 2.0 envelope for ACP requests.
type acpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Meta    json.RawMessage `json:"_meta,omitempty"`
}

// acpProtocolVersion is the MAJOR ACP version this agent implements.
const acpProtocolVersion = 1

// acpContentBlock is a single block in an ACP prompt array.
type acpContentBlock struct {
	Type        string       `json:"type"`
	Text        string       `json:"text,omitempty"`
	MimeType    string       `json:"mimeType,omitempty"`
	Data        string       `json:"data,omitempty"` // base64 for type=image
	URI         string       `json:"uri,omitempty"`  // image source or resource_link
	Name        string       `json:"name,omitempty"` // resource_link
	Title       string       `json:"title,omitempty"`
	Description string       `json:"description,omitempty"`
	Size        *int64       `json:"size,omitempty"`
	Resource    *acpResource `json:"resource,omitempty"`
}

// acpResource holds inline content for a resource block in an ACP prompt.
// Text is embedded context; Blob is base64 binary (application/pdf in this pass).
type acpResource struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"`
}

// acpSessionParams holds params for session/new, session/load, session/resume.
type acpSessionParams struct {
	Cwd        string          `json:"cwd"`
	SessionID  string          `json:"sessionId"`
	MCPServers []mcp.MCPConfig `json:"mcpServers,omitempty"`
}

// acpPromptParams holds params for session/prompt.
type acpPromptParams struct {
	SessionID string          `json:"sessionId"`
	Prompt    json.RawMessage `json:"prompt"`
}

// acpSessionIDParams holds params for methods that only need a session ID.
type acpSessionIDParams struct {
	SessionID string `json:"sessionId"`
}

// acpConfigSetParams holds params for session/set_config_option.
type acpConfigSetParams struct {
	SessionID string `json:"sessionId"`
	ConfigID  string `json:"configId"`
	Value     string `json:"value"`
}

func validateACPRequest(body []byte) (*parsedRequest, error) {
	var env acpRequest
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, clientErrorf(ErrInvalidRequest, "invalid JSON-RPC: %v", err)
	}
	if env.JSONRPC != "2.0" {
		return nil, clientErrorf(ErrInvalidRequest, "jsonrpc version must be \"2.0\"")
	}
	if env.Method == "" {
		return nil, clientErrorf(ErrInvalidRequest, "method is required")
	}

	pr := &parsedRequest{
		ID:           env.ID,
		Method:       env.Method,
		Meta:         env.Meta,
		Notification: env.ID == nil,
		Params:       env.Params,
	}

	// JSON-RPC notifications have no id and must not receive a response.
	if pr.Notification {
		if env.Method == "session/cancel" && env.Params != nil {
			var p acpSessionIDParams
			if err := json.Unmarshal(env.Params, &p); err == nil {
				pr.ThreadID = p.SessionID
			}
		}
		return pr, nil
	}

	switch env.Method {
	case "initialize":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for initialize")
		}
		var p struct {
			ProtocolVersion int `json:"protocolVersion"`
		}
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid initialize params: %v", err)
		}
		// Client MUST send a positive major version; we negotiate in the result.
		if p.ProtocolVersion < 1 {
			return nil, clientErrorf(ErrInvalidRequest, "unsupported protocol version %d", p.ProtocolVersion)
		}
		pr.ProtocolVersion = p.ProtocolVersion
		pr.ClientCapsRaw = env.Params
		return pr, nil
	case "session/new":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for session/new")
		}
		var p acpSessionParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/new params: %v", err)
		}
		pr.CWD = p.Cwd
		pr.MCPServers = p.MCPServers
		return pr, nil
	case "session/load":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for session/load")
		}
		var p acpSessionParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/load params: %v", err)
		}
		if p.SessionID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "sessionId is required for session/load")
		}
		pr.ThreadID = p.SessionID
		pr.CWD = p.Cwd
		pr.MCPServers = p.MCPServers
		return pr, nil
	case "session/resume":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for session/resume")
		}
		var p acpSessionParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/resume params: %v", err)
		}
		if p.SessionID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "sessionId is required for session/resume")
		}
		pr.ThreadID = p.SessionID
		pr.CWD = p.Cwd
		pr.MCPServers = p.MCPServers
		return pr, nil
	case "session/prompt":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for session/prompt")
		}
		var p acpPromptParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/prompt params: %v", err)
		}
		if p.SessionID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "sessionId is required for session/prompt")
		}
		if len(p.Prompt) == 0 {
			return nil, clientErrorf(ErrInvalidRequest, "prompt must not be empty")
		}
		msg, err := parseACPPrompt(p.Prompt)
		if err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid prompt content: %v", err)
		}
		pr.ThreadID = p.SessionID
		pr.Prompt = msg.Content
		pr.UserMessage = msg
		return pr, nil
	case "session/set_config_option":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for session/set_config_option")
		}
		var p acpConfigSetParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid session/set_config_option params: %v", err)
		}
		if p.SessionID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "sessionId is required for session/set_config_option")
		}
		if p.ConfigID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "configId is required for session/set_config_option")
		}
		pr.ThreadID = p.SessionID
		pr.ConfigID = p.ConfigID
		pr.ConfigValue = p.Value
		return pr, nil
	case "session/close", "session/cancel":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for %s", env.Method)
		}
		var p acpSessionIDParams
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid %s params: %v", env.Method, err)
		}
		if p.SessionID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "sessionId is required for %s", env.Method)
		}
		pr.ThreadID = p.SessionID
		return pr, nil
	case "authenticate":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for authenticate")
		}
		var p struct {
			MethodID string `json:"methodId"`
		}
		if err := json.Unmarshal(env.Params, &p); err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid authenticate params: %v", err)
		}
		if strings.TrimSpace(p.MethodID) == "" {
			return nil, clientErrorf(ErrInvalidRequest, "methodId is required for authenticate")
		}
		pr.AuthMethodID = p.MethodID
		return pr, nil
	case "logout":
		if env.Params == nil {
			return nil, clientErrorf(ErrInvalidRequest, "params is required for logout")
		}
		return pr, nil
	default:
		// Admit unknown methods so HandleInbound can return JSON-RPC MethodNotFound.
		return pr, nil
	}
}

// parseACPPrompt maps an ACP content-block array into a user Message
// (text Content + multimodal ContentParts). Does not check model MIME support.
// Text-only prompts keep ContentParts empty so the OpenAI path stays string form.
func parseACPPrompt(raw json.RawMessage) (*tacklr.Message, error) {
	var blocks []acpContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("invalid prompt array: %w", err)
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("prompt must not be empty")
	}
	var textParts []string
	var binary []tacklr.ContentPart
	for i, b := range blocks {
		switch b.Type {
		case "text":
			if strings.TrimSpace(b.Text) == "" {
				return nil, fmt.Errorf("text block %d must have non-empty text", i)
			}
			textParts = append(textParts, b.Text)
		case "image":
			mime := streaming.NormalizeMIME(b.MimeType)
			if mime == "" {
				return nil, fmt.Errorf("image block %d requires mimeType", i)
			}
			if !strings.HasPrefix(mime, "image/") {
				return nil, fmt.Errorf("image block %d mimeType must be image/*, got %q", i, mime)
			}
			url, err := resolveImageURL(mime, b.Data, b.URI)
			if err != nil {
				return nil, fmt.Errorf("image block %d: %w", i, err)
			}
			binary = append(binary, tacklr.ContentPart{
				Type:     tacklr.ContentTypeInputImage,
				ImageURL: &tacklr.ImageURL{URL: url},
				FileData: &tacklr.FileData{MIMEType: mime}, // MIME for capability checks
			})
		case "resource":
			if b.Resource == nil {
				return nil, fmt.Errorf("resource block %d must have a resource field", i)
			}
			r := b.Resource
			hasBlob := strings.TrimSpace(r.Blob) != ""
			hasText := strings.TrimSpace(r.Text) != ""
			if !hasBlob && !hasText {
				return nil, fmt.Errorf("resource block %d requires text or blob", i)
			}
			if hasBlob {
				mime := streaming.NormalizeMIME(r.MimeType)
				if mime == "" {
					return nil, fmt.Errorf("resource blob block %d requires mimeType", i)
				}
				if mime != "application/pdf" {
					return nil, fmt.Errorf("resource blob block %d: only application/pdf is supported, got %q", i, mime)
				}
				filename := "document.pdf"
				if base := path.Base(strings.TrimSpace(r.URI)); base != "" && base != "." && base != "/" {
					filename = base
				}
				binary = append(binary, tacklr.ContentPart{
					Type: tacklr.ContentTypeInputFile,
					FileData: &tacklr.FileData{
						Data:     streaming.DataURL(mime, r.Blob),
						MIMEType: mime,
						Filename: filename,
					},
				})
			}
			if hasText {
				textParts = append(textParts, r.Text)
			}
		case "resource_link":
			// Baseline MUST: accept resource links. We do not fetch; surface a
			// stable text descriptor so the model sees the reference (no resource_link resolution).
			link, err := formatResourceLink(b)
			if err != nil {
				return nil, fmt.Errorf("resource_link block %d: %w", i, err)
			}
			textParts = append(textParts, link)
		case "audio":
			// Optional prompt capability; we advertise audio:false.
			return nil, fmt.Errorf("audio content blocks are not supported")
		default:
			return nil, fmt.Errorf("unsupported content block type %q at index %d", b.Type, i)
		}
	}
	if len(textParts) == 0 && len(binary) == 0 {
		return nil, fmt.Errorf("prompt must not be empty")
	}
	msg := &tacklr.Message{
		Role:    tacklr.RoleUser,
		Content: strings.Join(textParts, "\n\n"),
	}
	if len(binary) > 0 {
		msg.ContentParts = binary
	}
	return msg, nil
}

func resolveImageURL(mime, data, uri string) (string, error) {
	data = strings.TrimSpace(data)
	uri = strings.TrimSpace(uri)
	if data != "" {
		return streaming.DataURL(mime, data), nil
	}
	if strings.HasPrefix(uri, "data:") || strings.HasPrefix(uri, "https://") || strings.HasPrefix(uri, "http://") {
		return uri, nil
	}
	if uri == "" {
		return "", fmt.Errorf("requires data or uri")
	}
	return "", fmt.Errorf("uri must be data: or http(s) URL when data is empty")
}

// formatResourceLink builds a text stand-in for ContentBlock::ResourceLink (ACP baseline).
func formatResourceLink(b acpContentBlock) (string, error) {
	uri := strings.TrimSpace(b.URI)
	name := strings.TrimSpace(b.Name)
	if uri == "" {
		return "", fmt.Errorf("uri is required")
	}
	if name == "" {
		return "", fmt.Errorf("name is required")
	}
	var bld strings.Builder
	bld.WriteString("[Resource link] name=")
	bld.WriteString(name)
	bld.WriteString(" uri=")
	bld.WriteString(uri)
	if mt := strings.TrimSpace(b.MimeType); mt != "" {
		bld.WriteString(" mimeType=")
		bld.WriteString(mt)
	}
	if b.Size != nil {
		fmt.Fprintf(&bld, " size=%d", *b.Size)
	}
	if t := strings.TrimSpace(b.Title); t != "" {
		bld.WriteString(" title=")
		bld.WriteString(t)
	}
	if d := strings.TrimSpace(b.Description); d != "" {
		bld.WriteString("\n")
		bld.WriteString(d)
	}
	return bld.String(), nil
}

func eventToAcpJsonRpc(threadId string, event *streaming.StreamEvent) ([][]byte, error) {
	switch event.Type {
	case streaming.StreamEventMessage:
		if event.Content == "" {
			return nil, nil
		}
		var toStream [][]byte
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update": map[string]any{
					"messageId":     event.MessageID,
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]string{
						"type": "text",
						"text": event.Content,
					},
				},
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventReasoning:
		if event.Content == "" {
			return nil, nil
		}
		var toStream [][]byte
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update": map[string]any{
					"messageId":     event.MessageID,
					"sessionUpdate": "agent_thought_chunk",
					"content": map[string]string{
						"type": "text",
						"text": event.Content,
					},
				},
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventFunctionCall:
		// TODO: pass channel to harness runtime state hook for tools to allow tools to stream progressive updates
		var toStream [][]byte
		if event.Content != "" {
			data := map[string]any{
				"jsonrpc": "2.0",
				"method":  "session/update",
				"params": map[string]any{
					"sessionId": threadId,
					"update": map[string]any{
						"messageId":     event.MessageID,
						"sessionUpdate": "agent_message_chunk",
						"content": map[string]string{
							"type": "text",
							"text": event.Content,
						},
					},
				},
			}
			bytes, _ := json.Marshal(data)
			toStream = append(toStream, bytes)
		}
		for _, toolCall := range event.ToolCalls {
			data := map[string]any{
				"jsonrpc": "2.0",
				"method":  "session/update",
				"params": map[string]any{
					"sessionId": threadId,
					"update":    acpToolCallUpdate(toolCall, "tool_call", "in_progress", ""),
				},
			}
			bytes, _ := json.Marshal(data)
			toStream = append(toStream, bytes)
		}
		return toStream, nil
	case streaming.StreamEventToolUpdate:
		var toStream [][]byte
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update": map[string]any{
					"sessionUpdate": "tool_call_update",
					"toolCallId":    event.MessageID,
					"status":        "in_progress",
					"content":       acpToolCallContent(event.Content),
				},
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventPlanUpdate:
		var toStream [][]byte
		var todos []streaming.Todo
		err := json.Unmarshal(event.Data, &todos)
		if err != nil {
			return nil, err
		}
		var entries = make([]map[string]any, 0, len(todos))
		for _, todo := range todos {
			entries = append(entries, map[string]any{
				"content":  todo.Title,
				"status":   todo.Status,
				"priority": "medium",
			})
		}
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update": map[string]any{
					"sessionUpdate": "plan",
					"entries":       entries,
				},
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventToolResult:
		if len(event.ToolCalls) == 0 {
			slog.Warn("tool_result event missing ToolCalls")
			return nil, nil
		}
		var toStream [][]byte
		tc := event.ToolCalls[0]
		var status string
		if tc.Status == "error" {
			status = "failed"
		} else {
			status = "completed"
		}
		// Terminal status uses tool_call_update with ACP ToolCallContent[] (not a bare string).
		update := acpToolCallUpdate(tc, "tool_call_update", status, event.Content)
		if event.Content != "" {
			update["rawOutput"] = map[string]any{"output": event.Content}
		}
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update":    update,
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventComplete:
		var toStream [][]byte
		data := map[string]any{
			"jsonrpc": "2.0",
			"id":      event.TurnID,
			"result":  acpPromptResult(stopReasonEndTurn),
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventError:
		var toStream [][]byte
		// Semantic stop reasons are successful PromptResponse results, not RPC errors.
		if reason, ok := stopReasonFromError(event.Error); ok {
			data := map[string]any{
				"jsonrpc": "2.0",
				"id":      event.TurnID,
				"result":  acpPromptResult(reason),
			}
			bytes, _ := json.Marshal(data)
			toStream = append(toStream, bytes)
			return toStream, nil
		}
		msg := "internal error"
		if event.Error != nil {
			msg = event.Error.Error()
		} else if event.Content != "" {
			msg = event.Content
		}
		data := map[string]any{
			"jsonrpc": "2.0",
			"id":      event.TurnID,
			"error": map[string]any{
				"code":    jsonRPCCodeInternal,
				"message": msg,
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventInterrupt:
		return nil, nil // interrupt events are handled in the harness, never encoded over ACP
	default:
		slog.Warn("unhandled event type", "type", event.Type)
		return nil, nil
	}
}

// acpToolCallID is the client lifecycle id (same as ToolCall.Key).
func acpToolCallID(tc streaming.ToolCall) string { return tc.Key() }

// acpToolCallUpdate builds the common tool_call / tool_call_update body.
// Title is the human label; name is the programmatic tool id (ACP RFD-aligned).
func acpToolCallUpdate(tc streaming.ToolCall, sessionUpdate, status, content string) map[string]any {
	title := tc.Title
	if title == "" {
		title = tc.Name
	}
	update := map[string]any{
		"sessionUpdate": sessionUpdate,
		"toolCallId":    acpToolCallID(tc),
		"title":         title,
		"status":        status,
	}
	if tc.Name != "" {
		update["name"] = tc.Name
	}
	if tc.Category != "" {
		update["kind"] = tc.Category
	}
	if content != "" {
		update["content"] = acpToolCallContent(content)
	}
	return update
}

// acpToolCallContent wraps plain text as ACP ToolCallContent[].
func acpToolCallContent(text string) []map[string]any {
	return []map[string]any{
		{
			"type": "content",
			"content": map[string]any{
				"type": "text",
				"text": text,
			},
		},
	}
}
