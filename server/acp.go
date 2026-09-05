package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/ryanaldo34/tacklr"
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
	switch {
	case errors.Is(err, context.Canceled):
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

func requireSessionID(method, id string) error {
	if id == "" {
		return clientErrorf(ErrInvalidRequest, "sessionId is required for %s", method)
	}
	return nil
}

func rawObject(raw json.RawMessage) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	return m
}

func jsonString(m map[string]json.RawMessage, key string) string {
	var s string
	_ = json.Unmarshal(m[key], &s)
	return s
}

func jsonInt(m map[string]json.RawMessage, key string) int {
	var n int
	_ = json.Unmarshal(m[key], &n)
	return n
}

func validateACPRequest(body []byte) (*parsedRequest, error) {
	var env acpRequest
	if json.Unmarshal(body, &env) != nil || env.JSONRPC != "2.0" || env.Method == "" {
		return nil, clientErrorf(ErrInvalidRequest, "invalid JSON-RPC")
	}
	params := rawObject(env.Params)
	pr := &parsedRequest{
		ID:              env.ID,
		Method:          env.Method,
		Meta:            env.Meta,
		Notification:    env.ID == nil,
		Params:          env.Params,
		ThreadID:        jsonString(params, "sessionId"),
		CWD:             jsonString(params, "cwd"),
		ConfigID:        jsonString(params, "configId"),
		ConfigValue:     jsonString(params, "value"),
		AuthMethodID:    strings.TrimSpace(jsonString(params, "methodId")),
		ProtocolVersion: jsonInt(params, "protocolVersion"),
		ClientCapsRaw:   env.Params,
	}
	_ = json.Unmarshal(params["mcpServers"], &pr.MCPServers)
	_ = json.Unmarshal(params["responses"], &pr.Responses)

	if pr.Notification {
		return pr, nil
	}
	switch env.Method {
	case "initialize":
		if pr.ProtocolVersion < 1 {
			return nil, clientErrorf(ErrInvalidRequest, "unsupported protocol version %d", pr.ProtocolVersion)
		}
	case "session/load", "session/resume", "session/prompt", "session/set_config_option", "session/close", "session/cancel":
		if err := requireSessionID(env.Method, pr.ThreadID); err != nil {
			return nil, err
		}
		if env.Method == "session/prompt" {
			msg, err := parseACPPrompt(params["prompt"])
			if err != nil {
				return nil, clientErrorf(ErrInvalidRequest, "invalid prompt content: %v", err)
			}
			pr.Prompt, pr.UserMessage = msg.Content, msg
		}
	case "authenticate":
		if pr.AuthMethodID == "" {
			return nil, clientErrorf(ErrInvalidRequest, "methodId is required for authenticate")
		}
	}
	return pr, nil
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
			mime := tacklr.NormalizeMIME(b.MimeType)
			url, err := resolveImageURL(mime, b.Data, b.URI)
			if !strings.HasPrefix(mime, "image/") || err != nil {
				return nil, fmt.Errorf("image block %d is invalid", i)
			}
			binary = append(binary, tacklr.ContentPart{
				Type:     tacklr.ContentTypeInputImage,
				ImageURL: &tacklr.ImageURL{URL: url},
				FileData: &tacklr.FileData{MIMEType: mime},
			})
		case "resource":
			part, text, err := parseACPResource(i, b.Resource)
			if err != nil {
				return nil, err
			}
			if part != nil {
				binary = append(binary, *part)
			}
			if text != "" {
				textParts = append(textParts, text)
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
	msg := &tacklr.Message{
		Role:    tacklr.RoleUser,
		Content: strings.Join(textParts, "\n\n"),
	}
	if len(binary) > 0 {
		msg.ContentParts = binary
	}
	return msg, nil
}

func parseACPResource(i int, r *acpResource) (*tacklr.ContentPart, string, error) {
	if r == nil {
		return nil, "", fmt.Errorf("resource block %d is invalid", i)
	}
	hasBlob := strings.TrimSpace(r.Blob) != ""
	hasText := strings.TrimSpace(r.Text) != ""
	if !hasBlob && !hasText {
		return nil, "", fmt.Errorf("resource block %d is invalid", i)
	}
	var part *tacklr.ContentPart
	if hasBlob {
		mime := tacklr.NormalizeMIME(r.MimeType)
		if mime != "application/pdf" {
			return nil, "", fmt.Errorf("resource block %d is invalid", i)
		}
		filename := "document.pdf"
		if base := path.Base(strings.TrimSpace(r.URI)); base != "" && base != "." && base != "/" {
			filename = base
		}
		part = &tacklr.ContentPart{
			Type: tacklr.ContentTypeInputFile,
			FileData: &tacklr.FileData{
				Data:     tacklr.DataURL(mime, r.Blob),
				MIMEType: mime,
				Filename: filename,
			},
		}
	}
	return part, r.Text, nil
}

func resolveImageURL(mime, data, uri string) (string, error) {
	data = strings.TrimSpace(data)
	uri = strings.TrimSpace(uri)
	if data != "" {
		return tacklr.DataURL(mime, data), nil
	}
	if strings.HasPrefix(uri, "data:") || strings.HasPrefix(uri, "https://") || strings.HasPrefix(uri, "http://") {
		return uri, nil
	}
	return "", fmt.Errorf("requires data or http(s) uri")
}

// formatResourceLink builds a text stand-in for ContentBlock::ResourceLink (ACP baseline).
func formatResourceLink(b acpContentBlock) (string, error) {
	uri := strings.TrimSpace(b.URI)
	name := strings.TrimSpace(b.Name)
	if uri == "" || name == "" {
		return "", fmt.Errorf("resource_link requires name and uri")
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

func acpUpdateFrame(threadID string, update map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params":  map[string]any{"sessionId": threadID, "update": update},
	})
	return b
}

func acpTextChunk(threadID, messageID, sessionUpdate, text string) []byte {
	return acpUpdateFrame(threadID, map[string]any{
		"messageId":     messageID,
		"sessionUpdate": sessionUpdate,
		"content":       map[string]string{"type": "text", "text": text},
	})
}

func presentationToACP(threadID string, ev tacklr.StreamEvent) [][]byte {
	presented := presentStreamEvent(ev)
	switch presented.Type {
	case string(tacklr.StreamEventMessage):
		if presented.Content == "" {
			return nil
		}
		return [][]byte{acpTextChunk(threadID, presented.MessageID, "agent_message_chunk", presented.Content)}
	case string(tacklr.StreamEventReasoning):
		if presented.Content == "" {
			return nil
		}
		return [][]byte{acpTextChunk(threadID, presented.MessageID, "agent_thought_chunk", presented.Content)}
	case string(tacklr.StreamEventFunctionCall):
		var toStream [][]byte
		if presented.Content != "" {
			toStream = append(toStream, acpTextChunk(threadID, presented.MessageID, "agent_message_chunk", presented.Content))
		}
		for _, toolCall := range presented.ToolCalls {
			toStream = append(toStream, acpUpdateFrame(threadID, acpToolCallUpdate(toolCall, "tool_call", "in_progress", "")))
		}
		return toStream
	case string(tacklr.StreamEventToolUpdate):
		return [][]byte{acpUpdateFrame(threadID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    presented.MessageID,
			"status":        "in_progress",
			"content":       acpToolCallContent(presented.Content),
		})}
	case string(tacklr.StreamEventPlanUpdate):
		var todos []tacklr.Todo
		_ = json.Unmarshal(presented.Data, &todos)
		entries := make([]map[string]any, 0, len(todos))
		for _, todo := range todos {
			entries = append(entries, map[string]any{
				"content":  todo.Title,
				"status":   todo.Status,
				"priority": "medium",
			})
		}
		return [][]byte{acpUpdateFrame(threadID, map[string]any{"sessionUpdate": "plan", "entries": entries})}
	case string(tacklr.StreamEventToolResult):
		tc := presented.ToolCalls[0]
		status := "completed"
		if tc.Status == "error" {
			status = "failed"
		}
		update := acpToolCallUpdate(tc, "tool_call_update", status, presented.Content)
		if presented.Content != "" {
			update["rawOutput"] = map[string]any{"output": presented.Content}
		}
		return [][]byte{acpUpdateFrame(threadID, update)}
	case string(tacklr.StreamEventComplete):
		b, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "id": presented.TurnID, "result": acpPromptResult(stopReasonEndTurn),
		})
		return [][]byte{b}
	case string(tacklr.StreamEventError):
		if reason, ok := stopReasonFromError(presented.Error); ok {
			b, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": presented.TurnID, "result": acpPromptResult(reason),
			})
			return [][]byte{b}
		}
		msg := "internal error"
		if presented.Error != nil {
			msg = presented.Error.Error()
		}
		b, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "id": presented.TurnID,
			"error": map[string]any{"code": jsonRPCCodeInternal, "message": msg},
		})
		return [][]byte{b}
	default:
		return nil
	}
}

// acpToolCallUpdate builds the common tool_call / tool_call_update body.
// Title is the human label; name is the programmatic tool id (ACP RFD-aligned).
func acpToolCallUpdate(tc tacklr.ToolCall, sessionUpdate, status, content string) map[string]any {
	title := tc.Title
	if title == "" {
		title = tc.Name
	}
	update := map[string]any{
		"sessionUpdate": sessionUpdate,
		"toolCallId":    tc.Key(),
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
