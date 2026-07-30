package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
)

// acpRequest is the JSON-RPC 2.0 envelope for ACP requests.
type acpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Meta    json.RawMessage `json:"_meta,omitempty"`
}

// acpContentBlock is a single block in an ACP prompt array.
type acpContentBlock struct {
	Type     string       `json:"type"`
	Text     string       `json:"text,omitempty"`
	Resource *acpResource `json:"resource,omitempty"`
}

// acpResource holds inline content for a resource block in an ACP prompt.
type acpResource struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType"`
	Text     string `json:"text"`
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
		// Accept any positive protocol version; respond with the version we support.
		if p.ProtocolVersion < 1 {
			return nil, clientErrorf(ErrInvalidRequest, "unsupported protocol version %d", p.ProtocolVersion)
		}
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
		text, err := concatenateACPPrompt(p.Prompt)
		if err != nil {
			return nil, clientErrorf(ErrInvalidRequest, "invalid prompt content: %v", err)
		}
		if text == "" {
			return nil, clientErrorf(ErrInvalidRequest, "prompt must not be empty")
		}
		pr.ThreadID = p.SessionID
		pr.Prompt = text
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
		// No auth required; accept empty success.
		return pr, nil
	default:
		return nil, clientErrorf(ErrInvalidRequest, "unsupported method: %s", env.Method)
	}
}

// concatenateACPPrompt joins text blocks and inline resource content into a
// single prompt string.
func concatenateACPPrompt(raw json.RawMessage) (string, error) {
	var blocks []acpContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", fmt.Errorf("invalid prompt array: %w", err)
	}
	var parts []string
	for i, b := range blocks {
		switch b.Type {
		case "text":
			if b.Text == "" {
				return "", fmt.Errorf("text block %d must have non-empty text", i)
			}
			parts = append(parts, b.Text)
		case "resource":
			if b.Resource == nil {
				return "", fmt.Errorf("resource block %d must have a resource field", i)
			}
			parts = append(parts, b.Resource.Text)
		default:
			return "", fmt.Errorf("unsupported content block type %q at index %d", b.Type, i)
		}
	}
	return strings.Join(parts, "\n\n"), nil
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
					"update": map[string]any{
						"sessionUpdate": "tool_call",
						"toolCallId":    acpToolCallID(toolCall),
						"title":         toolCall.Name,
						"status":        "in_progress",
						"kind":          toolCall.Category,
					},
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
		var todos []control.Todo
		err := json.Unmarshal(event.Data, &todos)
		if err != nil {
			return nil, err
		}
		var entries = make([]map[string]any, 0, len(todos))
		for _, todo := range todos {
			entries = append(entries, map[string]any{
				"content": todo.Title,
				"status":  todo.Status,
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
					"entries": entries,
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
		update := map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    acpToolCallID(tc),
			"title":         tc.Name,
			"status":        status,
		}
		if event.Content != "" {
			update["content"] = acpToolCallContent(event.Content)
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
			"result": map[string]string{
				"stopReason": "end_turn",
			},
		}
		bytes, _ := json.Marshal(data)
		toStream = append(toStream, bytes)
		return toStream, nil
	case streaming.StreamEventError:
		// TODO: make better errors such as max tokens reached, model refusal, etc.
		var toStream [][]byte
		data := map[string]any{
			"jsonrpc": "2.0",
			"id":      event.TurnID,
			"error": map[string]any{
				"code":    -32603,
				"message": event.Error.Error(),
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

// acpToolCallID prefers ID, then CallID — providers like llama.cpp may only set one.
func acpToolCallID(tc streaming.ToolCall) string {
	if tc.ID != "" {
		return tc.ID
	}
	return tc.CallID
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
