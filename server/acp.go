package server

import (
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/streaming"
)

func eventToAcpJsonRpc(threadId string, event *streaming.StreamEvent) ([][]byte, error) {
	switch event.Type {
	case streaming.StreamEventMessage:
		toStream := make([][]byte, 0, 1)
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
		toStream[0] = bytes
		return toStream, nil
	case streaming.StreamEventReasoning:
		toStream := make([][]byte, 0, 1)
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
		toStream[0] = bytes
		return toStream, nil
	case streaming.StreamEventFunctionCall:
		// TODO: pass channel to harness runtime state hook for tools to allow tools to stream progressive updates
		toStream := make([][]byte, 0, len(event.ToolCalls))
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
			toStream[0] = bytes
		}
		for i, toolCall := range event.ToolCalls {
			data := map[string]any{
				"jsonrpc": "2.0",
				"method":  "session/update",
				"params": map[string]any{
					"sessionId": threadId,
					"update": map[string]any{
						"sessionUpdate": "tool_call",
						"toolCallId":    toolCall.ID,
						"title":         toolCall.Name,
						"status":        "in_progress",
						"kind":          "read",
					},
				},
			}
			bytes, _ := json.Marshal(data)
			toStream[i] = bytes
		}
		return toStream, nil
	case streaming.StreamEventToolResult:
		toStream := make([][]byte, 0, 1)
		tc := event.ToolCalls[0]
		data := map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": threadId,
				"update": map[string]any{
					"sessionUpdate": "tool_call",
					"toolCallId":    tc.ID,
					"title":         tc.Name,
					"status":        "completed",
					"content":       event.Content,
				},
			},
		}
		bytes, _ := json.Marshal(data)
		toStream[0] = bytes
		return toStream, nil
	case streaming.StreamEventComplete:
		toStream := make([][]byte, 0, 1)
		data := map[string]any{
			"jsonrpc": "2.0",
			"id":      event.TurnID,
			"result": map[string]string{
				"stopReason": "end_turn",
			},
		}
		bytes, _ := json.Marshal(data)
		toStream[0] = bytes
		return toStream, nil
	case streaming.StreamEventError:
		// TODO: make better errors such as max tokens reached, model refusal, etc.
		toStream := make([][]byte, 0, 1)
		data := map[string]any{
			"jsonrpc": "2.0",
			"id":      event.TurnID,
			"error": map[string]any{
				"code":    -32603,
				"message": event.Error.Error(),
			},
		}
		bytes, _ := json.Marshal(data)
		toStream[0] = bytes
		return toStream, nil
	case streaming.StreamEventInterrupt:
		return nil, fmt.Errorf("No need to process event of type %s", event.Type)
	}
	panic("unhandled event type: " + string(event.Type))
}
