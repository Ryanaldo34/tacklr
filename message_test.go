package tacklr

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMessageJSON(t *testing.T) {
	t.Run("user message with content", func(t *testing.T) {
		msg := Message{Role: RoleUser, Content: "Hello"}
		b, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		got := string(b)
		want := `{"role":"user","content":"Hello"}`
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}

		var decoded Message
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Role != RoleUser || decoded.Content != "Hello" {
			t.Errorf("got %+v", decoded)
		}
	})

	t.Run("assistant message with content", func(t *testing.T) {
		msg := Message{Role: RoleAssistant, Content: "Hi there"}
		b, _ := json.Marshal(msg)
		got := string(b)
		want := `{"role":"assistant","content":"Hi there"}`
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("assistant message with tool calls", func(t *testing.T) {
		msg := Message{
			Role:    RoleAssistant,
			Content: "Let me check the weather",
			ToolCalls: []ToolCall{
				{
					CallID:    "call_1",
					Name:      "get_weather",
					Arguments: `{"location":"NYC"}`,
					Type:      "function_call",
				},
			},
		}
		b, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		got := string(b)
		if !containsJSON(got, `"role":"assistant"`) {
			t.Errorf("missing role in %s", got)
		}
		if !containsJSON(got, `"call_id":"call_1"`) {
			t.Errorf("missing call_id in %s", got)
		}
		if !containsJSON(got, `"name":"get_weather"`) {
			t.Errorf("missing name in %s", got)
		}
		if !containsJSON(got, `"arguments":"{\"location\":\"NYC\"}"`) {
			t.Errorf("missing arguments in %s", got)
		}

		var decoded Message
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Role != RoleAssistant || decoded.Content != "Let me check the weather" {
			t.Errorf("role/content mismatch: %+v", decoded)
		}
		if len(decoded.ToolCalls) != 1 {
			t.Fatalf("expected 1 tool call, got %d", len(decoded.ToolCalls))
		}
		if decoded.ToolCalls[0].CallID != "call_1" || decoded.ToolCalls[0].Name != "get_weather" {
			t.Errorf("tool call mismatch: %+v", decoded.ToolCalls[0])
		}
	})

	t.Run("tool result message", func(t *testing.T) {
		msg := Message{
			Role:       RoleTool,
			Content:    `{"temp":72}`,
			ToolCallID: "call_1",
		}
		b, _ := json.Marshal(msg)
		got := string(b)
		if !containsJSON(got, `"role":"tool"`) {
			t.Errorf("missing role in %s", got)
		}
		if !containsJSON(got, `"tool_call_id":"call_1"`) {
			t.Errorf("missing tool_call_id in %s", got)
		}
	})

	t.Run("system message", func(t *testing.T) {
		msg := Message{Role: RoleSystem, Content: "Be helpful."}
		b, _ := json.Marshal(msg)
		got := string(b)
		want := `{"role":"system","content":"Be helpful."}`
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("empty content omits field", func(t *testing.T) {
		msg := Message{Role: RoleUser}
		b, _ := json.Marshal(msg)
		got := string(b)
		want := `{"role":"user"}`
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})
}

func TestContentPartTypes(t *testing.T) {
	t.Run("output text", func(t *testing.T) {
		cp := ContentPart{
			Type: ContentTypeOutputText,
			Text: "Hello world",
		}
		b, _ := json.Marshal(cp)
		got := string(b)
		want := `{"type":"output_text","text":"Hello world"}`
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("refusal", func(t *testing.T) {
		cp := ContentPart{
			Type:    ContentTypeRefusal,
			Refusal: "I cannot answer that.",
		}
		b, _ := json.Marshal(cp)
		got := string(b)
		if !containsJSON(got, `"type":"refusal"`) {
			t.Errorf("got %s", got)
		}
		if !containsJSON(got, `"refusal":"I cannot answer that."`) {
			t.Errorf("got %s", got)
		}
	})

	t.Run("input image", func(t *testing.T) {
		cp := ContentPart{
			Type: ContentTypeInputImage,
			ImageURL: &ImageURL{
				URL: "https://example.com/image.png",
			},
		}
		b, _ := json.Marshal(cp)
		got := string(b)
		if !containsJSON(got, `"type":"input_image"`) {
			t.Errorf("got %s", got)
		}
		if !containsJSON(got, `"url":"https://example.com/image.png"`) {
			t.Errorf("got %s", got)
		}
	})
}

func TestRoundTripMessageWithToolCalls(t *testing.T) {
	original := Message{
		Role:    RoleAssistant,
		Content: "Let me check",
		ToolCalls: []ToolCall{
			{
				CallID:    "call_1",
				Type:      "function_call",
				Name:      "get_weather",
				Arguments: `{"location":"NYC"}`,
				Status:    "completed",
			},
			{
				CallID:    "call_2",
				Type:      "function_call",
				Name:      "get_time",
				Arguments: `{"timezone":"EST"}`,
				Status:    "completed",
			},
		},
	}

	b, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Message
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Role != original.Role {
		t.Errorf("role: got %q, want %q", decoded.Role, original.Role)
	}
	if decoded.Content != original.Content {
		t.Errorf("content: got %q, want %q", decoded.Content, original.Content)
	}
	if len(decoded.ToolCalls) != len(original.ToolCalls) {
		t.Fatalf("tool calls: got %d, want %d", len(decoded.ToolCalls), len(original.ToolCalls))
	}
	for i, tc := range decoded.ToolCalls {
		if tc.CallID != original.ToolCalls[i].CallID {
			t.Errorf("toolcall[%d].CallID: got %q, want %q", i, tc.CallID, original.ToolCalls[i].CallID)
		}
		if tc.Name != original.ToolCalls[i].Name {
			t.Errorf("toolcall[%d].Name: got %q, want %q", i, tc.Name, original.ToolCalls[i].Name)
		}
	}
}

func containsJSON(s, substr string) bool {
	return strings.Contains(s, substr)
}
