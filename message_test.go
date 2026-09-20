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

func TestToolCall_KeyAndWireID(t *testing.T) {
	if (ToolCall{ID: "i", CallID: "c"}).Key() != "i" {
		t.Fatal("Key prefers ID")
	}
	if (ToolCall{CallID: "c"}).Key() != "c" {
		t.Fatal("Key falls back to CallID")
	}
	if (ToolCall{ID: "i", CallID: "c"}).WireID() != "c" {
		t.Fatal("WireID prefers CallID")
	}
	if (ToolCall{ID: "i"}).WireID() != "i" {
		t.Fatal("WireID falls back to ID")
	}
}

// TestMIMEHelpers_andMessageMIMETypes covers NormalizeMIME/DataURL/MIMEFromDataURL
// and Message.MIMETypes first-seen binary collection.
func TestMIMEHelpers_andMessageMIMETypes(t *testing.T) {
	if NormalizeMIME(" Image/PNG; charset=binary ") != "image/png" {
		t.Fatal("NormalizeMIME")
	}
	if !IsTextMIME("") || !IsTextMIME("text/plain") || IsTextMIME("image/png") {
		t.Fatal("IsTextMIME")
	}
	if MIMEFromDataURL("https://x") != "" {
		t.Fatal("non-data URL")
	}
	if MIMEFromDataURL("data:image/jpeg;base64,AAAA") != "image/jpeg" {
		t.Fatal("MIMEFromDataURL")
	}
	if DataURL("image/png", "data:image/png;base64,XX") != "data:image/png;base64,XX" {
		t.Fatal("DataURL passthrough")
	}
	if DataURL("", "abc") != "data:application/octet-stream;base64,abc" {
		t.Fatal("DataURL default mime")
	}
	if DataURL("image/png", "abc") != "data:image/png;base64,abc" {
		t.Fatal("DataURL build")
	}

	var nilMsg *Message
	if nilMsg.MIMETypes() != nil {
		t.Fatal("nil message")
	}
	if (&Message{}).MIMETypes() != nil {
		t.Fatal("empty parts")
	}
	m := &Message{ContentParts: []ContentPart{
		{Type: ContentTypeInputText, Text: "hi"},
		{Type: ContentTypeInputImage, FileData: &FileData{MIMEType: "image/png"}},
		{Type: ContentTypeInputImage, FileData: &FileData{MIMEType: "image/png"}}, // dedupe
		{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/webp;base64,x"}},
		{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "application/pdf"}},
		{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "text/plain"}}, // ignored
	}}
	got := m.MIMETypes()
	if len(got) != 3 || got[0] != "image/png" || got[1] != "image/webp" || got[2] != "application/pdf" {
		t.Fatalf("MIMETypes = %v", got)
	}
	// Image without FileData MIME uses data URL parse.
	m2 := &Message{ContentParts: []ContentPart{
		{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/gif;base64,y"}},
	}}
	if g := m2.MIMETypes(); len(g) != 1 || g[0] != "image/gif" {
		t.Fatalf("data-url only = %v", g)
	}
}
