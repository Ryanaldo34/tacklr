package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
)

// request is a Messages API request. counted is the same request for the
// count endpoint, which takes no response settings.
type request struct {
	Model        string        `json:"model"`
	MaxTokens    int           `json:"max_tokens,omitempty"`
	Stream       bool          `json:"stream,omitempty"`
	System       []textBlock   `json:"system,omitempty"`
	Tools        []tool        `json:"tools,omitempty"`
	ToolChoice   *toolChoice   `json:"tool_choice,omitempty"`
	Thinking     *thinking     `json:"thinking,omitempty"`
	OutputConfig *outputConfig `json:"output_config,omitempty"`
	// CacheControl caches the conversation up to its last block, so the
	// next request of the session reads it from the cache.
	CacheControl *cacheControl `json:"cache_control,omitempty"`
	Messages     []turn        `json:"messages"`
}

func (r request) counted() request {
	r.MaxTokens, r.Stream, r.OutputConfig, r.CacheControl = 0, false, nil, nil
	return r
}

// summary is a short line of the request's shape for a failure's log
// event: its number of turns, and the role and block types of the last
// turns, which the request added.
func (r request) summary() string {
	from := max(0, len(r.Messages)-25)
	parts := make([]string, 0, len(r.Messages)-from)
	for i, t := range r.Messages[from:] {
		types := make([]string, len(t.Content))
		for j, b := range t.Content {
			var head struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(b, &head)
			types[j] = head.Type
		}
		parts = append(parts, fmt.Sprintf("%d:%s[%s]", from+i, t.Role, strings.Join(types, ",")))
	}
	return fmt.Sprintf("%d turns: %s", len(r.Messages), strings.Join(parts, "; "))
}

type cacheControl struct {
	Type string `json:"type"`
}

var ephemeral = cacheControl{Type: "ephemeral"}

type toolChoice struct {
	Type string `json:"type"`
}

type thinking struct {
	Type    string `json:"type"`
	Display string `json:"display,omitempty"`
}

type outputConfig struct {
	Effort string `json:"effort,omitempty"`
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

// toolDefinitions are the tools as the API takes them. A namespaced tool
// is named namespace__name.
func toolDefinitions(tools []*tacklr.Tool) []tool {
	defs := make([]tool, len(tools))
	for i, t := range tools {
		defs[i] = tool{Name: t.QualifiedName(), Description: t.Description(), InputSchema: t.Parameters()}
	}
	return defs
}

// turn is one message: a role and its content blocks, each the JSON the
// API takes.
type turn struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
}

type textBlock struct {
	Type         string        `json:"type"`
	Text         string        `json:"text"`
	CacheControl *cacheControl `json:"cache_control,omitempty"`
}

type mediaBlock struct {
	Type   string `json:"type"`
	Source source `json:"source"`
}

type source struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
	FileID    string `json:"file_id,omitempty"`
}

type toolUseBlock struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type toolResultBlock struct {
	Type      string `json:"type"`
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content,omitempty"`
}

// systemBlocks is the system prompt with a cache breakpoint: tools render
// before the system prompt, so it caches both.
func systemBlocks(prompt string) []textBlock {
	if prompt = strings.TrimSpace(prompt); prompt == "" {
		return nil
	}
	return []textBlock{{Type: "text", Text: prompt, CacheControl: &ephemeral}}
}

// encode is v's JSON. v is a block type of this package, which always
// encodes.
func encode(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// turns maps the window to Messages API turns.
//
// The API has user and assistant turns only. Developer and system
// messages (the plan, a handoff, a summary) go in the user turn as text
// in a system-reminder tag. Reasoning, text and tool calls of one reply
// form one assistant turn, in the order the model wrote them, and the
// results of its calls open the next user turn. A result whose call is
// not in the turn before it is dropped: the API rejects it.
func turns(msgs []*message.Message) []turn {
	var out []turn
	calls := map[string]bool{} // tool_use ids of the last assistant turn
	results := 0               // tool_result blocks that open the last user turn
	add := func(role string, block json.RawMessage) {
		if len(out) == 0 || out[len(out)-1].Role != role {
			out = append(out, turn{Role: role})
			if role == "assistant" {
				clear(calls)
			}
			results = 0
		}
		out[len(out)-1].Content = append(out[len(out)-1].Content, block)
	}
	addResult := func(block json.RawMessage) {
		if len(out) == 0 || out[len(out)-1].Role != "user" {
			add("user", block)
		} else {
			// Results come before any other block of the turn.
			last := &out[len(out)-1]
			last.Content = append(last.Content[:results], append([]json.RawMessage{block}, last.Content[results:]...)...)
		}
		results++
	}
	for _, m := range msgs {
		if m == nil {
			continue
		}
		switch m.Role {
		case message.RoleUser:
			for _, b := range userBlocks(m) {
				add("user", b)
			}
		case message.RoleDeveloper, message.RoleSystem:
			if text := strings.TrimSpace(m.Content); text != "" {
				block := textBlock{Type: "text", Text: "<system-reminder>\n" + text + "\n</system-reminder>"}
				if tacklr.IsPlanDocument(m) {
					block.CacheControl = &ephemeral
				}
				add("user", encode(block))
			}
		case message.RoleReasoning:
			if block, ok := thinkingBlock(m.EncryptedContent); ok {
				add("assistant", block)
			}
		case message.RoleAssistant:
			if m.Content != "" {
				add("assistant", encode(textBlock{Type: "text", Text: m.Content}))
			}
			for _, tc := range m.ToolCalls {
				add("assistant", encode(toolUseBlock{Type: "tool_use", ID: tc.WireID(), Name: qualifiedName(tc), Input: toolInput(tc.Arguments)}))
				calls[tc.WireID()] = true
			}
		case message.RoleTool:
			if len(out) > 0 && (out[len(out)-1].Role == "assistant" || results > 0) && calls[m.ToolCallID] {
				addResult(encode(toolResultBlock{Type: "tool_result", ToolUseID: m.ToolCallID, Content: m.Content}))
			}
		}
	}
	return out
}

// thinkingBlock is a reasoning message's block, which the stream kept as
// the API sent it. Reasoning another provider wrote is not one.
func thinkingBlock(raw string) (json.RawMessage, bool) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal([]byte(raw), &head) != nil || (head.Type != "thinking" && head.Type != "redacted_thinking") {
		return nil, false
	}
	return json.RawMessage(raw), true
}

func qualifiedName(tc message.ToolCall) string {
	if tc.Namespace == "" {
		return tc.Name
	}
	return tc.Namespace + "__" + tc.Name
}

// toolInput is a call's arguments as the API takes them: a JSON object.
func toolInput(args string) json.RawMessage {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(args), &obj) != nil || obj == nil {
		return json.RawMessage("{}")
	}
	return json.RawMessage(args)
}

// userBlocks are a user message's images and documents, then its text.
func userBlocks(m *message.Message) []json.RawMessage {
	var blocks []json.RawMessage
	for _, p := range m.ContentParts {
		if b, ok := mediaPart(p); ok {
			blocks = append(blocks, encode(b))
		}
	}
	if text := strings.TrimSpace(m.Content); text != "" {
		blocks = append(blocks, encode(textBlock{Type: "text", Text: text}))
	}
	return blocks
}

// mediaPart is an image or a document block for p, from its data, URL
// or Files API id.
func mediaPart(p message.ContentPart) (mediaBlock, bool) {
	kind := "image"
	if p.Type == message.ContentTypeInputFile {
		kind = "document"
	} else if p.Type != message.ContentTypeInputImage {
		return mediaBlock{}, false
	}
	fd := p.FileData
	if fd == nil && p.ImageURL != nil {
		fd = &message.FileData{URL: p.ImageURL.URL}
	}
	if fd == nil {
		return mediaBlock{}, false
	}
	switch {
	case fd.FileID != "":
		return mediaBlock{Type: kind, Source: source{Type: "file", FileID: fd.FileID}}, true
	case fd.Data != "":
		return base64Block(kind, message.DataURL(fd.MIMEType, fd.Data))
	case strings.HasPrefix(fd.URL, "data:"):
		return base64Block(kind, fd.URL)
	case fd.URL != "":
		return mediaBlock{Type: kind, Source: source{Type: "url", URL: fd.URL}}, true
	}
	return mediaBlock{}, false
}

// base64Block is a block whose source is a base64 data URL.
func base64Block(kind, dataURL string) (mediaBlock, bool) {
	meta, data, ok := strings.Cut(strings.TrimPrefix(dataURL, "data:"), ",")
	mime, isBase64 := strings.CutSuffix(meta, ";base64")
	if !ok || !isBase64 || data == "" {
		return mediaBlock{}, false
	}
	return mediaBlock{Type: kind, Source: source{Type: "base64", MediaType: message.NormalizeMIME(mime), Data: data}}, true
}
