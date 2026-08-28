package tacklr

import "strings"

// MessageRole indicates who sent the message.
type MessageRole string

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleReasoning MessageRole = "reasoning"
	RoleSystem    MessageRole = "system"
	RoleDeveloper MessageRole = "developer"
	RoleTool      MessageRole = "tool"
)

type ToolCategory string

const (
	ToolCategoryRead    ToolCategory = "read"
	ToolCategoryEdit    ToolCategory = "edit"
	ToolCategorySearch  ToolCategory = "search"
	ToolCategoryFetch   ToolCategory = "fetch"
	ToolCategoryMove    ToolCategory = "move"
	ToolCategoryThink   ToolCategory = "think"
	ToolCategoryExecute ToolCategory = "execute"
	ToolCategoryDelete  ToolCategory = "delete"
)

type TodoStatus string

const (
	TodoStatusPending    TodoStatus = "pending"
	TodoStatusCompleted  TodoStatus = "completed"
	TodoStatusInProgress TodoStatus = "in_progress"
)

// ItemStatus tracks the lifecycle state of an output item.
type ItemStatus string

const (
	StatusInProgress ItemStatus = "in_progress"
	StatusCompleted  ItemStatus = "completed"
	StatusIncomplete ItemStatus = "incomplete"
)

const (
	ContentTypeOutputText = "output_text"
	ContentTypeInputText  = "input_text"
	ContentTypeInputImage = "input_image"
	ContentTypeInputFile  = "input_file"
	ContentTypeRefusal    = "refusal"
)

// ContentPart is a single content block within a message.
// Discriminated by Type — oneOf{output_text, input_text, input_image, input_file, refusal}.
type ContentPart struct {
	Type        string       `json:"type"`
	Text        string       `json:"text,omitempty"`
	Refusal     string       `json:"refusal,omitempty"`
	ImageURL    *ImageURL    `json:"image_url,omitempty"`
	FileData    *FileData    `json:"file_data,omitempty"`
	Annotations []Annotation `json:"annotations,omitempty"`
}

// ImageURL represents an image input by URL or data URI.
type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// FileData represents an image or file input by ID, URL, or base64 data.
type FileData struct {
	FileID   string `json:"file_id,omitempty"`
	URL      string `json:"url,omitempty"`
	Data     string `json:"data,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
	// Filename is preferred by providers for input_file (e.g. PDF data URLs).
	Filename string `json:"filename,omitempty"`
}

// Annotation attaches file/URL citations to output_text content.
type Annotation struct {
	Type   string         `json:"type"`
	Text   string         `json:"text,omitempty"`
	FileID string         `json:"file_id,omitempty"`
	URL    *URLAnnotation `json:"url,omitempty"`
}

// URLAnnotation references a specific URL as a citation source.
type URLAnnotation struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

// ToolCall represents an assistant request to invoke a tool.
type ToolCall struct {
	ID        string       `json:"id,omitempty"`
	Type      string       `json:"type,omitempty"`
	CallID    string       `json:"call_id"`
	Name      string       `json:"name,omitempty"`  // programmatic tool id (model-facing)
	Title     string       `json:"title,omitempty"` // human-readable invocation label for UIs/protocols
	Category  ToolCategory `json:"category,omitempty"`
	Namespace string       `json:"namespace,omitempty"`
	Arguments string       `json:"arguments,omitempty"`
	Status    string       `json:"status,omitempty"`
}

// Key is the client/lifecycle id: provider item id, else call_id.
func (tc ToolCall) Key() string {
	if tc.ID != "" {
		return tc.ID
	}
	return tc.CallID
}

// WireID is the Responses API call_id field: CallID, else ID.
func (tc ToolCall) WireID() string {
	if tc.CallID != "" {
		return tc.CallID
	}
	return tc.ID
}

// StreamEventType categorizes events sent to the caller.
type StreamEventType string

const (
	StreamEventMessage      StreamEventType = "message"
	StreamEventReasoning    StreamEventType = "reasoning"
	StreamEventFunctionCall StreamEventType = "function_call"
	StreamEventToolResult   StreamEventType = "tool_result"
	StreamEventComplete     StreamEventType = "complete"
	StreamEventError        StreamEventType = "error"
	StreamEventInterrupt    StreamEventType = "yield"
	StreamEventToolUpdate   StreamEventType = "tool_update"
	StreamEventPlanUpdate   StreamEventType = "plan_update"
)

// StreamEvent is the harness interior event bus. Protocols map these events
// to wire formats; the harness does not own protocol framing.
type StreamEvent struct {
	Type      StreamEventType
	TurnID    string
	MessageID string
	Content   string
	Data      []byte
	ToolCalls []ToolCall
	// Error is in-process only. Workflow Streams cannot encode error values;
	// Fail is the durable stand-in (sentinel Error() text).
	Error error  `json:"-"`
	Fail  string `json:"fail,omitempty"`
}

// LLMResponseChunk is the streaming unit emitted by an InferenceStrategy's
// Invoke call. Provider parse only — not client-facing wire.
type LLMResponseChunk struct {
	TurnId     string
	MessageId  string
	ToolCalls  []ToolCall
	Type       StreamEventType
	Content    string
	IsComplete bool
	// Error is set on terminal provider failures (Type == StreamEventError).
	// Harness copies it onto StreamEvent.Error so protocols can errors.Is
	// stop-reason sentinels (refusal, max_tokens, …).
	Error error

	// Token usage when the provider reports it (typically on StreamEventComplete
	// after response.completed). Zero means unknown / not reported.
	InputTokens     int
	OutputTokens    int
	ReasoningTokens int

	// EncryptedContent is Responses reasoning.encrypted_content. Provider parse
	// only; copied onto Message so the next turn can replay the item statelessly.
	EncryptedContent string
}

// Message is the primary conversation unit in the context window.
// It handles both simple text and structured content, tool calls, tool results,
// and reasoning content produced by reasoning models.
// The Role field determines the purpose:
//   - system/developer: system instructions
//   - user: user input (Content or ContentParts)
//   - assistant: model response (Content + optional ToolCalls)
//   - reasoning: model reasoning content (a distinct previous-response item)
//   - tool: result of a tool execution (ToolCallID + Content)
type Message struct {
	Role    MessageRole `json:"role"`
	Content string      `json:"content,omitempty"`

	// MessageID is the provider-assigned identifier for this output item,
	// used when serializing prior assistant or reasoning turns as typed
	// response items.
	MessageID string `json:"message_id,omitempty"`

	// EncryptedContent is the Responses API reasoning ciphertext
	// (include=reasoning.encrypted_content). Required to replay a reasoning
	// item by id without a provider store lookup.
	EncryptedContent string `json:"encrypted_content,omitempty"`

	ContentParts     []ContentPart `json:"content_parts,omitempty"`
	ToolCalls        []ToolCall    `json:"tool_calls,omitempty"`
	ToolCallID       string        `json:"tool_call_id,omitempty"`
	StructuredOutput any           `json:"-"`
}

// MIMETypes returns unique binary MIME types from ContentParts (images/files).
// Producers set FileData.MIMEType (and image parts via FileData or data URL).
// Text and refusal parts are ignored. Order is first-seen.
func (m *Message) MIMETypes() []string {
	if m == nil || len(m.ContentParts) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	add := func(mime string) {
		mime = NormalizeMIME(mime)
		if mime == "" || IsTextMIME(mime) {
			return
		}
		if _, ok := seen[mime]; ok {
			return
		}
		seen[mime] = struct{}{}
		out = append(out, mime)
	}
	for _, p := range m.ContentParts {
		switch p.Type {
		case ContentTypeInputImage:
			if p.FileData != nil && p.FileData.MIMEType != "" {
				add(p.FileData.MIMEType)
				continue
			}
			if p.ImageURL != nil {
				add(MIMEFromDataURL(p.ImageURL.URL))
			}
		case ContentTypeInputFile:
			if p.FileData != nil {
				add(p.FileData.MIMEType)
			}
		}
	}
	return out
}

// NormalizeMIME lowercases a MIME type and strips parameters (after ';').
func NormalizeMIME(mime string) string {
	mime = strings.TrimSpace(strings.ToLower(mime))
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = strings.TrimSpace(mime[:i])
	}
	return mime
}

// IsTextMIME is true for empty and text/* types (always model-safe as text).
func IsTextMIME(mime string) bool {
	mime = NormalizeMIME(mime)
	return mime == "" || strings.HasPrefix(mime, "text/")
}

// MIMEFromDataURL extracts the MIME type from a data: URL, or empty.
func MIMEFromDataURL(u string) string {
	u = strings.TrimSpace(u)
	if !strings.HasPrefix(u, "data:") {
		return ""
	}
	rest := strings.TrimPrefix(u, "data:")
	if i := strings.Index(rest, ","); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.TrimSuffix(rest, ";base64")
	return NormalizeMIME(rest)
}

// DataURL builds a data:<mime>;base64,<data> URL. data may already be a data URL.
func DataURL(mime, data string) string {
	data = strings.TrimSpace(data)
	if strings.HasPrefix(data, "data:") {
		return data
	}
	mime = NormalizeMIME(mime)
	if mime == "" {
		mime = "application/octet-stream"
	}
	return "data:" + mime + ";base64," + strings.TrimSpace(data)
}
