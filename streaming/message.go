package streaming

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
	TodoStatusPending   TodoStatus = "pending"
	TodoStatusCompleted TodoStatus = "completed"
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
	Name      string       `json:"name,omitempty"`
	Category  ToolCategory `json:"category,omitempty"`
	Namespace string       `json:"namespace,omitempty"`
	Arguments string       `json:"arguments,omitempty"`
	Status    string       `json:"status,omitempty"`
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

// StreamEvent is what the caller receives from AgentHarness.Run.
type StreamEvent struct {
	Type      StreamEventType
	TurnID    string
	MessageID string
	Content   string
	Data      []byte
	ToolCalls []ToolCall
	Error     error
}

// LLMResponseChunk is the streaming unit emitted by an InferenceStrategy's
// Invoke call. It is the shared contract between inference providers,
// streaming strategies, and the agent loop.
type LLMResponseChunk struct {
	TurnId     string
	MessageId  string
	ToolCalls  []ToolCall
	Type       StreamEventType
	Content    string
	IsComplete bool
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

	ContentParts     []ContentPart `json:"content_parts,omitempty"`
	ToolCalls        []ToolCall    `json:"tool_calls,omitempty"`
	ToolCallID       string        `json:"tool_call_id,omitempty"`
	StructuredOutput any           `json:"-"`
}
