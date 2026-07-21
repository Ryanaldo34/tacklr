package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleReasoning MessageRole = "reasoning"
	RoleSystem    MessageRole = "system"
	RoleDeveloper MessageRole = "developer"
	RoleTool      MessageRole = "tool"

	StatusInProgress ItemStatus = "in_progress"
	StatusCompleted  ItemStatus = "completed"
	StatusIncomplete ItemStatus = "incomplete"

	ContentTypeOutputText = "output_text"
	ContentTypeInputText  = "input_text"
	ContentTypeInputImage = "input_image"
	ContentTypeInputFile  = "input_file"
	ContentTypeRefusal    = "refusal"

	StreamEventMessage      StreamEventType = "message"
	StreamEventReasoning    StreamEventType = "reasoning"
	StreamEventFunctionCall StreamEventType = "function_call"
	StreamEventToolResult   StreamEventType = "tool_result"
	StreamEventComplete     StreamEventType = "complete"
	StreamEventError        StreamEventType = "error"
	StreamEventInterrupt    StreamEventType = "yield"
)

type (
	MessageRole       = streaming.MessageRole
	ItemStatus        = streaming.ItemStatus
	ContentPart       = streaming.ContentPart
	ImageURL          = streaming.ImageURL
	FileData          = streaming.FileData
	Annotation        = streaming.Annotation
	URLAnnotation     = streaming.URLAnnotation
	ToolCall          = streaming.ToolCall
	StreamEventType   = streaming.StreamEventType
	StreamEvent       = streaming.StreamEvent
	LLMResponseChunk  = streaming.LLMResponseChunk
	Message           = streaming.Message
	StreamingStrategy = streaming.StreamingStrategy
)

var (
	ErrModelRefused = errors.New("model refused")
	ErrApiKeyNotSet = errors.New("api key not set")
	ErrModelNotSet  = errors.New("model not set")
	ErrUnknownModel = errors.New("unknown model")
	ErrToolNotFound = errors.New("tool not found")
)

// HarnessRuntime is re-exported from the control package so that tools.go
// and test files can reference it without the control package prefix.
type HarnessRuntime = control.HarnessRuntime

type Response struct {
	Status            ItemStatus
	Messages          []*Message
	IncompleteDetails string
}

type InferenceStrategy interface {
	WithApiKey(string) InferenceStrategy
	WithModel(string) InferenceStrategy
	WithURL(string) InferenceStrategy
	WithReasoningLevel(string) InferenceStrategy
	WithStructuredOutput(any) InferenceStrategy
	SetSystemPrompt(string)
	Invoke(context.Context, []*Message, []*Tool) (chan LLMResponseChunk, error)
	CountTokens(context.Context, []*Message, []*Tool) (int, error)
	CompressContextWindow() error
	MaxContextWindow() (int, error)
}

type AgentWatchDog interface {
	RecordThinking(*Message) error
	RecordOutput(*Message) error
	RecordError(error) error
	RecordTokens(int, int) error
	RecordToolCalls(*Message) error
	RecordToolResult(*Message) error
}

type pendingToolCall struct {
	ToolCall        *ToolCall
	InterruptActive bool
}

type AgentHarness struct {
	Model                InferenceStrategy
	SessionId            string
	Tools                []*Tool
	MCPConfigs           []mcp.MCPConfig
	SystemPrompt         string
	ContextWindow        []*Message
	Store                stores.BaseStore
	Runtime              control.HarnessRuntime
	WatchDog             AgentWatchDog
	MaxWindowSize        int
	compressionPrompt    string
	streamingStrategy    StreamingStrategy
	interruptToRequester map[string]string
	pendingToolCalls     map[string]pendingToolCall
	skillByName          map[string]skills.Skill
	skillDirectories     []string
	skillsInitialized    bool
	mcpClients           []*mcp.Client
	mcpInitialized       bool
}

// harnessSessionState is the persisted portion of AgentHarness required to
// resume a turn after an interrupt. It is stored alongside the context window.
type harnessSessionState struct {
	Runtime              control.HarnessRuntime     `json:"runtime"`
	PendingToolCalls     map[string]pendingToolCall `json:"pendingToolCalls"`
	InterruptToRequester map[string]string          `json:"interruptToRequester"`
}

func (a *AgentHarness) checkpointSession(ctx context.Context) error {
	slog.Debug("checkpointing session", "session_id", a.SessionId, "context_window_size", len(a.ContextWindow))
	winJsonb, winErr := json.Marshal(a.ContextWindow)
	if winErr != nil {
		return winErr
	}
	state := harnessSessionState{
		Runtime:              a.Runtime,
		PendingToolCalls:     a.pendingToolCalls,
		InterruptToRequester: a.interruptToRequester,
	}
	stateJsonb, stateErr := json.Marshal(state)
	if stateErr != nil {
		slog.Error("failed to marshal session state", "session_id", a.SessionId, "error", stateErr)
		return stateErr
	}
	if a.Store == nil {
		return nil
	}
	if err := a.Store.SaveSession(ctx, a.SessionId, winJsonb, stateJsonb); err != nil {
		return err
	}
	return nil
}

func (a *AgentHarness) fitContextWindowBeforeNextTurn(ctx context.Context, nextPrompt *Message, out chan StreamEvent) error {
	if len(a.ContextWindow) == 0 {
		a.ContextWindow = append(a.ContextWindow, nextPrompt)
		prompt := nextPrompt.Content
		if len([]rune(prompt)) > 50 {
			prompt = string([]rune(prompt)[:50]) + "..."
		}
		slog.Info("starting new agent turn", "prompt", prompt)
		return nil
	}
	tempWindow := append(a.ContextWindow, nextPrompt)
	currSize, err := a.Model.CountTokens(ctx, tempWindow, a.Tools)
	if err != nil {
		return fmt.Errorf("count tokens: %w", err)
	}
	if currSize <= a.MaxWindowSize {
		a.ContextWindow = tempWindow
		return nil
	}
	// Compress/Compact the context window which now exceeds the max window size
	slog.Info("max window size exceeded, compressing context window", "max_size", a.MaxWindowSize, "current_size", currSize)
	if a.compressionPrompt == "" {
		a.compressionPrompt = "Please summarize the entire message history into a single, concise summary including key items for your current and past tasks with a primary focus on your current task"
	}
	a.Model.SetSystemPrompt(a.compressionPrompt)
	events, err := a.Model.Invoke(ctx, a.ContextWindow, a.Tools)
	if err != nil {
		return fmt.Errorf("invoke: %w", err)
	}
	var compressed = &Message{}
	for chunk := range events {
		a.streamChunk(chunk, out)
		compressed.Content += chunk.Content
		if chunk.IsComplete {
			compressed.Role = RoleAssistant
			a.ContextWindow = append(a.ContextWindow[:0], compressed, nextPrompt)
			a.Model.SetSystemPrompt(a.SystemPrompt)
		}
	}
	return nil
}

func (a *AgentHarness) findTool(name, namespace string) *Tool {
	idx := slices.IndexFunc(a.Tools, func(t *Tool) bool {
		return t.Name == name && t.Namespace == namespace
	})
	if idx < 0 {
		return nil
	}
	return a.Tools[idx]
}

func (a *AgentHarness) streamChunk(chunk LLMResponseChunk, out chan<- StreamEvent) {
	if a.streamingStrategy != nil {
		if chunk.Type == streaming.StreamEventFunctionCall {
			// Model APIs don't return metadata, so we need to look up the tool to get metadata to stream to client
			for _, tc := range chunk.ToolCalls {
				tool := a.findTool(tc.Name, tc.Namespace)
				if tool != nil {
					tc.Category = tool.Category
					if tool.DisplayName != "" {
						tc.Name = tool.DisplayName
					} else {
						tc.Name = tool.Name
					}
				}
			}
		}
		if err := a.streamingStrategy.Stream(chunk, out); err != nil {
			slog.Warn("streaming chunk", "error", err)
			return
		}
		return
	}
	defaultStream(chunk, out)
}

func (a *AgentHarness) recordOutput(msg *Message) {
	if a.WatchDog != nil {
		if err := a.WatchDog.RecordOutput(msg); err != nil {
			slog.Warn("failure to record model output", "error", err)
		}
	}
}

func (a *AgentHarness) recordToolResult(msg *Message) {
	if a.WatchDog != nil {

		if err := a.WatchDog.RecordToolResult(msg); err != nil {
			slog.Warn("failed to record tool result", "error", err)
		}
	}
}

func defaultStream(chunk LLMResponseChunk, out chan<- StreamEvent) {
	out <- StreamEvent{
		Type:      chunk.Type,
		TurnID:    chunk.TurnId,
		MessageID: chunk.MessageId,
		ToolCalls: chunk.ToolCalls,
		Content:   chunk.Content,
	}
}

func (a *AgentHarness) WithStreamingStrategy(strategy StreamingStrategy) *AgentHarness {
	a.streamingStrategy = strategy
	return a
}

// initMCP connects to all configured MCP servers, discovers their tools, and
// appends them to a.Tools. It is idempotent — subsequent calls are no-ops so
// that ReturnFromInterrupt → Run does not re-discover.
//
// Unreachable servers are logged and skipped; their tools are not injected
// into the context window but reachable servers' tools are still available.
func (a *AgentHarness) initMCP(ctx context.Context) {
	if a.mcpInitialized || len(a.MCPConfigs) == 0 {
		a.mcpInitialized = true
		return
	}
	a.mcpInitialized = true

	discovered, clients := mcp.DiscoverAllTools(ctx, a.MCPConfigs)
	for _, dt := range discovered {
		tool := &Tool{
			Name:        dt.Name,
			Description: dt.Description,
			Namespace:   dt.Namespace,
			Parameters:  dt.Schema,
			HandlerFunc: func(ctx context.Context, args map[string]any, _ *HarnessRuntime) (string, error) {
				return dt.CallFunc(ctx, args)
			},
		}
		a.Tools = append(a.Tools, tool)
	}
	a.mcpClients = clients
}

// Close releases all resources held by the harness, including MCP client
// connections. It should be called when the harness is no longer needed
// (typically after the events channel from Run has been drained).
func (a *AgentHarness) Close() {
	for _, c := range a.mcpClients {
		if err := c.Close(); err != nil {
			slog.Warn("failed to close MCP client", "server", c.Name(), "error", err)
		}
	}
}

func (a *AgentHarness) Run(ctx context.Context, prompt string) (<-chan StreamEvent, error) {
	if err := a.initSkills(); err != nil {
		return nil, fmt.Errorf("load skills: %w", err)
	}
	a.initMCP(ctx)
	out := make(chan StreamEvent)
	err := a.fitContextWindowBeforeNextTurn(ctx, &Message{Role: RoleUser, Content: prompt}, out)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		a.Runtime.EnsureInitialized()
		for {
			select {
			case <-ctx.Done():
				out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: context cancelled: %w", ctx.Err())}
				return
			default:
				// returning from an interrupt will have pending tool calls to take care of, so we will want to handle those first before doing more work
				var toolResults []*Message
				var toolCalls []ToolCall
				if len(a.pendingToolCalls) == 0 {
					events, err := a.Model.Invoke(ctx, a.ContextWindow, a.Tools)
					if err != nil {
						out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("run: invoke: %w", err)}
						return
					}
					// Accumulate streamed text so completion frames (which carry
					// empty Content) still produce a full context-window message.
					streamedContent := map[string]string{}
					for chunk := range events {
						a.streamChunk(chunk, out)
						if !chunk.IsComplete && chunk.Content != "" &&
							(chunk.Type == StreamEventMessage || chunk.Type == StreamEventReasoning) {
							key := string(chunk.Type) + ":" + chunk.MessageId
							streamedContent[key] += chunk.Content
						}
						// Only append completed messages to the context window
						if chunk.IsComplete {
							toolCalls = append(toolCalls, chunk.ToolCalls...)
							role := RoleAssistant
							if chunk.Type == StreamEventReasoning {
								role = RoleReasoning
							}
							content := chunk.Content
							if content == "" {
								key := string(chunk.Type) + ":" + chunk.MessageId
								content = streamedContent[key]
							}
							msg := &Message{
								Role:      role,
								Content:   content,
								ToolCalls: toolCalls,
								MessageID: chunk.MessageId,
							}
							a.ContextWindow = append(a.ContextWindow, msg)
							a.recordOutput(msg)
						}
					}
					// If there are no tool calls in the latest completed message, break the loop
					if len(a.ContextWindow[len(a.ContextWindow)-1].ToolCalls) == 0 {
						out <- StreamEvent{Type: StreamEventComplete}
						return
					}
					toolResults = make([]*Message, len(a.ContextWindow[len(a.ContextWindow)-1].ToolCalls))
				} else {
					toolResults = make([]*Message, len(a.pendingToolCalls))
					toolCalls = make([]ToolCall, 0, len(a.pendingToolCalls))
					for _, tc := range a.pendingToolCalls {
						if !tc.InterruptActive {
							toolCalls = append(toolCalls, *tc.ToolCall)
						}
					}
				}

				var runningTools sync.WaitGroup
				for i, tc := range toolCalls {
					runningTools.Add(1)
					go func(i int, tc ToolCall) {
						defer runningTools.Done()
						tool := a.findTool(tc.Name, tc.Namespace)
						if tool == nil {
							toolErr := fmt.Errorf("tool %q: %w", tc.Name, ErrToolNotFound)
							toolResults[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    toolErr.Error(),
							}
							out <- StreamEvent{Type: StreamEventToolResult, MessageID: tc.CallID, Content: toolResults[i].Content, ToolCalls: []ToolCall{tc}}
							return
						}
						runtimeCopy := a.Runtime
						runtimeCopy.CurrentToolCallID = tc.CallID
						output, err := tool.Invoke(ctx, tc.Arguments, &runtimeCopy)
						// Tools can raise interrupts to pause the loop and return control to the consumer for further input or action
						var interrupt control.Interrupt
						if errors.As(err, &interrupt) {
							intrId := uuid.New().String()
							serialized, err := interrupt.Serialize()
							if err != nil {
								out <- StreamEvent{Type: StreamEventError, Error: fmt.Errorf("serialize interrupt: %w", err)}
								return
							}
							payload := map[string]any{"interruptId": intrId, "data": serialized}
							data, _ := json.Marshal(payload)
							out <- StreamEvent{Type: StreamEventInterrupt, Data: data}
							a.pendingToolCalls[tc.CallID] = pendingToolCall{ToolCall: &tc, InterruptActive: true}
							a.interruptToRequester[intrId] = tc.CallID
							a.checkpointSession(ctx)
							return
						}
						if _, ok := a.pendingToolCalls[tc.CallID]; ok {
							delete(a.pendingToolCalls, tc.CallID)
						}
						// Other general errors that were unhandled in the tool call
						if err != nil {
							toolResults[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    fmt.Sprintf("An error occurred: %s", err.Error()),
							}
							tc.Status = "error"
							out <- StreamEvent{Type: StreamEventToolResult, MessageID: tc.CallID, Content: toolResults[i].Content, ToolCalls: []ToolCall{tc}}
						} else {
							toolResults[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    output,
							}
							tc.Status = "success"
							out <- StreamEvent{Type: StreamEventToolResult, MessageID: tc.CallID, Content: output, ToolCalls: []ToolCall{tc}}
						}
						a.recordToolResult(toolResults[i])
					}(i, tc)
				}
				runningTools.Wait()
				// Append only non-nil results (interrupted tools have nil results)
				for _, r := range toolResults {
					if r != nil {
						a.ContextWindow = append(a.ContextWindow, r)
					}
				}

				// If all pending tool calls are active interrupts, yield back
				// to the consumer and wait for ReturnFromInterrupt.
				if len(a.pendingToolCalls) > 0 {
					err := a.checkpointSession(ctx)
					if err != nil {
						slog.Error("failed to save session", "session_id", a.SessionId, "error", err)
					}
					return
				}
			}
			err := a.checkpointSession(ctx)
			if err != nil {
				slog.Error("failed to save session", "session_id", a.SessionId, "error", err)
			}
		}
	}()
	return out, nil
}

func (a *AgentHarness) ReturnFromInterrupt(ctx context.Context, finishedInterrupts map[string][]byte) (<-chan StreamEvent, error) {
	for interruptId, payload := range finishedInterrupts {
		toolCallId, ok := a.interruptToRequester[interruptId]
		if !ok {
			return nil, fmt.Errorf("no tool call id found for interrupt %s: %w", interruptId, control.ErrInterruptNotFound)
		}
		if _, err := a.Runtime.ReturnInterrupt(toolCallId, payload); err != nil {
			return nil, fmt.Errorf("return from interrupt %q: %w", interruptId, err)
		}
		delete(a.interruptToRequester, interruptId)
		if tc, ok := a.pendingToolCalls[toolCallId]; ok {
			a.pendingToolCalls[toolCallId] = pendingToolCall{ToolCall: tc.ToolCall, InterruptActive: false}
		} else {
			return nil, fmt.Errorf("no pending tool call found for tool call id %s", toolCallId)
		}
	}
	return a.Run(ctx, "")
}

type Config struct {
	MaxWindowSize    int
	SystemPrompt     string
	SkillDirectories []string
}

func NewAgent(cfg Config, model InferenceStrategy, store stores.BaseStore, watchdog AgentWatchDog) *AgentHarness {
	runtime := control.HarnessRuntime{}
	runtime.EnsureInitialized()
	runtime.Store = store
	return &AgentHarness{
		Model:                model,
		MaxWindowSize:        cfg.MaxWindowSize,
		SystemPrompt:         cfg.SystemPrompt,
		Store:                store,
		Runtime:              runtime,
		WatchDog:             watchdog,
		skillDirectories:     cfg.SkillDirectories,
		ContextWindow:        nil,
		SessionId:            "",
		interruptToRequester: make(map[string]string),
		pendingToolCalls:     make(map[string]pendingToolCall),
	}
}

func (a *AgentHarness) initSkills() error {
	if a.skillsInitialized {
		return nil
	}
	loaded, err := skills.LoadDirectories(a.skillDirectories)
	if err != nil {
		return err
	}
	a.skillByName = make(map[string]skills.Skill, len(loaded))
	for _, skill := range loaded {
		a.skillByName[skill.Name] = skill
	}
	if len(loaded) > 0 {
		a.SystemPrompt = strings.TrimSpace(a.SystemPrompt + "\n\n" + skills.Catalog(loaded))
		a.Model.SetSystemPrompt(a.SystemPrompt)
		a.Tools = append(a.Tools, a.skillTool())
	}
	a.skillsInitialized = true
	return nil
}

func (a *AgentHarness) skillTool() *Tool {
	return &Tool{Name: "read_skill", Description: "Load the full instructions for an available skill.", Handler: func(args struct {
		Name string `json:"name" desc:"Skill name from the available skills catalog"`
	}) (string, error) {
		skill, ok := a.skillByName[args.Name]
		if !ok {
			return "", fmt.Errorf("unknown skill %q", args.Name)
		}
		return skill.Instructions, nil
	}}
}

func NewAgentHarnessFromSession(ctx context.Context, sessionId string, cfg Config, model InferenceStrategy, store stores.BaseStore, watchdog AgentWatchDog) (*AgentHarness, error) {
	winBytes, stateBytes, err := store.LoadSession(ctx, sessionId)
	if err != nil {
		return nil, fmt.Errorf("load session %q: %w", sessionId, err)
	}
	var contextWindow []*Message
	if err := json.Unmarshal(winBytes, &contextWindow); err != nil {
		return nil, fmt.Errorf("unmarshal context window: %w", err)
	}
	var state harnessSessionState
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		return nil, fmt.Errorf("unmarshal session state: %w", err)
	}
	state.Runtime.Store = store
	state.Runtime.EnsureInitialized()
	h := &AgentHarness{
		Model:                model,
		Store:                store,
		SessionId:            sessionId,
		ContextWindow:        contextWindow,
		Runtime:              state.Runtime,
		WatchDog:             watchdog,
		MaxWindowSize:        cfg.MaxWindowSize,
		SystemPrompt:         cfg.SystemPrompt,
		compressionPrompt:    "",
		streamingStrategy:    nil,
		interruptToRequester: state.InterruptToRequester,
		pendingToolCalls:     state.PendingToolCalls,
		skillDirectories:     cfg.SkillDirectories,
	}
	return h, nil
}
