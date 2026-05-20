package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

type SessionStore interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

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
	WithResponseStrategy(string) InferenceStrategy
	SetSystemPrompt(string)
	Invoke(context.Context, []*Message, []*Tool) (chan LLMResponseChunk, error)
	CountTokens(context.Context, []*Message, []*Tool) (int, error)
	CompressContextWindow() error
	MaxContextWindow() (int, error)
}

type HarnessRuntime struct {
	GraphDB  *neo4j.Driver
	VectorDB *milvusclient.Client
	Store    SessionStore
	State    map[string]any
}

type AgentHarness struct {
	Model             InferenceStrategy
	SessionId         string
	Tools             []*Tool
	SystemPrompt      string
	ContextWindow     []*Message
	Runtime           HarnessRuntime
	WatchDog          AgentWatchDog
	MaxWindowSize     int
	compressionPrompt string
	streamingStrategy StreamingStategy
}

func (a *AgentHarness) checkpointSession(ctx context.Context, isYielding bool) {
	slog.Info("Checkpointing session", "sessionId", a.SessionId, "contextWindowSize", len(a.ContextWindow))
	winJsonb, err := json.Marshal(a.ContextWindow)
	stateJsonb, err := json.Marshal(a.Runtime.State)
	if err != nil {
		slog.Error("Failed to marshal context window", "sessionId", a.SessionId, "error", err)
		return
	}
	query := `UPDATE postgres.session SET context_window = $1, state = $2, is_yielding = $3 WHERE session_id = $4`
	_, err = a.Runtime.Store.Exec(ctx, query, winJsonb, stateJsonb, isYielding, a.SessionId)
	if err != nil {
		slog.Error("Failed to update session context window", "sessionId", a.SessionId, "error", err)
	}
}

func (a *AgentHarness) fitContextWindowBeforeNextTurn(ctx context.Context, nextPrompt *Message, out chan StreamEvent) error {
	if len(a.ContextWindow) == 0 {
		a.ContextWindow = append(a.ContextWindow, nextPrompt)
		if len(nextPrompt.Content) < 50 {
			slog.Info("Starting new agent turn", "prompt", nextPrompt.Content)
		} else {
			slog.Info("Starting new agent turn", "prompt", nextPrompt.Content[:50])
		}
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
	slog.Info("Max window size exceeded, compressing context window", "maxSize", a.MaxWindowSize, "currentSize", currSize)
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
		if err := a.streamingStrategy.Stream(chunk, out); err != nil {
			slog.Error("streaming chunk", "error", err)
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

func (a *AgentHarness) WithStreamingStrategy(strategy string) *AgentHarness {
	switch strategy {
	case "buffered":
		a.streamingStrategy = &BufferedStreamer{}
	default:
		a.streamingStrategy = nil // use default passthrough
	}
	return a
}

func (a *AgentHarness) Run(ctx context.Context, prompt string) (<-chan StreamEvent, error) {
	out := make(chan StreamEvent)
	err := a.fitContextWindowBeforeNextTurn(ctx, &Message{Role: RoleUser, Content: prompt}, out)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				out <- StreamEvent{Type: StreamEventError, Error: ctx.Err()}
				return
			default:
				events, err := a.Model.Invoke(ctx, a.ContextWindow, a.Tools)
				if err != nil {
					out <- StreamEvent{Type: StreamEventError, Error: err}
					return
				}
				var calls []ToolCall
				for chunk := range events {
					a.streamChunk(chunk, out)
					// Only append completed messages to the context window
					if chunk.IsComplete {
						calls = append(calls, chunk.ToolCalls...)
						msg := &Message{Role: RoleAssistant, Content: chunk.Content, ToolCalls: calls}
						a.ContextWindow = append(a.ContextWindow, msg)
						a.recordOutput(msg)
					}
				}

				var wg sync.WaitGroup
				// If there are no tool calls in the latest completed message, break the loop
				if len(a.ContextWindow[len(a.ContextWindow)-1].ToolCalls) == 0 {
					return
				}
				results := make([]*Message, len(a.ContextWindow[len(a.ContextWindow)-1].ToolCalls))
				for i, tc := range a.ContextWindow[len(a.ContextWindow)-1].ToolCalls {
					wg.Add(1)
					go func(i int, tc ToolCall) {
						defer wg.Done()
						tool := a.findTool(tc.Name, tc.Namespace)
						if tool == nil {
							results[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    fmt.Sprintf("tool %q not found", tc.Name),
							}
							out <- StreamEvent{Type: StreamEventToolResult, Content: results[i].Content}
							return
						}
						output, err := tool.Invoke(tc.Arguments, &a.Runtime)
						// If the tool yields control back to the consumer, send a yield event and pause the loop
						var yieldErr *YieldToConsumerError
						if errors.As(err, &yieldErr) {
							out <- StreamEvent{Type: StreamEventYield, Data: yieldErr.Data}
							a.checkpointSession(ctx, true)
							return
						}
						// Other general errors that were unhandled in the tool call
						if err != nil {
							results[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    err.Error(),
							}
							out <- StreamEvent{Type: StreamEventToolResult, Content: results[i].Content}
						} else {
							results[i] = &Message{
								Role:       RoleTool,
								ToolCallID: tc.CallID,
								Content:    output,
							}
							a.recordToolResult(results[i])
							out <- StreamEvent{Type: StreamEventToolResult, Content: output}
						}
					}(i, tc)
				}
				wg.Wait()
				a.ContextWindow = append(a.ContextWindow, results...)
				a.checkpointSession(ctx, false)
			}
		}
	}()
	return out, nil
}

func NewAgentHarnessFromSession(ctx context.Context, sessionId string, model InferenceStrategy, runtime HarnessRuntime, watchdog AgentWatchDog, streamer StreamingStategy) (*AgentHarness, error) {
	query := `SELECT context_window, state, is_yielding FROM postgres.session WHERE session_id = $1`
	row := runtime.Store.QueryRow(ctx, query, sessionId)
	var contextWindow []*Message
	var state map[string]any
	var isYielding bool
	err := row.Scan(&contextWindow, &state, &isYielding)
	if err != nil {
		return nil, err
	}
	return &AgentHarness{
		Model:             model,
		SessionId:         sessionId,
		ContextWindow:     contextWindow,
		Runtime:           runtime,
		WatchDog:          watchdog,
		MaxWindowSize:     8192,
		compressionPrompt: "",
		streamingStrategy: streamer,
	}, nil
}
