package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

// ContextPolicy controls when and how conversation windows are reshaped.
type ContextPolicy struct {
	// PressureRatio is the fraction of MaxSize at which Fit starts collapsing
	// (e.g. 0.85 means compress when estimated tokens exceed 85% of max).
	PressureRatio float64
	// CompressFraction is the heuristic used when estimating how much of the
	// window to summarize (message fraction and token-diff seed).
	CompressFraction float64
	// StreamFitSummary, when true, Fit returns model summary chunks for the
	// harness to stream to the client (current product default).
	StreamFitSummary bool
}

// DefaultContextPolicy matches historical harness behavior.
func DefaultContextPolicy() ContextPolicy {
	return ContextPolicy{
		PressureRatio:    0.85,
		CompressFraction: 0.25,
		StreamFitSummary: true,
	}
}

// FitInput is the window + message Fit should incorporate.
type FitInput struct {
	Window              []*Message
	NewMsg              *Message
	Tools               []*Tool
	MaxSize             int
	Policy              ContextPolicy
	Model               InferenceStrategy
	RestoreSystemPrompt string // applied after Fit completes
}

// FitResult is the window ready for the next step, plus optional stream chunks.
type FitResult struct {
	Window []*Message
	// Chunks are model deltas from the summary invoke when StreamFitSummary is true.
	// The harness is responsible for cancel-aware emission.
	Chunks []LLMResponseChunk
}

// HandoffInput drives post-complete_todo window rebuild.
type HandoffInput struct {
	Window              []*Message
	Plan                []control.Todo
	Tools               []*Tool
	Model               InferenceStrategy
	RestoreSystemPrompt string
}

// HandoffResult is the silent post-handoff window (no client stream).
type HandoffResult struct {
	Window []*Message
}

// ContextManager reshapes conversation windows. It does not run tools or the agent loop.
type ContextManager interface {
	Fit(ctx context.Context, in FitInput) (FitResult, error)
	Handoff(ctx context.Context, in HandoffInput) (HandoffResult, error)
}

// ModelContextManager implements ContextManager via the model for summarize/handoff.
type ModelContextManager struct{}

// NewModelContextManager returns the default ContextManager implementation.
func NewModelContextManager() *ModelContextManager {
	return &ModelContextManager{}
}

// Fit collapses the window when token estimate exceeds the pressure threshold,
// then appends NewMsg. When under pressure, it summarizes a prefix of the window.
func (m *ModelContextManager) Fit(ctx context.Context, in FitInput) (FitResult, error) {
	if err := ctx.Err(); err != nil {
		return FitResult{}, err
	}
	if in.NewMsg == nil {
		return FitResult{Window: in.Window}, nil
	}
	policy := in.Policy
	if policy.PressureRatio <= 0 {
		policy.PressureRatio = DefaultContextPolicy().PressureRatio
	}
	if policy.CompressFraction <= 0 {
		policy.CompressFraction = DefaultContextPolicy().CompressFraction
	}

	tempWindow := append(append([]*Message(nil), in.Window...), in.NewMsg)
	if in.Model == nil {
		return FitResult{Window: tempWindow}, nil
	}

	currSize, err := in.Model.CountTokens(ctx, tempWindow, in.Tools)
	if err != nil {
		slog.Error("failed to count tokens while fitting context window", "area", "context_management", "error", err)
		return FitResult{}, fmt.Errorf("count tokens: %w", err)
	}
	if len(in.Window) == 0 || float64(currSize) <= float64(in.MaxSize)*policy.PressureRatio {
		return FitResult{Window: tempWindow}, nil
	}

	slog.Info("max context window size exceeded or approaching, compressing context window",
		"area", "context_management", "max_size", in.MaxSize, "current_size", currSize)

	in.Model.SetSystemPrompt(fmt.Sprintf(
		"Please summarize the entire message history into a single, concise summary including key items for your current and past tasks with a primary focus on your current task. Current task or follow-up question to answer: %s",
		in.NewMsg.Content,
	))

	windowLen := len(in.Window)
	if windowLen == 0 {
		if in.RestoreSystemPrompt != "" {
			in.Model.SetSystemPrompt(in.RestoreSystemPrompt)
		}
		return FitResult{Window: tempWindow}, nil
	}

	numMessagesToCompress := int(math.Round(float64(windowLen) * policy.CompressFraction))
	if currSize > in.MaxSize {
		diff := currSize - in.MaxSize
		start := int(math.Round(float64(diff) * policy.CompressFraction))
		if start < 1 {
			start = 1
		}
		if start > windowLen {
			start = windowLen
		}
		for start < windowLen {
			staged := tempWindow[start:]
			count, err := in.Model.CountTokens(ctx, staged, in.Tools)
			if err != nil {
				return FitResult{}, fmt.Errorf("count tokens: %w", err)
			}
			if float64(count) <= float64(in.MaxSize)*policy.PressureRatio {
				break
			}
			start++
		}
		numMessagesToCompress = start
	}
	if numMessagesToCompress < 1 {
		numMessagesToCompress = 1
	}
	if numMessagesToCompress > windowLen {
		numMessagesToCompress = windowLen
	}

	contextToSummarize := in.Window[:numMessagesToCompress]
	events, err := in.Model.Invoke(ctx, contextToSummarize, in.Tools)
	if err != nil {
		return FitResult{}, fmt.Errorf("invoke: %w", err)
	}

	firstUserMsg := in.Window[0]
	compressed := &Message{Role: RoleAssistant}
	var chunks []LLMResponseChunk
	for chunk := range events {
		if chunk.Type == StreamEventError {
			return FitResult{}, fmt.Errorf("compress: %s", chunk.Content)
		}
		if policy.StreamFitSummary {
			chunks = append(chunks, chunk)
		}
		compressed.Content += chunk.Content
	}

	newWindow := append([]*Message{firstUserMsg, compressed}, tempWindow[numMessagesToCompress:]...)
	if in.RestoreSystemPrompt != "" {
		in.Model.SetSystemPrompt(in.RestoreSystemPrompt)
	}
	return FitResult{Window: newWindow, Chunks: chunks}, nil
}

// Handoff silently invokes the model to produce a developer handoff and returns
// [firstUser, handoff, optional continue nudge] when plan work remains.
func (m *ModelContextManager) Handoff(ctx context.Context, in HandoffInput) (HandoffResult, error) {
	if err := ctx.Err(); err != nil {
		return HandoffResult{}, err
	}
	if in.Model == nil {
		return HandoffResult{}, fmt.Errorf("handoff: model is required")
	}

	var plan strings.Builder
	for _, todo := range in.Plan {
		line := fmt.Sprintf("- %s: %s\nStatus: %s\n", todo.Title, todo.Description, todo.Status)
		plan.WriteString(line)
	}
	prompt := fmt.Sprintf(
		`Your task is to produce a handoff for someone to complete the remaining todo items in the plan, not a summary of the completed work, but rather, an informative overview of the process that has completed the work so far. You will ensure to inform the handoff recipient that this is a work in progress and that they should expect to complete the remaining todo items. This is your only task, and you will not add any additional commentary, thoughts, etc. This is not a generic summary as the handoff needs to include the following sections:
	Objective: Overall mission and success criteria analyzed from the plan and to-do items. This is outlined by the original user prompt and should be carried forward in the handoff (or is in previous handoff summaries).
	Completed Work: What is now true because of the completed todo(s) and an overview of the current state of the plan & implementation. Someone should know exactly what was done & what work is remaining and be able to pick up the remaining work seamlessly.
	Key Decisions: Architectural or implementation choices that should not be revisited.
	State Changes: Files changed, APIs added/removed, new abstractions, configuration changes, etc.
	Discoveries: Facts learned that affect remaining work.
	Constraints: Requirements, assumptions, and invariants that future todos must respect.
	Remaining Work: Newly discovered tasks, blockers, or dependencies.
	Validation: What was verified and what still requires verification.
	Relevant Context for Remaining Todos: Only information the next todos are likely to need which was gathered or observed in the completed work.

Current plan todos:
%s`, plan.String())

	in.Model.SetSystemPrompt(prompt)
	events, err := in.Model.Invoke(ctx, in.Window, in.Tools)
	if err != nil {
		return HandoffResult{}, err
	}

	// Silent: do not stream. Last completed StreamEventMessage full text only.
	asm := newStreamAssembler()
	var lastCompletedMessage string
	for chunk := range events {
		if chunk.Type == StreamEventError {
			return HandoffResult{}, fmt.Errorf("compress: %s", chunk.Content)
		}
		asm.AddDelta(chunk)
		if chunk.IsComplete && chunk.Type == StreamEventMessage {
			if content := asm.CompleteContent(chunk); content != "" {
				lastCompletedMessage = content
			}
		}
	}

	firstUser := firstUserMessage(in.Window)
	if firstUser == nil {
		firstUser = &Message{Role: RoleUser, Content: "Continue the active plan."}
	}
	window := []*Message{
		firstUser,
		{Role: RoleDeveloper, Content: lastCompletedMessage},
	}
	if planHasOpenTodos(in.Plan) {
		window = append(window, &Message{
			Role:    RoleDeveloper,
			Content: continuePlanNudge,
		})
	}
	if in.RestoreSystemPrompt != "" {
		in.Model.SetSystemPrompt(in.RestoreSystemPrompt)
	}
	return HandoffResult{Window: window}, nil
}

// continuePlanNudge is appended after handoff compression when todos remain so
// the next Invoke keeps tool-calling instead of ending the turn.
const continuePlanNudge = `The plan still has incomplete todos. Continue executing now: work the in-progress todo (or the next pending one), call tools as needed, and do not stop for user confirmation. Do not restate the handoff; act on the next todo.`

func planHasOpenTodos(plan []control.Todo) bool {
	for _, todo := range plan {
		if todo.Status != streaming.TodoStatusCompleted {
			return true
		}
	}
	return false
}
