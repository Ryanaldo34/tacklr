package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/ryanaldo34/tacklr/control"
)

// AbsorbResult is returned by ModelTasks.Absorb after incorporating a message.
type AbsorbResult struct {
	// SummaryChunks when StreamFitSummary is true; harness streams to the client.
	SummaryChunks []LLMResponseChunk
}

// ModelTasks runs product-level model operations.
// It uses InferenceStrategy for transport and provider CountTokens, and
// ContextManager for reading/applying structured windows.
// Token counting is not a product method — it stays on InferenceStrategy.
type ModelTasks interface {
	// Turn streams the next agent step for the current context and tools.
	Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error)

	// Absorb incorporates msg under window pressure (may summarize via the model).
	Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error)

	// Handoff rebuilds context after complete_todo or plan-document edit.
	Handoff(ctx context.Context, plan []control.Todo, planDoc string, tools []*Tool, systemPrompt string) error
}

// DefaultModelTasks is the standard ModelTasks implementation.
type DefaultModelTasks struct {
	model   InferenceStrategy
	context ContextManager
	policy  ContextPolicy
	maxSize int
}

// NewDefaultModelTasks wires provider + context structure for product model ops.
func NewDefaultModelTasks(model InferenceStrategy, ctx ContextManager, policy ContextPolicy, maxSize int) *DefaultModelTasks {
	if policy.PressureRatio <= 0 && policy.CompressFraction <= 0 {
		policy = DefaultContextPolicy()
	}
	return &DefaultModelTasks{
		model:   model,
		context: ctx,
		policy:  policy,
		maxSize: maxSize,
	}
}

func (t *DefaultModelTasks) Turn(ctx context.Context, tools []*Tool, systemPrompt string) (<-chan LLMResponseChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.model == nil {
		return nil, fmt.Errorf("turn: model is required")
	}
	if systemPrompt != "" {
		t.model.SetSystemPrompt(systemPrompt)
	}
	return t.model.Invoke(ctx, t.context.Messages(), tools)
}

func (t *DefaultModelTasks) Absorb(ctx context.Context, msg *Message, tools []*Tool, systemPrompt string) (AbsorbResult, error) {
	if err := ctx.Err(); err != nil {
		return AbsorbResult{}, err
	}
	if msg == nil {
		return AbsorbResult{}, nil
	}
	window, chunks, err := absorbFit(ctx, t.context.Messages(), msg, t.model, tools, t.maxSize, t.policy, systemPrompt)
	if err != nil {
		return AbsorbResult{}, err
	}
	t.context.Replace(window)
	return AbsorbResult{SummaryChunks: chunks}, nil
}

func (t *DefaultModelTasks) Handoff(ctx context.Context, plan []control.Todo, planDoc string, tools []*Tool, systemPrompt string) error {
	window, err := handoffGenerate(ctx, t.context.Messages(), plan, planDoc, t.model, tools, systemPrompt)
	if err != nil {
		return err
	}
	t.context.Replace(window)
	return nil
}

// absorbFit collapses under pressure then appends newMsg.
// Uses InferenceStrategy.CountTokens (provider-specific) and Invoke for summary.
func absorbFit(
	ctx context.Context,
	window []*Message,
	newMsg *Message,
	model InferenceStrategy,
	tools []*Tool,
	maxSize int,
	policy ContextPolicy,
	restoreSystemPrompt string,
) ([]*Message, []LLMResponseChunk, error) {
	if policy.PressureRatio <= 0 {
		policy.PressureRatio = DefaultContextPolicy().PressureRatio
	}
	if policy.CompressFraction <= 0 {
		policy.CompressFraction = DefaultContextPolicy().CompressFraction
	}

	tempWindow := append(append([]*Message(nil), window...), newMsg)
	if model == nil {
		return tempWindow, nil, nil
	}

	currSize, err := model.CountTokens(ctx, tempWindow, tools)
	if err != nil {
		slog.Error("failed to count tokens while absorbing message", "area", "model_tasks", "error", err)
		return nil, nil, fmt.Errorf("count tokens: %w", err)
	}
	if len(window) == 0 || float64(currSize) <= float64(maxSize)*policy.PressureRatio {
		return tempWindow, nil, nil
	}

	slog.Info("max context window size exceeded or approaching, compressing context window",
		"area", "model_tasks", "max_size", maxSize, "current_size", currSize)

	model.SetSystemPrompt(fmt.Sprintf(
		"Please summarize the entire message history into a single, concise summary including key items for your current and past tasks with a primary focus on your current task. Current task or follow-up question to answer: %s",
		newMsg.Content,
	))

	anchorLen := protectedPrefixLen(window)
	if anchorLen == 0 {
		anchorLen = 1
	}
	if anchorLen > len(window) {
		anchorLen = len(window)
	}
	anchors := window[:anchorLen]
	unprotected := window[anchorLen:]
	if len(unprotected) == 0 {
		if restoreSystemPrompt != "" {
			model.SetSystemPrompt(restoreSystemPrompt)
		}
		return tempWindow, nil, nil
	}

	rebuild := func(mid []*Message) []*Message {
		out := make([]*Message, 0, anchorLen+len(mid)+1)
		out = append(out, anchors...)
		out = append(out, mid...)
		out = append(out, newMsg)
		return out
	}

	numMessagesToCompress := int(math.Round(float64(len(unprotected)) * policy.CompressFraction))
	if currSize > maxSize {
		diff := currSize - maxSize
		start := int(math.Round(float64(diff) * policy.CompressFraction))
		if start < 1 {
			start = 1
		}
		if start > len(unprotected) {
			start = len(unprotected)
		}
		for start < len(unprotected) {
			count, err := model.CountTokens(ctx, rebuild(unprotected[start:]), tools)
			if err != nil {
				return nil, nil, fmt.Errorf("count tokens: %w", err)
			}
			if float64(count) <= float64(maxSize)*policy.PressureRatio {
				break
			}
			start++
		}
		numMessagesToCompress = start
	}
	if numMessagesToCompress < 1 {
		numMessagesToCompress = 1
	}
	if numMessagesToCompress > len(unprotected) {
		numMessagesToCompress = len(unprotected)
	}

	events, err := model.Invoke(ctx, unprotected[:numMessagesToCompress], tools)
	if err != nil {
		return nil, nil, fmt.Errorf("invoke: %w", err)
	}

	compressed := &Message{Role: RoleAssistant}
	var chunks []LLMResponseChunk
	for chunk := range events {
		if chunk.Type == StreamEventError {
			return nil, nil, fmt.Errorf("compress: %s", chunk.Content)
		}
		if policy.StreamFitSummary {
			chunks = append(chunks, chunk)
		}
		compressed.Content += chunk.Content
	}

	mid := append([]*Message{compressed}, unprotected[numMessagesToCompress:]...)
	if restoreSystemPrompt != "" {
		model.SetSystemPrompt(restoreSystemPrompt)
	}
	return rebuild(mid), chunks, nil
}

func handoffGenerate(
	ctx context.Context,
	window []*Message,
	plan []control.Todo,
	planDoc string,
	model InferenceStrategy,
	tools []*Tool,
	restoreSystemPrompt string,
) ([]*Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if model == nil {
		return nil, fmt.Errorf("handoff: model is required")
	}
	if len(window) == 0 || window[0] == nil {
		return nil, fmt.Errorf("handoff: empty window")
	}

	var planB strings.Builder
	for _, todo := range plan {
		line := fmt.Sprintf("- %s: %s\nStatus: %s\n", todo.Title, todo.Description, todo.Status)
		planB.WriteString(line)
	}
	prompt := fmt.Sprintf(
		`Your task is to produce a handoff for someone to complete the remaining todo items in the plan, not a summary of the completed work, but rather, an informative overview of the process that has completed the work so far. You will ensure to inform the handoff recipient that this is a work in progress and that they should expect to complete the remaining todo items. This is your only task, and you will not add any additional commentary, thoughts, etc. This is not a generic summary as the handoff needs to include the following sections:
	Objective: Overall mission and success criteria (brief; a durable PROJECT PLAN document will remain in context — do not restate the full blueprint).
	Completed Work: What is now true because of the completed todo(s) and an overview of the current state of the plan & implementation. Someone should know exactly what was done & what work is remaining and be able to pick up the remaining work seamlessly.
	Key Decisions: Architectural or implementation choices that should not be revisited.
	State Changes: Files changed, APIs added/removed, new abstractions, configuration changes, etc.
	Discoveries: Facts learned that affect remaining work (including anything that forced a plan revision).
	Constraints: Requirements, assumptions, and invariants that future todos must respect.
	Remaining Work: Newly discovered tasks, blockers, or dependencies.
	Validation: What was verified and what still requires verification.
	Relevant Context for Remaining Todos: Only information the next todos are likely to need which was gathered or observed in the completed work.

Current plan todos:
%s`, planB.String())

	model.SetSystemPrompt(prompt)
	events, err := model.Invoke(ctx, window, tools)
	if err != nil {
		return nil, err
	}

	asm := newStreamAssembler()
	var lastCompletedMessage string
	for chunk := range events {
		if chunk.Type == StreamEventError {
			return nil, fmt.Errorf("compress: %s", chunk.Content)
		}
		asm.AddDelta(chunk)
		if chunk.IsComplete && chunk.Type == StreamEventMessage {
			if content := asm.CompleteContent(chunk); content != "" {
				lastCompletedMessage = content
			}
		}
	}

	user := *window[0]
	out := make([]*Message, 0, 4)
	out = append(out, &user)
	if planDoc != "" {
		out = append(out, buildPlanDocumentMessage(planDoc))
	}
	out = append(out, &Message{Role: RoleDeveloper, Content: lastCompletedMessage})
	if planHasOpenTodos(plan) {
		out = append(out, &Message{
			Role:    RoleDeveloper,
			Content: continuePlanNudge,
		})
	}
	if restoreSystemPrompt != "" {
		model.SetSystemPrompt(restoreSystemPrompt)
	}
	return out, nil
}
