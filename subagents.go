package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Sentinel errors for the subagent orchestrator.
var (
	ErrWorkerNotFound    = errors.New("worker not found")
	ErrWorkerNoOutput    = errors.New("worker produced no output")
	ErrWorkerIncomplete  = errors.New("worker finished without completing")
	ErrWorkerNoModel     = errors.New("worker has no model")
	ErrEmptyWorkerTask   = errors.New("worker task is empty")
	ErrWorkerParkMissing = errors.New("parked worker state is missing")
)

const parkedWorkersStateKey = "_parked_workers"

// SubAgent describes a specialized worker that a harness can spawn via the
// spawn_worker tool. Specs may nest via SubAgents so interrupt propagation
// and orchestration stay self-similar at any depth.
type SubAgent struct {
	Tools        []*Tool
	Instructions string
	Model        InferenceStrategy
	WorkerName   string
	Description  string
	// SubAgents are nested workers available to this worker when it runs.
	SubAgents []*SubAgent
}

// parkedWorkerMeta is durable park metadata stored in user State (SessionManager bag).
// Live harness pointers are not stored here; they live in parkedWorkersLive.
type parkedWorkerMeta struct {
	WorkerName        string   `json:"workerName"`
	WorkerSessionID   string   `json:"workerSessionId"`
	Task              string   `json:"task"`
	ChildInterruptIDs []string `json:"childInterruptIds"`
}

// initSubAgentWorkers registers worker specs on the harness. Invalid entries
// (nil, empty name, nil model) are skipped with a warning. Duplicate names
// keep the first registration and log a warning for later ones.
func (h *AgentHarness) initSubAgentWorkers(specs []*SubAgent) {
	if h.subagents == nil {
		h.subagents = make(map[string]*SubAgent)
	}
	for _, spec := range specs {
		if spec == nil {
			slog.Warn("skipping nil subagent spec", "area", "subagent")
			continue
		}
		if spec.WorkerName == "" {
			slog.Warn("skipping subagent with empty worker name", "area", "subagent")
			continue
		}
		if spec.Model == nil {
			slog.Warn("skipping subagent with nil model", "area", "subagent", "worker", spec.WorkerName)
			continue
		}
		if _, exists := h.subagents[spec.WorkerName]; exists {
			slog.Warn("duplicate subagent worker name; keeping first", "area", "subagent", "worker", spec.WorkerName)
			continue
		}
		// Store a shallow copy so later mutation of the caller's slice
		// headers does not affect the registry entry.
		cp := *spec
		h.subagents[spec.WorkerName] = &cp
	}
}

// workerNames returns registered worker names in sorted order.
func (a *AgentHarness) workerNames() []string {
	if len(a.subagents) == 0 {
		return nil
	}
	names := make([]string, 0, len(a.subagents))
	for name := range a.subagents {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// formatSubAgentPromptList builds the deterministic AVAILABLE SUB-AGENTS list.
func (a *AgentHarness) formatSubAgentPromptList() string {
	names := a.workerNames()
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	for _, name := range names {
		spec := a.subagents[name]
		if spec.Description != "" {
			fmt.Fprintf(&b, " - %s: %s\n", name, spec.Description)
		} else {
			fmt.Fprintf(&b, " - %s\n", name)
		}
	}
	return b.String()
}

type spawnWorkerArgs struct {
	TaskDescriptionAndContext string `json:"task_description_and_context" desc:"Clear task goal, acceptance criteria, and helpful context for the worker"`
	WorkerName                string `json:"worker_name" desc:"Name of a registered sub-agent worker to spawn"`
}

func (a *AgentHarness) spawnTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "spawn_worker",
		DisplayName: "Spawn Worker",
		Description: "Use to spawn a sub-agent or \"worker\" to help parallelize a task or handle smaller subtasks and assist with research. Ensure the task is clearly outlined with a clear goal, acceptance criteria, and helpful context. Choose worker_name from the AVAILABLE SUB-AGENTS listed in the system prompt.",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, args spawnWorkerArgs, runtime HarnessRuntime) (string, error) {
			return a.runWorker(ctx, args.WorkerName, args.TaskDescriptionAndContext, runtime)
		},
	})
}

// runWorker creates or resumes an isolated worker harness, drains events into
// parent tool updates, and either returns final output or bubbles a child
// interrupt onto the parent Runtime (self-similar at any nesting depth).
func (a *AgentHarness) runWorker(ctx context.Context, workerName, task string, runtime HarnessRuntime) (string, error) {
	spec, ok := a.subagents[workerName]
	if !ok {
		return "", fmt.Errorf("worker %q: %w", workerName, ErrWorkerNotFound)
	}
	if strings.TrimSpace(task) == "" && a.getParkMeta(runtime.CurrentToolCallID) == nil {
		return "", fmt.Errorf("worker %q: %w", workerName, ErrEmptyWorkerTask)
	}
	if spec.Model == nil {
		return "", fmt.Errorf("worker %q: %w", workerName, ErrWorkerNoModel)
	}

	toolCallID := runtime.CurrentToolCallID
	logAttrs := []any{
		"area", "subagent",
		"session_id", a.sessionId,
		"worker", workerName,
		"tool_call_id", toolCallID,
		"task_len", len(task),
	}

	start := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Drop any residual resolved interrupt for this spawn call — resume is
	// driven by park metadata + stashed resolution payloads, not RaiseInterrupt.
	_, _ = runtime.TakeResolvedInterrupt(toolCallID)

	var worker *AgentHarness
	var closeOnExit bool
	meta := a.getParkMeta(toolCallID)
	resuming := meta != nil

	if resuming {
		slog.Info("resuming parked worker", logAttrs...)
		if meta.Task != "" {
			task = meta.Task
		}
		var err error
		worker, err = a.attachParkedWorker(ctx, toolCallID, *meta, spec)
		if err != nil {
			return "", fmt.Errorf("worker %q: %w", workerName, err)
		}
		// Live-attached workers stay in the cache until success; loaded ones
		// should be closed when this invocation finishes (success or re-park).
		a.parkMu.Lock()
		_, live := a.parkedWorkersLive[toolCallID]
		a.parkMu.Unlock()
		closeOnExit = !live
	} else {
		slog.Info("spawning worker", logAttrs...)
		worker = a.newWorkerHarness(workerCtx, workerName, toolCallID, spec)
		closeOnExit = true
	}

	// On success we close; on bubble we keep the worker (live park) and must
	// not close. Track with a flag.
	parked := false
	defer func() {
		if closeOnExit && !parked {
			worker.Close()
		}
	}()

	var events <-chan StreamEvent
	var err error
	if resuming {
		payloads := a.childResolutionPayloads(toolCallID, meta)
		if len(payloads) == 0 {
			return "", fmt.Errorf("worker %q: resume missing resolution payload", workerName)
		}
		events, err = worker.ReturnFromInterrupt(workerCtx, payloads)
		if err != nil {
			a.clearPark(toolCallID)
			return "", fmt.Errorf("resuming worker %q: %w", workerName, err)
		}
		runtime.EmitUpdate(fmt.Sprintf("Worker %q resumed", workerName))
	} else {
		events, err = worker.Run(workerCtx, task)
		if err != nil {
			slog.Error("failed to start worker", append(logAttrs, "error", err, "elapsed", time.Since(start).Round(time.Millisecond))...)
			return "", fmt.Errorf("starting worker %q: %w", workerName, err)
		}
		runtime.EmitUpdate(fmt.Sprintf("Worker %q started", workerName))
	}

	drained, drainErr := drainWorkerEvents(ctx, workerName, events, runtime.EmitUpdate)
	elapsed := time.Since(start).Round(time.Millisecond)

	if ctx.Err() != nil {
		a.clearPark(toolCallID)
		slog.Info("worker cancelled", append(logAttrs, "elapsed", elapsed, "error", ctx.Err())...)
		return "", ctx.Err()
	}
	if drainErr != nil {
		a.clearPark(toolCallID)
		slog.Warn("worker failed", append(logAttrs, "elapsed", elapsed, "error", drainErr)...)
		return "", fmt.Errorf("worker %q failed: %w", workerName, drainErr)
	}
	if drained.completed {
		a.clearPark(toolCallID)
		result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
		if result == "" {
			slog.Warn("worker produced no output", append(logAttrs, "elapsed", elapsed)...)
			return "", fmt.Errorf("worker %q: %w", workerName, ErrWorkerNoOutput)
		}
		slog.Info("worker completed", append(logAttrs, "elapsed", elapsed, "output_length", len(result))...)
		return result, nil
	}

	// Incomplete: bubble child interrupts, or fail.
	childIntrIDs, childIntr := collectChildInterrupts(worker, drained.interruptIDs)
	if childIntr == nil {
		a.clearPark(toolCallID)
		slog.Warn("worker incomplete", append(logAttrs, "elapsed", elapsed)...)
		return "", fmt.Errorf("worker %q: %w", workerName, ErrWorkerIncomplete)
	}

	// Ensure child is durable when a store is available.
	if worker.store != nil && worker.sessionId != "" {
		if err := worker.checkpointSession(ctx); err != nil {
			slog.Error("failed to checkpoint worker", append(logAttrs, "error", err)...)
			// Continue with live park; durability is best-effort here.
		}
	}

	parkMeta := parkedWorkerMeta{
		WorkerName:        workerName,
		WorkerSessionID:   worker.sessionId,
		Task:              task,
		ChildInterruptIDs: childIntrIDs,
	}
	a.setPark(toolCallID, parkMeta, worker)
	parked = true
	closeOnExit = false

	slog.Info("bubbling worker interrupt", append(logAttrs,
		"elapsed", elapsed,
		"child_interrupts", len(childIntrIDs),
		"worker_session_id", worker.sessionId,
	)...)

	// Adopt the same interrupt object onto the parent Runtime under this
	// spawn_worker tool call id, then return it as an error so the parent
	// harness parks spawn_worker like any other tool interrupt.
	_, adoptErr := runtime.AdoptInterrupt(childIntr)
	if adoptErr == nil {
		// Resume signal from AdoptInterrupt — unexpected during bubble.
		return "", fmt.Errorf("worker %q: unexpected resolved interrupt during bubble", workerName)
	}
	return "", adoptErr
}

func (a *AgentHarness) newWorkerHarness(ctx context.Context, workerName, parentToolCallID string, spec *SubAgent) *AgentHarness {
	sessionID := workerSessionID(a.sessionId, workerName, parentToolCallID)
	worker := NewAgent(ctx, a.workerOptsFromSpec(spec))
	worker.sessionId = sessionID
	return worker
}

func workerSessionID(parentSessionID, workerName, parentToolCallID string) string {
	if parentSessionID == "" {
		return fmt.Sprintf("w/%s/%s", workerName, parentToolCallID)
	}
	return fmt.Sprintf("%s/w/%s/%s", parentSessionID, workerName, parentToolCallID)
}

func (a *AgentHarness) workerOptsFromSpec(spec *SubAgent) AgentOptions {
	// Preserve parent Exa key so workers get web_search when the parent did
	// (env still works if options key is empty).
	return AgentOptions{
		Config: Config{
			MaxWindowSize: a.maxWindowSize,
			SystemPrompt:  spec.Instructions,
		},
		Model:      spec.Model,
		Tools:      slices.Clone(spec.Tools),
		MCPConfigs: slices.Clone(a.mcpConfigs),
		Store:      a.store,
		SubAgents:  spec.SubAgents,
		ExaAPIKey:  a.exaAPIKey,
	}
}

func (a *AgentHarness) attachParkedWorker(ctx context.Context, toolCallID string, meta parkedWorkerMeta, spec *SubAgent) (*AgentHarness, error) {
	a.parkMu.Lock()
	if live, ok := a.parkedWorkersLive[toolCallID]; ok && live != nil {
		a.parkMu.Unlock()
		return live, nil
	}
	a.parkMu.Unlock()

	if a.store == nil || meta.WorkerSessionID == "" {
		return nil, fmt.Errorf("%w: no live worker and no durable session", ErrWorkerParkMissing)
	}
	worker, err := NewAgentFromSession(ctx, meta.WorkerSessionID, a.workerOptsFromSpec(spec))
	if err != nil {
		return nil, fmt.Errorf("%w: load session %q: %w", ErrWorkerParkMissing, meta.WorkerSessionID, err)
	}
	// Cache for subsequent re-parks in this process.
	a.parkMu.Lock()
	if a.parkedWorkersLive == nil {
		a.parkedWorkersLive = make(map[string]*AgentHarness)
	}
	a.parkedWorkersLive[toolCallID] = worker
	a.parkMu.Unlock()
	return worker, nil
}

func (a *AgentHarness) childResolutionPayloads(parentToolCallID string, meta *parkedWorkerMeta) map[string][]byte {
	if meta == nil || len(meta.ChildInterruptIDs) == 0 {
		return nil
	}
	payload, ok := a.interruptPayloads[parentToolCallID]
	if !ok || len(payload) == 0 {
		return nil
	}
	// One parent spawn_worker interrupt maps to one consumer resolution.
	// Forward the same payload bytes to every pending child interrupt id
	// recorded at park time (typically one).
	out := make(map[string][]byte, len(meta.ChildInterruptIDs))
	for _, id := range meta.ChildInterruptIDs {
		out[id] = payload
	}
	return out
}

func collectChildInterrupts(worker *AgentHarness, drainedIDs []string) (ids []string, primary interrupt.Interrupt) {
	if worker == nil {
		return nil, nil
	}
	// Prefer ids observed on the event stream; fall back to harness maps.
	seen := make(map[string]struct{})
	for _, id := range drainedIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		for id := range worker.interruptToRequester {
			ids = append(ids, id)
		}
	}

	// Resolve primary interrupt object via interruptToRequester → tool call → Pending.
	for _, id := range ids {
		tcID, ok := worker.interruptToRequester[id]
		if !ok {
			continue
		}
		if intr, ok := worker.runtime.PendingInterrupt(tcID); ok {
			primary = intr
			break
		}
	}
	// If stream ids didn't map (e.g. only pending maps populated), scan pending tools.
	if primary == nil {
		for tcID, ptc := range worker.pendingToolCalls {
			if !ptc.InterruptActive {
				continue
			}
			if intr, ok := worker.runtime.PendingInterrupt(tcID); ok {
				primary = intr
				// Ensure we have at least one interrupt id for resume forwarding.
				if len(ids) == 0 {
					for id, mapped := range worker.interruptToRequester {
						if mapped == tcID {
							ids = append(ids, id)
						}
					}
				}
				break
			}
		}
	}
	return ids, primary
}

// --- park metadata (durable user State + live harness cache) ---

// parkStore groups parked-worker get/set/clear over durable state and the
// same-process live map. Callers use a.parks().
type parkStore struct {
	h *AgentHarness
}

func (a *AgentHarness) parks() parkStore { return parkStore{h: a} }

func (a *AgentHarness) getParkMeta(toolCallID string) *parkedWorkerMeta {
	return a.parks().get(toolCallID)
}

func (a *AgentHarness) setPark(toolCallID string, meta parkedWorkerMeta, worker *AgentHarness) {
	a.parks().set(toolCallID, meta, worker)
}

func (a *AgentHarness) clearPark(toolCallID string) {
	a.parks().clear(toolCallID)
}

func (p parkStore) get(toolCallID string) *parkedWorkerMeta {
	if toolCallID == "" {
		return nil
	}
	parks := p.load()
	if meta, ok := parks[toolCallID]; ok {
		cp := meta
		return &cp
	}
	return nil
}

func (p parkStore) set(toolCallID string, meta parkedWorkerMeta, worker *AgentHarness) {
	parks := p.load()
	parks[toolCallID] = meta
	p.store(parks)

	p.h.parkMu.Lock()
	if p.h.parkedWorkersLive == nil {
		p.h.parkedWorkersLive = make(map[string]*AgentHarness)
	}
	if worker != nil {
		p.h.parkedWorkersLive[toolCallID] = worker
	}
	p.h.parkMu.Unlock()
}

func (p parkStore) clear(toolCallID string) {
	if toolCallID == "" {
		return
	}
	parks := p.load()
	if _, ok := parks[toolCallID]; ok {
		delete(parks, toolCallID)
		p.store(parks)
	}
	p.h.parkMu.Lock()
	live := p.h.parkedWorkersLive[toolCallID]
	delete(p.h.parkedWorkersLive, toolCallID)
	p.h.parkMu.Unlock()
	if live != nil {
		live.Close()
	}
	if p.h.interruptPayloads != nil {
		delete(p.h.interruptPayloads, toolCallID)
	}
}

func (p parkStore) load() map[string]parkedWorkerMeta {
	raw, ok := p.h.runtime.StateGet(parkedWorkersStateKey)
	if !ok || raw == nil {
		return map[string]parkedWorkerMeta{}
	}
	switch v := raw.(type) {
	case string:
		var m map[string]parkedWorkerMeta
		if err := json.Unmarshal([]byte(v), &m); err != nil || m == nil {
			return map[string]parkedWorkerMeta{}
		}
		return m
	case []byte:
		var m map[string]parkedWorkerMeta
		if err := json.Unmarshal(v, &m); err != nil || m == nil {
			return map[string]parkedWorkerMeta{}
		}
		return m
	case map[string]parkedWorkerMeta:
		return v
	default:
		// After JSON checkpoint round-trip, nested objects may appear as map[string]any.
		b, err := json.Marshal(raw)
		if err != nil {
			return map[string]parkedWorkerMeta{}
		}
		var m map[string]parkedWorkerMeta
		if err := json.Unmarshal(b, &m); err != nil || m == nil {
			return map[string]parkedWorkerMeta{}
		}
		return m
	}
}

func (p parkStore) store(parks map[string]parkedWorkerMeta) {
	if len(parks) == 0 {
		p.h.runtime.StateDelete(parkedWorkersStateKey)
		return
	}
	b, err := json.Marshal(parks)
	if err != nil {
		slog.Error("marshal parked workers", "area", "subagent", "error", err)
		return
	}
	// Store as string so checkpoint JSON round-trips cleanly.
	p.h.runtime.StateSet(parkedWorkersStateKey, string(b))
}

// workerDrainResult is the outcome of draining a child event stream.
type workerDrainResult struct {
	lastAssistant string
	completed     bool
	interruptIDs  []string
}

// drainWorkerEvents consumes a worker event stream, forwarding progress via
// emit. Interrupts are recorded (not treated as hard errors) so the caller
// can park and bubble them.
func drainWorkerEvents(
	ctx context.Context,
	workerName string,
	events <-chan StreamEvent,
	emit func(string),
) (workerDrainResult, error) {
	var result workerDrainResult
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return result, nil
			}
			switch ev.Type {
			case StreamEventError:
				if ev.Error != nil {
					return result, ev.Error
				}
				return result, errors.New("worker stream error with no details")
			case StreamEventComplete:
				result.completed = true
			case StreamEventInterrupt:
				id := parseInterruptID(ev.Data)
				if id != "" {
					result.interruptIDs = append(result.interruptIDs, id)
				}
				if emit != nil {
					emit(fmt.Sprintf("[%s] awaiting user input", workerName))
				}
			case StreamEventMessage:
				if ev.Content != "" {
					result.lastAssistant = ev.Content
					if emit != nil {
						emit(fmt.Sprintf("[%s] %s", workerName, ev.Content))
					}
				}
			case StreamEventReasoning:
				if ev.Content != "" && emit != nil {
					slog.Debug("worker reasoning", "area", "subagent", "worker", workerName)
					emit(fmt.Sprintf("[%s thinking] %s", workerName, ev.Content))
				}
			case StreamEventFunctionCall:
				if emit != nil {
					for _, tc := range ev.ToolCalls {
						slog.Debug("worker tool call", "area", "subagent", "worker", workerName, "tool", tc.Name)
						emit(fmt.Sprintf("[%s] tool call: %s", workerName, tc.Name))
					}
				}
			}
		}
	}
}

func parseInterruptID(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	var payload struct {
		InterruptId string `json:"interruptId"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return ""
	}
	return payload.InterruptId
}

// finalWorkerOutput prefers the last non-empty assistant/reasoning message
// in the worker context window, falling back to streamed assistant content.
func finalWorkerOutput(window []*Message, streamed string) string {
	for i := len(window) - 1; i >= 0; i-- {
		msg := window[i]
		if (msg.Role == RoleAssistant || msg.Role == RoleReasoning) && msg.Content != "" {
			return msg.Content
		}
	}
	return streamed
}
