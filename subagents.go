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

// Worker failures wrap the package categories (ErrNotFound, ErrInvalid, ErrFailed).

const parkedWorkersStateKey = "_parked_workers"

// SubAgent describes a specialized worker that a harness can spawn via the
// spawn_worker tool. Specs may nest via SubAgents so interrupt propagation
// and orchestration stay self-similar at any depth.
//
// Workers inherit the host MountSession, brain engine, and shared index
// bridge. They skip planningWriteLock. They do not get a second FUSE.
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

// initSubAgentWorkers registers worker specs. Invalid or duplicate specs are
// constructor errors (panic): a misconfigured host must not start a harness
// that silently drops workers.
func (h *AgentHarness) initSubAgentWorkers(specs []*SubAgent) {
	for _, spec := range specs {
		if spec == nil {
			panic("tacklr: SubAgent must not be nil")
		}
		if spec.WorkerName == "" {
			panic("tacklr: SubAgent.WorkerName is required")
		}
		if spec.Model == nil {
			panic("tacklr: SubAgent.Model is required")
		}
		if _, exists := h.subagents[spec.WorkerName]; exists {
			panic("tacklr: duplicate SubAgent worker name " + spec.WorkerName)
		}
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
		DisplayName: "Spawn {worker_name}",
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
		return "", fmt.Errorf("worker %q: %w", workerName, ErrNotFound)
	}
	if strings.TrimSpace(task) == "" && a.getParkMeta(runtime.CurrentToolCallID) == nil {
		return "", fmt.Errorf("worker %q: empty task: %w", workerName, ErrInvalid)
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
		events, err = worker.ReturnFromInterrupt(workerCtx, a.childResolutionPayloads(toolCallID, meta))
		if err != nil {
			a.clearPark(toolCallID)
			return "", fmt.Errorf("resuming worker %q: %w: %w", workerName, ErrFailed, err)
		}
		runtime.EmitUpdate(fmt.Sprintf("Worker %q resumed", workerName))
	} else {
		events, err = worker.Run(workerCtx, task)
		if err != nil {
			slog.Error("failed to start worker", append(logAttrs, "error", err, "elapsed", time.Since(start).Round(time.Millisecond))...)
			return "", fmt.Errorf("starting worker %q: %w: %w", workerName, ErrFailed, err)
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
		return "", fmt.Errorf("worker %q failed: %w: %w", workerName, ErrFailed, drainErr)
	}
	if drained.completed {
		a.clearPark(toolCallID)
		result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
		if result == "" {
			slog.Warn("worker produced no output", append(logAttrs, "elapsed", elapsed)...)
			return "", fmt.Errorf("worker %q: no output: %w", workerName, ErrFailed)
		}
		slog.Info("worker completed", append(logAttrs, "elapsed", elapsed, "output_length", len(result))...)
		return result, nil
	}

	// Incomplete: bubble child interrupts, or fail.
	childIntrIDs, childIntr := collectChildInterrupts(worker, drained.interruptIDs)
	if childIntr == nil {
		a.clearPark(toolCallID)
		slog.Warn("worker incomplete", append(logAttrs, "elapsed", elapsed)...)
		return "", fmt.Errorf("worker %q: incomplete: %w", workerName, ErrFailed)
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
	_, err = runtime.AdoptInterrupt(childIntr)
	return "", err
}

func (a *AgentHarness) newWorkerHarness(ctx context.Context, workerName, parentToolCallID string, spec *SubAgent) *AgentHarness {
	sessionID := workerSessionID(a.sessionId, workerName, parentToolCallID)
	worker := NewAgent(ctx, a.workerOptsForSpawn(spec))
	worker.sessionId = sessionID
	return worker
}

func workerSessionID(parentSessionID, workerName, parentToolCallID string) string {
	if parentSessionID == "" {
		return fmt.Sprintf("w/%s/%s", workerName, parentToolCallID)
	}
	return fmt.Sprintf("%s/w/%s/%s", parentSessionID, workerName, parentToolCallID)
}

// workerOptsFromSpec builds shared worker options: host mount, brain,
// shared index bridge, and skipPlanningLock. Omits SearchNamespace so
// resume keeps the checkpointed worker session value.
func (a *AgentHarness) workerOptsFromSpec(spec *SubAgent) AgentOptions {
	return AgentOptions{
		Config: Config{
			MaxWindowSize: a.maxWindowSize,
			SystemPrompt:  spec.Instructions,
		},
		Model:                spec.Model,
		Tools:                slices.Clone(spec.Tools),
		MCPConfigs:           slices.Clone(a.mcpConfigs),
		Store:                a.store,
		SubAgents:            spec.SubAgents,
		ExaAPIKey:            a.exaAPIKey,
		Brain:                a.brain,
		BrainWriteKinds:      a.brainWriteKinds,
		MountSession:         a.VFS(),
		RunCommandUnattended: a.runCommandUnattended,
		shareIndexBridge:     a.vfsBridge,
		skipPlanningLock:     true,
	}
}

func (a *AgentHarness) workerOptsForSpawn(spec *SubAgent) AgentOptions {
	opts := a.workerOptsFromSpec(spec)
	if id, ok := a.SearchNamespace(); ok {
		cp := id
		opts.SearchNamespace = &cp
	}
	return opts
}

func (a *AgentHarness) attachParkedWorker(ctx context.Context, toolCallID string, meta parkedWorkerMeta, spec *SubAgent) (*AgentHarness, error) {
	a.parkMu.Lock()
	if live, ok := a.parkedWorkersLive[toolCallID]; ok && live != nil {
		a.parkMu.Unlock()
		return live, nil
	}
	a.parkMu.Unlock()

	if a.store == nil || meta.WorkerSessionID == "" {
		return nil, fmt.Errorf("parked worker state is missing: %w", ErrNotFound)
	}
	worker, err := NewAgentFromSession(ctx, meta.WorkerSessionID, a.workerOptsFromSpec(spec))
	if err != nil {
		return nil, fmt.Errorf("parked worker state is missing: load session %q: %w: %w", meta.WorkerSessionID, ErrNotFound, err)
	}
	a.parkMu.Lock()
	a.parkedWorkersLive[toolCallID] = worker
	a.parkMu.Unlock()
	return worker, nil
}

func (a *AgentHarness) childResolutionPayloads(parentToolCallID string, meta *parkedWorkerMeta) map[string][]byte {
	// One parent spawn_worker interrupt maps to one consumer resolution.
	// Forward the same payload bytes to every pending child interrupt id
	// recorded at park time (typically one).
	payload := a.interruptPayloads[parentToolCallID]
	out := make(map[string][]byte, len(meta.ChildInterruptIDs))
	for _, id := range meta.ChildInterruptIDs {
		out[id] = payload
	}
	return out
}

// collectChildInterrupts resolves the primary interrupt from ids observed on
// the worker event stream. Interrupts always arrive as stream events; if none
// map to a pending interrupt the caller treats the worker as incomplete.
func collectChildInterrupts(worker *AgentHarness, drainedIDs []string) (ids []string, primary interrupt.Interrupt) {
	ids = drainedIDs
	for _, id := range ids {
		tcID, ok := worker.interruptToRequester[id]
		if !ok {
			continue
		}
		if intr, ok := worker.session.PendingInterrupt(tcID); ok {
			return ids, intr
		}
	}
	return ids, nil
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
	raw, ok := p.h.session.StateGet(parkedWorkersStateKey)
	if !ok || raw == nil {
		return map[string]parkedWorkerMeta{}
	}
	m, ok := raw.(map[string]parkedWorkerMeta)
	if !ok || m == nil {
		return map[string]parkedWorkerMeta{}
	}
	out := make(map[string]parkedWorkerMeta, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (p parkStore) store(parks map[string]parkedWorkerMeta) {
	if len(parks) == 0 {
		p.h.session.StateDelete(parkedWorkersStateKey)
		return
	}
	p.h.session.StateSet(parkedWorkersStateKey, parks)
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
				if ev.Content != "" {
					return result, errors.New(ev.Content)
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
