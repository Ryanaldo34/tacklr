package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Worker failures wrap the package categories (ErrNotFound, ErrInvalid, ErrFailed).

// Specialist describes a specialized worker that a harness can spawn via the
// spawn_specialist tool. Specs may nest via Specialists so interrupt propagation
// and orchestration stay self-similar at any depth.
//
// Workers inherit the parent session world via workerOptsFromSpec (VFS including
// /skills, brain, index bridge, MCP, web, policy, watchdog, interceptors).
// Spec fields replace model, instructions, tools, and nested Specialists.
// They skip planningWriteLock. They do not get a second FUSE.
type Specialist struct {
	Tools        []*Tool
	Instructions string
	Model        InferenceStrategy
	Name         string
	Description  string
	// Specialists are nested workers available to this worker when it runs.
	Specialists []*Specialist
}

// parkedWorkerMeta is durable park metadata. Live harness pointers live in parkedWorkersLive.
type parkedWorkerMeta = session.ParkedWorkerMeta

func workerParkMeta(worker *AgentHarness, name, task string, childIDs []string) (parkedWorkerMeta, error) {
	meta := parkedWorkerMeta{
		Specialist:        name,
		Task:              task,
		ChildInterruptIDs: childIDs,
	}
	if worker == nil {
		return meta, nil
	}
	meta.WorkerSessionID = worker.sessionId
	cp, err := worker.Checkpoint()
	if err != nil {
		return parkedWorkerMeta{}, err
	}
	meta.Checkpoint = cp
	return meta, nil
}

// initSpecialists registers worker specs. Invalid or duplicate specs are
// constructor errors (panic): a misconfigured host must not start a harness
// that silently drops workers.
func (h *AgentHarness) initSpecialists(specs []*Specialist) error {
	for _, spec := range specs {
		if spec == nil {
			return fmt.Errorf("tacklr: Specialist must not be nil")
		}
		if spec.Name == "" {
			return fmt.Errorf("tacklr: Specialist.Name is required")
		}
		if spec.Model == nil {
			return fmt.Errorf("tacklr: Specialist.Model is required")
		}
		if _, exists := h.specialists[spec.Name]; exists {
			return fmt.Errorf("tacklr: duplicate Specialist name %s", spec.Name)
		}
		cp := *spec
		h.specialists[spec.Name] = &cp
	}
	return nil
}

// workerNames returns registered worker names in sorted order.
func (a *AgentHarness) workerNames() []string {
	if len(a.specialists) == 0 {
		return nil
	}
	names := make([]string, 0, len(a.specialists))
	for name := range a.specialists {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// formatSpecialistPromptList builds the deterministic AVAILABLE SPECIALISTS list.
func (a *AgentHarness) formatSpecialistPromptList() string {
	names := a.workerNames()
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	for _, name := range names {
		spec := a.specialists[name]
		if spec.Description != "" {
			fmt.Fprintf(&b, " - %s: %s\n", name, spec.Description)
		} else {
			fmt.Fprintf(&b, " - %s\n", name)
		}
	}
	return b.String()
}

type spawnSpecialistArgs struct {
	TaskDescriptionAndContext string `json:"task_description_and_context" desc:"Clear task goal, acceptance criteria, and helpful context for the worker"`
	Specialist                string `json:"specialist" desc:"Name of a registered specialist to spawn"`
	Block                     *bool  `json:"block" desc:"Wait for the worker and return its result. Defaults to true. Set false to schedule a background job and continue the turn."`
}

func (a *AgentHarness) spawnTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "spawn_specialist",
		DisplayName: "Spawn {specialist}",
		Description: "Spawn a specialist. block defaults to true and returns the worker result before continuing. Set block=false to schedule a background job, continue other work, then use list_children, get_child, or cancel_child.",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, args spawnSpecialistArgs, runtime HarnessRuntime) (string, error) {
			block := args.Block == nil || *args.Block
			return a.runWorker(ctx, args.Specialist, args.TaskDescriptionAndContext, block, runtime)
		},
	})
}

// runWorker creates or resumes an isolated worker harness, drains events into
// parent tool updates, and either returns final output or bubbles a child
// interrupt onto the parent session (self-similar at any nesting depth).
// When block is false (and this is not a parked synchronous spawn), the worker
// is scheduled as a job and this call returns immediately.
func (a *AgentHarness) runWorker(ctx context.Context, specialist, task string, block bool, runtime HarnessRuntime) (string, error) {
	spec, ok := a.specialists[specialist]
	if !ok {
		return "", fmt.Errorf("worker %q is not registered. Pass a specialist from the available specialists: %w", specialist, ErrNotFound)
	}
	if strings.TrimSpace(task) == "" && a.getParkMeta(runtime.CurrentToolCallID()) == nil {
		return "", fmt.Errorf("worker %q: empty task. Pass task_description_and_context with the goal and constraints: %w", specialist, ErrInvalid)
	}

	toolCallID := runtime.CurrentToolCallID()
	logAttrs := []any{
		"area", "subagent",
		"session_id", a.sessionId,
		"worker", specialist,
		"tool_call_id", toolCallID,
		"task_len", len(task),
	}

	start := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Drop any residual resolved interrupt for this spawn call — resume is
	// driven by park metadata + stashed resolution payloads, not RaiseInterrupt.
	_, _ = a.session.TakeResolvedInterrupt(toolCallID)

	meta := a.getParkMeta(toolCallID)
	if !block && meta == nil {
		return a.scheduleBackgroundWorker(specialist, task, toolCallID, runtime)
	}

	var worker *AgentHarness
	var closeOnExit bool
	resuming := meta != nil

	if resuming {
		slog.Info("resuming parked worker", logAttrs...)
		if meta.Task != "" {
			task = meta.Task
		}
		var err error
		worker, err = a.attachParkedWorker(ctx, toolCallID, *meta, spec)
		if err != nil {
			return "", fmt.Errorf("worker %q: %w", specialist, err)
		}
		// Live-attached workers stay in the cache until success; loaded ones
		// should be closed when this invocation finishes (success or re-park).
		a.parkMu.Lock()
		_, live := a.parkedWorkersLive[toolCallID]
		a.parkMu.Unlock()
		closeOnExit = !live
	} else {
		slog.Info("spawning worker", logAttrs...)
		var err error
		worker, err = a.newWorkerHarness(workerCtx, specialist, toolCallID, spec)
		if err != nil {
			return "", fmt.Errorf("worker %q: %w", specialist, err)
		}
		closeOnExit = true
	}
	run := &workerRun{
		id:         toolCallID,
		specialist: specialist,
		task:       task,
		status:     jobStatusRunning,
		worker:     worker,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	a.registerJob(run)
	defer a.removeJob(toolCallID)
	failRun := func(err error) error {
		run.setTerminal(jobStatusFailed, "", err)
		return err
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
			return "", failRun(fmt.Errorf("%w: resuming worker %q: %w", ErrFailed, specialist, err))
		}
		runtime.EmitUpdate(fmt.Sprintf("Specialist %q resumed", specialist))
	} else {
		events, err = worker.Run(workerCtx, task)
		if err != nil {
			slog.Error("failed to start worker", append(logAttrs, "error", err, "elapsed", time.Since(start).Round(time.Millisecond))...)
			return "", failRun(fmt.Errorf("%w: starting worker %q: %w", ErrFailed, specialist, err))
		}
		runtime.EmitUpdate(fmt.Sprintf("Specialist %q started", specialist))
	}

	drained, drainErr := drainWorkerEvents(ctx, specialist, events, runtime.EmitUpdate)
	elapsed := time.Since(start).Round(time.Millisecond)

	if ctx.Err() != nil {
		a.clearPark(toolCallID)
		slog.Info("worker cancelled", append(logAttrs, "elapsed", elapsed, "error", ctx.Err())...)
		return "", failRun(ctx.Err())
	}
	if drainErr != nil {
		a.clearPark(toolCallID)
		slog.Warn("worker failed", append(logAttrs, "elapsed", elapsed, "error", drainErr)...)
		return "", failRun(fmt.Errorf("%w: worker %q: %w", ErrFailed, specialist, drainErr))
	}
	if drained.completed {
		a.clearPark(toolCallID)
		result := finalWorkerOutput(worker.Messages(), drained.lastAssistant)
		if result == "" {
			slog.Warn("worker produced no output", append(logAttrs, "elapsed", elapsed)...)
			return "", failRun(fmt.Errorf("worker %q: no output: %w", specialist, ErrFailed))
		}
		run.setTerminal(jobStatusCompleted, result, nil)
		slog.Info("worker completed", append(logAttrs, "elapsed", elapsed, "output_length", len(result))...)
		return result, nil
	}

	// Incomplete: bubble child interrupts, or fail.
	childIntrIDs, childIntr := collectChildInterrupts(worker, drained.interruptIDs)
	if childIntr == nil {
		a.clearPark(toolCallID)
		slog.Warn("worker incomplete", append(logAttrs, "elapsed", elapsed)...)
		return "", failRun(fmt.Errorf("worker %q: incomplete: %w", specialist, ErrFailed))
	}

	parkMeta, err := workerParkMeta(worker, specialist, task, childIntrIDs)
	if err != nil {
		slog.Error("failed to checkpoint worker", append(logAttrs, "error", err)...)
		return "", failRun(fmt.Errorf("checkpoint interrupted worker %q: %w", specialist, err))
	}
	a.setPark(toolCallID, parkMeta, worker)
	parked = true
	closeOnExit = false

	slog.Info("bubbling worker interrupt", append(logAttrs,
		"elapsed", elapsed,
		"child_interrupts", len(childIntrIDs),
		"worker_session_id", worker.sessionId,
	)...)

	// Adopt the same interrupt object onto the parent session under this
	// spawn_specialist tool call id, then return it as an error so the parent
	// harness parks spawn_specialist like any other tool interrupt.
	_, err = a.session.AdoptInterrupt(toolCallID, childIntr)
	if err != nil {
		run.setInterrupted(childIntr, childIntrIDs)
	}
	return "", err
}

func (a *AgentHarness) newWorkerHarness(ctx context.Context, specialist, parentToolCallID string, spec *Specialist) (*AgentHarness, error) {
	opts := a.workerOptsFromSpec(spec)
	if id, ok := a.session.Search.Namespace(); ok {
		cp := id
		opts.SearchNamespace = &cp
	}
	sessionID := workerSessionID(a.sessionId, specialist, parentToolCallID)
	worker, err := NewAgent(ctx, opts)
	if err != nil {
		return nil, err
	}
	worker.sessionId = sessionID
	return worker, nil
}

func workerSessionID(parentSessionID, specialist, parentToolCallID string) string {
	if parentSessionID == "" {
		return fmt.Sprintf("w/%s/%s", specialist, parentToolCallID)
	}
	return fmt.Sprintf("%s/w/%s/%s", parentSessionID, specialist, parentToolCallID)
}

// FindSpecialist returns the named worker from specs, including nested Specialists.
func FindSpecialist(specs []*Specialist, name string) *Specialist {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		if spec.Name == name {
			return spec
		}
		if found := FindSpecialist(spec.Specialists, name); found != nil {
			return found
		}
	}
	return nil
}

// WithSpecialist overlays a worker spec onto the parent session world. The child
// keeps parent MCP, brain, interceptors, and skills. Model, tools, nested
// workers, and instructions come from spec. Planning write lock is off.
// MountSession and SessionID stay as the caller set them (Runtime injects
// a child tree; embed still passes the parent pointer).
func (o AgentOptions) WithSpecialist(spec *Specialist) AgentOptions {
	if spec == nil {
		return o
	}
	out := o
	out.Config.SystemPrompt = spec.Instructions
	if spec.Model != nil {
		out.Model = spec.Model
	}
	out.Tools = slices.Clone(spec.Tools)
	out.Specialists = spec.Specialists
	out.disablePlanningLock = true
	return out
}

// workerOptsFromSpec is the parent session world with worker spec fields overlaid.
// Omits SearchNamespace so resume keeps the checkpointed worker session value.
func (a *AgentHarness) workerOptsFromSpec(spec *Specialist) AgentOptions {
	return AgentOptions{
		Config: Config{
			MaxWindowSize:   a.maxWindowSize,
			MaxTurnRequests: a.maxTurnRequests,
		},
		WatchDog:              a.watchDog,
		MCPConfigs:            slices.Clone(a.mcpConfigs),
		MCPCredentialResolver: a.mcpCredentialResolver,
		ContextPolicy:         a.contextPolicy,
		ToolInterceptors:      slices.Clone(a.hostInterceptors),
		ToolResultHooks:       maps.Clone(a.hostResultHooks),
		SkillsLoader:          a.skillsLoader,
		ExaAPIKey:             a.exaAPIKey,
		Brain:                 a.brain,
		BrainWriteKinds:       a.brainWriteKinds,
		MountSession:          a.session.VFS,
		runCommandUnattended:  a.runCommandUnattended,
		writeUnattended:       a.writeUnattended,
		shareIndexBridge:      a.vfsBridge,
	}.WithSpecialist(spec)
}

func (a *AgentHarness) attachParkedWorker(ctx context.Context, toolCallID string, meta parkedWorkerMeta, spec *Specialist) (*AgentHarness, error) {
	a.parkMu.Lock()
	if live, ok := a.parkedWorkersLive[toolCallID]; ok && live != nil {
		a.parkMu.Unlock()
		return live, nil
	}
	a.parkMu.Unlock()

	if meta.Checkpoint == nil {
		return nil, fmt.Errorf("parked worker state is missing: %w", ErrNotFound)
	}
	opts := a.workerOptsFromSpec(spec)
	opts.SessionID = meta.WorkerSessionID
	worker, err := NewAgent(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("parked worker state is missing: %w: %w", ErrNotFound, err)
	}
	if err := worker.RestoreCheckpoint(*meta.Checkpoint); err != nil {
		worker.Close()
		return nil, fmt.Errorf("parked worker state is missing: restore %q: %w: %w", meta.WorkerSessionID, ErrNotFound, err)
	}
	a.parkMu.Lock()
	a.parkedWorkersLive[toolCallID] = worker
	a.parkMu.Unlock()
	return worker, nil
}

func (a *AgentHarness) childResolutionPayloads(parentToolCallID string, meta *parkedWorkerMeta) map[string][]byte {
	// One parent spawn_specialist interrupt maps to one consumer resolution.
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
		if intr, ok := worker.session.PendingInterrupt(id); ok {
			return ids, intr
		}
	}
	return ids, nil
}

func (a *AgentHarness) getParkMeta(toolCallID string) *parkedWorkerMeta {
	if toolCallID == "" {
		return nil
	}
	meta, ok := a.session.ParkedWorker(toolCallID)
	if !ok {
		return nil
	}
	cp := meta
	return &cp
}

func (a *AgentHarness) setPark(toolCallID string, meta parkedWorkerMeta, worker *AgentHarness) {
	a.session.SetParkedWorker(toolCallID, meta)
	a.parkMu.Lock()
	if worker != nil {
		a.parkedWorkersLive[toolCallID] = worker
	}
	a.parkMu.Unlock()
}

func (a *AgentHarness) clearPark(toolCallID string) {
	if toolCallID == "" {
		return
	}
	a.session.DeleteParkedWorker(toolCallID)
	a.parkMu.Lock()
	live := a.parkedWorkersLive[toolCallID]
	delete(a.parkedWorkersLive, toolCallID)
	a.parkMu.Unlock()
	if live != nil {
		live.Close()
	}
	if a.interruptPayloads != nil {
		delete(a.interruptPayloads, toolCallID)
	}
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
	specialist string,
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
					emit(fmt.Sprintf("[%s] awaiting user input", specialist))
				}
			case StreamEventMessage:
				if ev.Content != "" {
					result.lastAssistant = ev.Content
					if emit != nil {
						emit(fmt.Sprintf("[%s] %s", specialist, ev.Content))
					}
				}
			case StreamEventReasoning:
				if ev.Content != "" && emit != nil {
					slog.Debug("specialist reasoning", "area", "specialist", "specialist", specialist)
					emit(fmt.Sprintf("[%s thinking] %s", specialist, ev.Content))
				}
			case StreamEventFunctionCall:
				if emit != nil {
					for _, tc := range ev.ToolCalls {
						slog.Debug("worker tool call", "area", "subagent", "worker", specialist, "tool", tc.Name)
						emit(fmt.Sprintf("[%s] tool call: %s", specialist, tc.Name))
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
