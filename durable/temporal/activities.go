package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// liveTurns lets a same-process Runtime.Cancel stop the activity body without
// waiting for a Temporal heartbeat round-trip. Cross-process workers still
// cancel via activity context + heartbeats.
var liveTurns sync.Map // durable.SessionID -> context.CancelFunc

func bindLiveTurn(id durable.SessionID, cancel context.CancelFunc) func() {
	liveTurns.Store(id, cancel)
	return func() { liveTurns.Delete(id) }
}

func cancelLiveTurn(id durable.SessionID) {
	if v, ok := liveTurns.LoadAndDelete(id); ok {
		if cancel, ok := v.(context.CancelFunc); ok {
			cancel()
		}
	}
}

func canceledIf(ctx context.Context, err error) error {
	if err := ctx.Err(); err != nil {
		return temporal.NewCanceledError(err.Error())
	}
	return err
}

// activities are the Inference and Tool bodies registered on the worker.
type activities struct {
	Catalog        durable.Catalog
	Snapshots      durable.SnapshotStore
	Projection     vfs.Projection
	Fallback       durable.EventLog
	DisableStreams bool
	Secrets        durable.SecretStorage
}

// inferenceInput is one Inference step. Mounts and identity are the Snapshot
// row this activity will save. State is the Prompt/Resume overlay. Tokens
// come from SecretStorage, not this struct.
type inferenceInput struct {
	SessionID     durable.SessionID
	Parent        durable.SessionID
	AgentID       string
	MCPServers    []mcp.MCPConfig
	User          *tacklr.Message
	HadToolRound  bool
	ModelRequests int
	Resume        map[string][]byte
	Mounts        []durable.MountRecipe
	Specialist    string
	Children      []durable.SessionID
	State         map[string]any
}

// inferenceOutput is the typed Inference activity result.
type inferenceOutput struct {
	Complete  bool
	ToolCalls []tacklr.ToolCall
	Result    string
}

// toolInput is the typed Tool activity argument.
type toolInput struct {
	SessionID  durable.SessionID
	Parent     durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Call       tacklr.ToolCall
	Mounts     []durable.MountRecipe
	Specialist string
	// Children are this session's child ids (snapshot identity and list_children).
	Children []durable.SessionID
	State    map[string]any
}

// toolOutput is the typed Tool activity result. Spawn/cancel/await are this
// call's Runtime intent; the workflow starts, cancels, or waits via ExecuteChildWorkflow.
type toolOutput struct {
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
	SpawnID       durable.SessionID
	SpawnSpec     string
	SpawnTask     string
	CancelID      durable.SessionID
	AwaitID       durable.SessionID
}

// commitToolInput records a tool output on the staged batch without executing
// the tool. SessionWorkflow uses this after spawn_specialist child completion.
type commitToolInput struct {
	SessionID  durable.SessionID
	Parent     durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Call       tacklr.ToolCall
	Output     string
	Mounts     []durable.MountRecipe
	Specialist string
	Children   []durable.SessionID
	State      map[string]any
}

func (a *activities) Inference(ctx context.Context, in inferenceInput) (inferenceOutput, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer bindLiveTurn(in.SessionID, cancel)()
	defer startHeartbeat(ctx)()
	ctx = telemetry.BindTurnContext(ctx, in.AgentID, string(in.SessionID))
	attempt := int32(1)
	if activity.IsActivity(ctx) {
		attempt = activity.GetInfo(ctx).Attempt
	}
	if attempt > 1 {
		slog.WarnContext(ctx, "inference retry",
			"area", telemetry.AreaRuntime, "session_id", in.SessionID,
			"agent_id", in.AgentID, "attempt", attempt)
	} else {
		slog.InfoContext(ctx, "inference started",
			"area", telemetry.AreaRuntime, "session_id", in.SessionID,
			"agent_id", in.AgentID, "had_tools", in.HadToolRound)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	h, ms, skillsMS, rev, err := a.harness(ctx, in.SessionID, in.Parent, in.AgentID, in.MCPServers, in.Mounts, in.Specialist, in.State)
	if err != nil {
		pub := err
		if err := ctx.Err(); err != nil {
			pub = err
		}
		slog.ErrorContext(ctx, "inference harness", "area", telemetry.AreaRuntime, "error", pub)
		return inferenceOutput{}, canceledIf(ctx, err)
	}
	defer func() {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(in.SessionID), "inference")
	}()
	eng := h.Drive()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	if len(in.Resume) > 0 {
		if err := eng.ApplyResume(in.Resume); err != nil {
			return inferenceOutput{}, canceledIf(ctx, err)
		}
		if pending := eng.PendingToolCalls(); len(pending) > 0 {
			_, err = a.save(ctx, in.SessionID, h, rev, durable.Snapshot{
				AgentID: in.AgentID, Specialist: in.Specialist, Parent: in.Parent,
				Children: in.Children, Mounts: in.Mounts,
			})
			return inferenceOutput{ToolCalls: pending}, err
		}
	}
	if in.User != nil {
		if err := eng.AbsorbUser(ctx, in.User, out); err != nil {
			slog.ErrorContext(ctx, "inference absorb", "area", telemetry.AreaRuntime, "error", err)
			return inferenceOutput{}, canceledIf(ctx, err)
		}
	}
	st := &tacklr.TurnState{HadToolRound: in.HadToolRound, ModelRequests: in.ModelRequests}
	step, err := eng.RunInference(ctx, st, out)
	if err != nil {
		if ctx.Err() != nil {
			slog.WarnContext(ctx, "inference cancelled", "area", telemetry.AreaRuntime)
		} else {
			slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
		}
		return inferenceOutput{}, canceledIf(ctx, err)
	}
	if _, err = a.save(ctx, in.SessionID, h, rev, durable.Snapshot{
		AgentID: in.AgentID, Specialist: in.Specialist, Parent: in.Parent,
		Children: in.Children, Mounts: in.Mounts,
	}); err != nil {
		slog.ErrorContext(ctx, "inference persist", "area", telemetry.AreaRuntime, "error", err)
		return inferenceOutput{}, err
	}
	result := ""
	if step.Complete {
		for _, m := range h.Drive().Messages() {
			if m != nil && m.Role == tacklr.RoleAssistant && m.Content != "" {
				result = m.Content
			}
		}
	}
	slog.InfoContext(ctx, "inference completed",
		"area", telemetry.AreaRuntime, "complete", step.Complete, "tool_calls", len(step.ToolCalls))
	return inferenceOutput{Complete: step.Complete, ToolCalls: step.ToolCalls, Result: result}, nil
}

func (a *activities) Tool(ctx context.Context, in toolInput) (toolOutput, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer bindLiveTurn(in.SessionID, cancel)()
	defer startHeartbeat(ctx)()
	ctx = telemetry.BindTurnContext(ctx, in.AgentID, string(in.SessionID))
	attempt := int32(1)
	if activity.IsActivity(ctx) {
		attempt = activity.GetInfo(ctx).Attempt
	}
	if attempt > 1 {
		slog.WarnContext(ctx, "tool retry",
			"area", telemetry.AreaHarness, "session_id", in.SessionID,
			"agent_id", in.AgentID, "tool", in.Call.Name, "attempt", attempt)
	} else {
		slog.InfoContext(ctx, "tool started",
			"area", telemetry.AreaHarness, "session_id", in.SessionID,
			"agent_id", in.AgentID, "tool", in.Call.Name, "namespace", in.Call.Namespace)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	h, ms, skillsMS, rev, err := a.harness(ctx, in.SessionID, in.Parent, in.AgentID, in.MCPServers, in.Mounts, in.Specialist, in.State)
	if err != nil {
		slog.ErrorContext(ctx, "tool harness", "area", telemetry.AreaHarness, "error", err)
		return toolOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(in.SessionID), "tool")
	}()
	kids := &activityChildren{
		parent:  in.SessionID,
		agentID: in.AgentID,
		catalog: a.Catalog,
		known:   append([]durable.SessionID(nil), in.Children...),
	}
	h.BindChildHost(kids)
	eng := h.Drive()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	step, runErr := eng.RunToolCall(ctx, in.Call, out)
	if runErr != nil {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, "tool cancelled", "area", telemetry.AreaHarness, "tool", in.Call.Name)
			return toolOutput{}, canceledIf(ctx, runErr)
		}
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", in.Call.Name, "error", runErr)
	}
	_, saveErr := a.save(ctx, in.SessionID, h, rev, durable.Snapshot{
		AgentID: in.AgentID, Specialist: in.Specialist, Parent: in.Parent,
		Children: in.Children, Mounts: in.Mounts,
	})
	if saveErr != nil {
		slog.ErrorContext(ctx, "tool persist", "area", telemetry.AreaHarness, "error", saveErr)
		if runErr != nil {
			return toolOutput{}, fmt.Errorf("tool: %w: persist: %w", runErr, saveErr)
		}
		return toolOutput{}, saveErr
	}
	status := "success"
	if step.Interrupted {
		status = "interrupt"
	}
	slog.InfoContext(ctx, "tool completed",
		"area", telemetry.AreaHarness, "tool", in.Call.Name, "status", status)
	return toolOutput{
		Interrupted:   step.Interrupted,
		InterruptID:   step.InterruptID,
		InterruptData: step.InterruptData,
		SpawnID:       kids.spawnID,
		SpawnSpec:     kids.spawnSpec,
		SpawnTask:     kids.spawnTask,
		CancelID:      kids.cancelID,
		AwaitID:       kids.awaitID,
	}, nil
}

func (a *activities) CommitToolOutput(ctx context.Context, in commitToolInput) (toolOutput, error) {
	h, ms, skillsMS, rev, err := a.harness(ctx, in.SessionID, in.Parent, in.AgentID, in.MCPServers, in.Mounts, in.Specialist, in.State)
	if err != nil {
		return toolOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(in.SessionID), "commit_tool")
	}()
	h.Drive().RecordToolResult(in.Call, in.Output)
	if _, err = a.save(ctx, in.SessionID, h, rev, durable.Snapshot{
		AgentID: in.AgentID, Specialist: in.Specialist, Parent: in.Parent,
		Children: in.Children, Mounts: in.Mounts,
	}); err != nil {
		return toolOutput{}, err
	}
	presented := in.Call
	presented.Status = "success"
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, tacklr.StreamEvent{
		Type:      tacklr.StreamEventToolResult,
		MessageID: in.Call.Key(),
		Content:   in.Output,
		ToolCalls: []tacklr.ToolCall{presented},
	}, true)
	return toolOutput{}, nil
}

func (a *activities) harness(ctx context.Context, id, parent durable.SessionID, agentID string, extraMCP []mcp.MCPConfig, mounts []durable.MountRecipe, specialist string, state map[string]any) (*tacklr.TurnManager, *vfs.MountSession, *vfs.MountSession, durable.Revision, error) {
	sec, err := a.Secrets.Get(ctx, id)
	if err != nil {
		return nil, nil, nil, "", err
	}
	hasTok := false
	for _, b := range sec.Auth.Bindings {
		if strings.TrimSpace(b.Auth.Token) != "" {
			hasTok = true
			break
		}
	}
	if !hasTok && parent != "" {
		sec, err = a.Secrets.Get(ctx, parent)
		if err != nil {
			return nil, nil, nil, "", err
		}
	}
	spec, ok := a.Catalog.Lookup(agentID)
	if !ok {
		return nil, nil, nil, "", durable.ErrAgentNotFound
	}
	if specialist != "" {
		over, err := durable.OverlaySpecialist(spec, specialist)
		if err != nil {
			return nil, nil, nil, "", err
		}
		spec = over
	}
	proj := a.Projection
	if proj == nil {
		proj = vfs.DirectProjection{}
	}
	ms, skillsMS, err := durable.OpenTurnSessions(ctx, string(id), spec, durable.BindingsForTurn(mounts, sec.Auth), proj)
	if err != nil {
		return nil, nil, nil, "", err
	}
	opts := spec.Options
	opts.SessionID = string(id)
	opts.MountSession = ms
	opts.SkillsSession = skillsMS
	opts.SkillsRoot = spec.SkillsRoot
	if len(extraMCP) > 0 {
		mcpConfigs := make([]mcp.MCPConfig, 0, len(opts.MCPConfigs)+len(extraMCP))
		mcpConfigs = append(mcpConfigs, opts.MCPConfigs...)
		mcpConfigs = append(mcpConfigs, extraMCP...)
		opts.MCPConfigs = mcpConfigs
	}
	h, err := tacklr.NewTurnManager(ctx, opts)
	if err != nil {
		durable.CloseTurnTrees(ms, skillsMS, string(id), "construct")
		return nil, nil, nil, "", err
	}
	var rev durable.Revision
	snap, loaded, loadErr := a.Snapshots.Load(ctx, id)
	switch {
	case loadErr == nil:
		if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
			h.Close()
			durable.CloseTurnTrees(ms, skillsMS, string(id), "restore")
			return nil, nil, nil, "", err
		}
		rev = loaded
	case errors.Is(loadErr, durable.ErrSessionNotFound):
	default:
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(id), "load")
		return nil, nil, nil, "", loadErr
	}
	if err := h.ApplySessionState(state); err != nil {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(id), "state")
		return nil, nil, nil, "", err
	}
	return h, ms, skillsMS, rev, nil
}

func (a *activities) save(ctx context.Context, id durable.SessionID, h *tacklr.TurnManager, expected durable.Revision, rec durable.Snapshot) (durable.Revision, error) {
	cp, err := h.Checkpoint()
	if err != nil {
		telemetry.RecordCheckpointAttempt(ctx, err)
		return "", err
	}
	rec.Checkpoint = *cp
	rev, err := a.Snapshots.Save(ctx, id, rec, expected)
	telemetry.RecordCheckpointAttempt(ctx, err)
	return rev, err
}

const streamBatchInterval = 200 * time.Millisecond

func (a *activities) openStream(ctx context.Context) *workflowstreams.Client {
	if a.DisableStreams || !activity.IsActivity(ctx) {
		return nil
	}
	c, err := workflowstreams.NewClientFromActivity(ctx, workflowstreams.Options{
		BatchInterval: streamBatchInterval,
	})
	if err != nil {
		return nil
	}
	return c
}

func closeStream(ctx context.Context, c *workflowstreams.Client) {
	if c != nil {
		_ = c.Close(publishContext(ctx))
	}
}

func (a *activities) emitter(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID) func(tacklr.StreamEvent) {
	first := true
	return func(ev tacklr.StreamEvent) {
		switch ev.Type {
		case tacklr.StreamEventComplete, tacklr.StreamEventInterrupt, tacklr.StreamEventError:
			// Turn-finished signals. SessionWorkflow commits Status, then EmitEvent.
			return
		}
		if ctx.Err() != nil {
			return
		}
		force := first
		first = false
		_ = a.publish(ctx, stream, sessionID, durable.TopicEvents, ev, force)
	}
}

// emitEventInput is the typed EmitEvent activity argument.
type emitEventInput struct {
	SessionID durable.SessionID
	Event     tacklr.StreamEvent
}

// EmitEvent publishes a turn-finished stream event after SessionWorkflow has
// committed Status (complete, failed, or yield).
func (a *activities) EmitEvent(ctx context.Context, in emitEventInput) error {
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	return a.publish(ctx, stream, in.SessionID, durable.TopicEvents, in.Event, true)
}

func startHeartbeat(ctx context.Context) func() {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		activity.RecordHeartbeat(ctx, "tick")
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, "tick")
			}
		}
	}()
	return func() {
		close(done)
		// Wait so a retry cannot RecordHeartbeat on the next attempt's handle.
		wg.Wait()
	}
}

func publishContext(ctx context.Context) context.Context {
	if ctx == nil || ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

func (a *activities) publish(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID, topic string, ev tacklr.StreamEvent, force bool) error {
	if ev.Error != nil && ev.Fail == "" {
		ev.Fail = ev.Error.Error()
	}
	pubCtx := publishContext(ctx)
	var streamErr error
	if stream != nil {
		stream.Topic(topic).Publish(ev, force)
		if force {
			streamErr = stream.Flush(pubCtx)
		}
	}
	if a.Fallback != nil {
		if err := a.Fallback.Append(pubCtx, sessionID, topic, ev); err != nil {
			return err
		}
	}
	return streamErr
}
