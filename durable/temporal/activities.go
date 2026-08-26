package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
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

// Activities are the Inference and Tool bodies registered on the worker.
type Activities struct {
	Catalog        durable.Catalog
	Snapshots      durable.SnapshotStore
	Projection     vfs.Projection
	Fallback       durable.EventLog
	DisableStreams bool
}

// InferenceInput is the typed Inference activity argument.
type InferenceInput struct {
	SessionID     durable.SessionID
	AgentID       string
	MCPServers    []mcp.MCPConfig
	Etag          string
	User          *streaming.Message
	HadToolRound  bool
	ModelRequests int
	Resume        map[string][]byte
	Auth          durable.AuthContext
	Mounts        []durable.MountRecipe
	Specialist    string
}

// InferenceOutput is the typed Inference activity result.
type InferenceOutput struct {
	Etag      string
	Complete  bool
	ToolCalls []streaming.ToolCall
	Result    string
}

// ToolInput is the typed Tool activity argument.
type ToolInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Etag       string
	Call       streaming.ToolCall
	Auth       durable.AuthContext
	Mounts     []durable.MountRecipe
	Specialist string
	// Known is this session's child ids for list_children (not a ChildOp ledger).
	Known []durable.SessionID
}

// ToolOutput is the typed Tool activity result. Spawn/cancel/await are this
// call's Runtime intent; the workflow starts, cancels, or waits via ExecuteChildWorkflow.
type ToolOutput struct {
	Etag        string
	Interrupted bool
	InterruptID string
	SpawnID     durable.SessionID
	SpawnSpec   string
	SpawnTask   string
	CancelID    durable.SessionID
	AwaitID     durable.SessionID
}

// CommitToolInput records a tool output on the staged batch without executing
// the tool. SessionWorkflow uses this after spawn_specialist child completion.
type CommitToolInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Etag       string
	Call       streaming.ToolCall
	Output     string
	Auth       durable.AuthContext
	Mounts     []durable.MountRecipe
	Specialist string
}

func (a *Activities) Inference(ctx context.Context, in InferenceInput) (InferenceOutput, error) {
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
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, streaming.StreamEvent{Type: streaming.StreamEventError, Content: "retry"}, true)
	}
	h, ms, etag, err := a.harness(ctx, in.SessionID, in.AgentID, in.MCPServers, in.Etag, in.Auth, in.Mounts, in.Specialist)
	if err != nil {
		pub := err
		if err := ctx.Err(); err != nil {
			pub = err
		}
		slog.ErrorContext(ctx, "inference harness", "area", telemetry.AreaRuntime, "error", pub)
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, streaming.StreamEvent{Type: streaming.StreamEventError, Error: pub, Content: pub.Error()}, true)
		return InferenceOutput{}, canceledIf(ctx, err)
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(in.SessionID), "inference")
	}()
	eng := h.Drive()
	out, stop := drive.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	if len(in.Resume) > 0 {
		if err := eng.ApplyResume(in.Resume); err != nil {
			_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, streaming.StreamEvent{Type: streaming.StreamEventError, Error: err, Content: err.Error()}, true)
			return InferenceOutput{}, canceledIf(ctx, err)
		}
		if pending := eng.PendingToolCalls(); len(pending) > 0 {
			etag, err = a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
			return InferenceOutput{Etag: etag, ToolCalls: pending}, err
		}
	}
	if in.User != nil {
		if err := eng.AbsorbUser(ctx, in.User, out); err != nil {
			slog.ErrorContext(ctx, "inference absorb", "area", telemetry.AreaRuntime, "error", err)
			return InferenceOutput{}, canceledIf(ctx, err)
		}
	}
	st := &drive.TurnState{HadToolRound: in.HadToolRound, ModelRequests: in.ModelRequests}
	step, err := eng.RunInference(ctx, st, out)
	if err != nil {
		pub := err
		if err := ctx.Err(); err != nil {
			pub = err
			slog.WarnContext(ctx, "inference cancelled", "area", telemetry.AreaRuntime)
		} else {
			slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
		}
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, streaming.StreamEvent{Type: streaming.StreamEventError, Error: pub, Content: pub.Error()}, true)
		return InferenceOutput{}, canceledIf(ctx, err)
	}
	etag, err = a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
	if err != nil {
		slog.ErrorContext(ctx, "inference persist", "area", telemetry.AreaRuntime, "error", err)
		return InferenceOutput{}, err
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
	return InferenceOutput{Etag: etag, Complete: step.Complete, ToolCalls: step.ToolCalls, Result: result}, nil
}

func (a *Activities) Tool(ctx context.Context, in ToolInput) (ToolOutput, error) {
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
		_ = a.publish(ctx, stream, in.SessionID, durable.TopicRetry, streaming.StreamEvent{Type: streaming.StreamEventError, Content: "retry"}, true)
	}
	h, ms, etag, err := a.harness(ctx, in.SessionID, in.AgentID, in.MCPServers, in.Etag, in.Auth, in.Mounts, in.Specialist)
	if err != nil {
		slog.ErrorContext(ctx, "tool harness", "area", telemetry.AreaHarness, "error", err)
		return ToolOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(in.SessionID), "tool")
	}()
	kids := &activityChildren{
		parent:  in.SessionID,
		agentID: in.AgentID,
		catalog: a.Catalog,
		known:   append([]durable.SessionID(nil), in.Known...),
	}
	h.BindChildHost(kids)
	eng := h.Drive()
	out, stop := drive.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	step, runErr := eng.RunToolCall(ctx, in.Call, out)
	if runErr != nil {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, "tool cancelled", "area", telemetry.AreaHarness, "tool", in.Call.Name)
			_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, streaming.StreamEvent{Type: streaming.StreamEventError, Error: err, Content: err.Error()}, true)
			return ToolOutput{}, canceledIf(ctx, runErr)
		}
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", in.Call.Name, "error", runErr)
	}
	etag, saveErr := a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
	if saveErr != nil {
		slog.ErrorContext(ctx, "tool persist", "area", telemetry.AreaHarness, "error", saveErr)
		if runErr != nil {
			return ToolOutput{}, fmt.Errorf("tool: %w: persist: %w", runErr, saveErr)
		}
		return ToolOutput{}, saveErr
	}
	status := "success"
	if step.Interrupted {
		status = "interrupt"
	}
	slog.InfoContext(ctx, "tool completed",
		"area", telemetry.AreaHarness, "tool", in.Call.Name, "status", status)
	return ToolOutput{
		Etag:        etag,
		Interrupted: step.Interrupted,
		InterruptID: step.InterruptID,
		SpawnID:     kids.spawnID,
		SpawnSpec:   kids.spawnSpec,
		SpawnTask:   kids.spawnTask,
		CancelID:    kids.cancelID,
		AwaitID:     kids.awaitID,
	}, nil
}

func (a *Activities) CommitToolOutput(ctx context.Context, in CommitToolInput) (ToolOutput, error) {
	h, ms, etag, err := a.harness(ctx, in.SessionID, in.AgentID, in.MCPServers, in.Etag, in.Auth, in.Mounts, in.Specialist)
	if err != nil {
		return ToolOutput{}, err
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(in.SessionID), "commit_tool")
	}()
	h.Drive().RecordToolResult(in.Call, in.Output)
	etag, err = a.save(ctx, in.SessionID, in.AgentID, h, etag, in.Mounts)
	if err != nil {
		return ToolOutput{}, err
	}
	presented := in.Call
	presented.Status = "success"
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	_ = a.publish(ctx, stream, in.SessionID, durable.TopicEvents, streaming.StreamEvent{
		Type:      streaming.StreamEventToolResult,
		MessageID: in.Call.Key(),
		Content:   in.Output,
		ToolCalls: []streaming.ToolCall{presented},
	}, true)
	return ToolOutput{Etag: etag}, nil
}

func (a *Activities) harness(ctx context.Context, id durable.SessionID, agentID string, extraMCP []mcp.MCPConfig, etag string, auth durable.AuthContext, mounts []durable.MountRecipe, specialist string) (*tacklr.TurnManager, *vfs.MountSession, string, error) {
	spec, ok := a.Catalog.Lookup(agentID)
	if !ok {
		return nil, nil, "", durable.ErrAgentNotFound
	}
	if specialist != "" {
		over, err := durable.OverlaySpecialist(spec, specialist)
		if err != nil {
			return nil, nil, "", err
		}
		spec = over
	}
	proj := a.Projection
	if proj == nil {
		proj = vfs.DirectProjection{}
	}
	ms, err := durable.OpenTurnVFS(ctx, string(id), spec, durable.BindingsForTurn(mounts, auth), proj)
	if err != nil {
		return nil, nil, "", err
	}
	opts := spec.Options
	opts.SessionID = string(id)
	opts.MountSession = ms
	if len(extraMCP) > 0 {
		mcpConfigs := make([]mcp.MCPConfig, 0, len(opts.MCPConfigs)+len(extraMCP))
		mcpConfigs = append(mcpConfigs, opts.MCPConfigs...)
		mcpConfigs = append(mcpConfigs, extraMCP...)
		opts.MCPConfigs = mcpConfigs
	}
	h, err := tacklr.NewTurnManager(ctx, opts)
	if err != nil {
		durable.CloseTurnVFS(ms, string(id), "construct")
		return nil, nil, "", err
	}
	if etag != "" {
		snap, loaded, loadErr := a.Snapshots.Load(ctx, id)
		if loadErr == nil {
			if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
				h.Close()
				durable.CloseTurnVFS(ms, string(id), "restore")
				return nil, nil, "", err
			}
			etag = loaded
		} else if !errors.Is(loadErr, durable.ErrSessionNotFound) {
			h.Close()
			durable.CloseTurnVFS(ms, string(id), "load")
			return nil, nil, "", loadErr
		}
	}
	return h, ms, etag, nil
}

func (a *Activities) save(ctx context.Context, id durable.SessionID, agentID string, h *tacklr.TurnManager, etag string, mounts []durable.MountRecipe) (string, error) {
	cp, err := h.Checkpoint()
	if err != nil {
		telemetry.RecordCheckpointAttempt(ctx, err)
		return "", err
	}
	etag, err = a.Snapshots.Save(ctx, id, durable.Snapshot{AgentID: agentID, Checkpoint: *cp, Mounts: mounts}, etag)
	telemetry.RecordCheckpointAttempt(ctx, err)
	return etag, err
}

const streamBatchInterval = 200 * time.Millisecond

func (a *Activities) openStream(ctx context.Context) *workflowstreams.Client {
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

func (a *Activities) emitter(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID) func(streaming.StreamEvent) {
	first := true
	return func(ev streaming.StreamEvent) {
		if ctx.Err() != nil && ev.Type != streaming.StreamEventError && ev.Type != streaming.StreamEventComplete {
			return
		}
		force := first || ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventInterrupt || ev.Type == streaming.StreamEventError
		first = false
		_ = a.publish(ctx, stream, sessionID, durable.TopicEvents, ev, force)
	}
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

func (a *Activities) publish(ctx context.Context, stream *workflowstreams.Client, sessionID durable.SessionID, topic string, ev streaming.StreamEvent, force bool) error {
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
