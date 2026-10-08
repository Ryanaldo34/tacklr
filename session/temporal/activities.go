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
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// liveTurns lets a same-process Runtime.Cancel stop the activity body without
// waiting for a Temporal heartbeat round-trip. Cross-process workers still
// cancel via activity context + heartbeats.
var liveTurns sync.Map // session.SessionID -> context.CancelFunc

func bindLiveTurn(id session.SessionID, cancel context.CancelFunc) func() {
	liveTurns.Store(id, cancel)
	return func() { liveTurns.Delete(id) }
}

func cancelLiveTurn(id session.SessionID) {
	if v, ok := liveTurns.LoadAndDelete(id); ok {
		if cancel, ok := v.(context.CancelFunc); ok {
			cancel()
		}
	}
}

// activityError is what an activity returns. Cancel is not a failure.
// A wrapped network error is retried. So is a model refusal (a content filter
// often clears on the next attempt) and a stale checkpoint (reload and save).
// Everything else stops on the first attempt.
func activityError(ctx context.Context, err error) error {
	if err == nil || temporal.IsCanceledError(err) {
		return err
	}
	if (ctx != nil && errors.Is(ctx.Err(), context.Canceled)) || errors.Is(err, context.Canceled) {
		if ctx != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		return temporal.NewCanceledError(err.Error())
	}
	if errors.Is(err, tacklr.ErrNetwork) || errors.Is(err, tacklr.ErrModelRefused) || errors.Is(err, session.ErrStaleCheckpoint) {
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), "", err)
}

// activities are the Inference and Tool bodies registered on the worker.
type activities struct {
	Agent          tacklr.AgentOptions
	Snapshots      session.SnapshotStore
	Projection     vfs.Projection
	Fallback       session.EventLog
	DisableStreams bool
	Secrets        session.SecretStorage
	Jobs           map[string]session.JobHandler
}

type runJobInput struct {
	Name string
	Task string
}

func (a *activities) Inference(ctx context.Context, in session.InferenceInput) (session.InferenceOutput, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer bindLiveTurn(in.SessionID, cancel)()
	defer startHeartbeat(ctx)()
	ctx = telemetry.BindTurnContext(ctx, a.Agent.Name, string(in.SessionID))
	attempt := int32(1)
	if activity.IsActivity(ctx) {
		attempt = activity.GetInfo(ctx).Attempt
	}
	if attempt > 1 {
		slog.WarnContext(ctx, "inference retry",
			"area", telemetry.AreaRuntime, "session_id", in.SessionID,
			"agent_id", a.Agent.Name, "attempt", attempt)
	} else {
		slog.InfoContext(ctx, "inference started",
			"area", telemetry.AreaRuntime, "session_id", in.SessionID,
			"agent_id", a.Agent.Name, "had_tools", in.HadToolRound)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, session.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Rec, in.MCPServers, in.State)
	if err != nil {
		pub := err
		if err := ctx.Err(); err != nil {
			pub = err
		}
		slog.ErrorContext(ctx, "inference harness", "area", telemetry.AreaRuntime, "error", pub)
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	defer func() {
		h.Close()
		adapter.CloseTurnVFS(ms)
	}()
	eng := h.Drive()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	if len(in.Resume) > 0 {
		if err := eng.ApplyResume(in.Resume); err != nil {
			return session.InferenceOutput{}, activityError(ctx, err)
		}
		if pending := eng.PendingToolCalls(); len(pending) > 0 {
			_, err = a.save(ctx, in.SessionID, h, rev, in.Rec)
			return session.InferenceOutput{ToolCalls: pending}, activityError(ctx, err)
		}
	}
	extra := in.Extra
	if in.User != nil {
		extra = append(extra, in.User)
	}
	if _, err := adapter.AbsorbAll(ctx, eng.AbsorbUser, extra, out); err != nil {
		slog.ErrorContext(ctx, "inference absorb", "area", telemetry.AreaRuntime, "error", err)
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	st := &tacklr.TurnState{HadToolRound: in.HadToolRound, ModelRequests: in.ModelRequests}
	step, err := eng.RunInference(ctx, st, out)
	if err != nil {
		if ctx.Err() != nil {
			slog.WarnContext(ctx, "inference cancelled", "area", telemetry.AreaRuntime)
		} else {
			slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
		}
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	if _, err = a.save(ctx, in.SessionID, h, rev, in.Rec); err != nil {
		slog.ErrorContext(ctx, "inference persist", "area", telemetry.AreaRuntime, "error", err)
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	result := ""
	if step.Complete {
		msgs := h.Drive().Messages()
		for i := len(msgs) - 1; i >= 0; i-- {
			if m := msgs[i]; m != nil && m.Role == tacklr.RoleAssistant && m.Content != "" {
				result = m.Content
				break
			}
		}
	}
	slog.InfoContext(ctx, "inference completed",
		"area", telemetry.AreaRuntime, "complete", step.Complete, "tool_calls", len(step.ToolCalls))
	return session.InferenceOutput{Complete: step.Complete, ToolCalls: step.ToolCalls, Result: result}, nil
}

func (a *activities) Tool(ctx context.Context, in session.ToolInput) (session.ToolOutput, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer bindLiveTurn(in.SessionID, cancel)()
	defer startHeartbeat(ctx)()
	ctx = telemetry.BindTurnContext(ctx, a.Agent.Name, string(in.SessionID))
	attempt := int32(1)
	if activity.IsActivity(ctx) {
		attempt = activity.GetInfo(ctx).Attempt
	}
	if attempt > 1 {
		slog.WarnContext(ctx, "tool retry",
			"area", telemetry.AreaHarness, "session_id", in.SessionID,
			"agent_id", a.Agent.Name, "tool", in.Call.Name, "attempt", attempt)
	} else {
		slog.InfoContext(ctx, "tool started",
			"area", telemetry.AreaHarness, "session_id", in.SessionID,
			"agent_id", a.Agent.Name, "tool", in.Call.Name, "namespace", in.Call.Namespace)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, session.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Rec, in.MCPServers, in.State)
	if err != nil {
		slog.ErrorContext(ctx, "tool harness", "area", telemetry.AreaHarness, "error", err)
		return session.ToolOutput{}, activityError(ctx, err)
	}
	defer func() {
		h.Close()
		adapter.CloseTurnVFS(ms)
	}()
	kids := &activityChildren{
		parent: in.SessionID,
		agent:  a.Agent,
		jobs:   a.Jobs,
		known:  append([]session.SessionID(nil), in.Rec.Children...),
	}
	h.BindJobHost(kids)
	eng := h.Drive()
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	step, runErr := eng.RunToolCall(ctx, in.Call, out)
	if runErr != nil {
		if err := ctx.Err(); err != nil {
			slog.WarnContext(ctx, "tool cancelled", "area", telemetry.AreaHarness, "tool", in.Call.Name)
			return session.ToolOutput{}, activityError(ctx, runErr)
		}
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", in.Call.Name, "error", runErr)
	}
	_, saveErr := a.save(ctx, in.SessionID, h, rev, in.Rec)
	if saveErr != nil {
		slog.ErrorContext(ctx, "tool persist", "area", telemetry.AreaHarness, "error", saveErr)
		if runErr != nil {
			saveErr = fmt.Errorf("tool: %w: persist: %w", runErr, saveErr)
		}
		return session.ToolOutput{}, activityError(ctx, saveErr)
	}
	status := "success"
	if step.Interrupted {
		status = "interrupt"
	}
	slog.InfoContext(ctx, "tool completed",
		"area", telemetry.AreaHarness, "tool", in.Call.Name, "status", status)
	await := kids.awaitID
	if await == "" && step.AwaitJobID != "" {
		await = session.SessionID(step.AwaitJobID)
	}
	return session.ToolOutput{
		Interrupted:   step.Interrupted,
		InterruptID:   step.InterruptID,
		InterruptData: step.InterruptData,
		CancelID:      kids.cancelID,
		AwaitID:       await,
		JobID:         kids.jobID,
		JobName:       kids.jobName,
		JobTask:       kids.jobTask,
		Child:         kids.child,
	}, nil
}

func (a *activities) RunJob(ctx context.Context, in runJobInput) (string, error) {
	if a.Jobs == nil {
		return "", activityError(ctx, fmt.Errorf("%w: %s", tacklr.ErrNotFound, in.Name))
	}
	fn, ok := a.Jobs[in.Name]
	if !ok {
		return "", activityError(ctx, fmt.Errorf("%w: %s", tacklr.ErrNotFound, in.Name))
	}
	out, err := fn(ctx, in.Task)
	return out, activityError(ctx, err)
}

func (a *activities) CommitToolOutput(ctx context.Context, in session.CommitInput) (session.ToolOutput, error) {
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Rec, in.MCPServers, in.State)
	if err != nil {
		return session.ToolOutput{}, activityError(ctx, err)
	}
	defer func() {
		h.Close()
		adapter.CloseTurnVFS(ms)
	}()
	h.Drive().RecordToolResult(in.Call, in.Output)
	if _, err = a.save(ctx, in.SessionID, h, rev, in.Rec); err != nil {
		return session.ToolOutput{}, activityError(ctx, err)
	}
	presented := in.Call
	presented.Status = "success"
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	_ = a.publish(ctx, stream, in.SessionID, session.TopicEvents, tacklr.StreamEvent{
		Type:      tacklr.StreamEventToolResult,
		MessageID: in.Call.Key(),
		Content:   in.Output,
		ToolCalls: []tacklr.ToolCall{presented},
	}, true)
	return session.ToolOutput{}, nil
}

func (a *activities) harness(ctx context.Context, id session.SessionID, rec session.Snapshot, extraMCP []mcp.MCPConfig, state map[string]any) (*tacklr.TurnManager, *vfs.MountSession, session.Revision, error) {
	sec, err := a.Secrets.Get(ctx, id)
	if err != nil {
		return nil, nil, "", err
	}
	if len(sec.Auth.Bindings) == 0 && rec.Parent != "" {
		sec, err = a.Secrets.Get(ctx, rec.Parent)
		if err != nil {
			return nil, nil, "", err
		}
	}
	spec := a.Agent
	if rec.Specialist != "" {
		over, err := adapter.OverlaySpecialist(spec, rec.Specialist)
		if err != nil {
			return nil, nil, "", err
		}
		spec = over
	}
	h, ms, err := adapter.ConstructTurn(ctx, spec, string(id), session.BindingsForTurn(rec.Mounts, sec.Auth), a.Projection, extraMCP)
	if err != nil {
		return nil, nil, "", err
	}
	rev, err := adapter.RestoreTurn(ctx, a.Snapshots, id, h, state)
	if err != nil {
		adapter.AbandonTurn(h, ms)
		return nil, nil, "", err
	}
	return h, ms, rev, nil
}

func (a *activities) save(ctx context.Context, id session.SessionID, h *tacklr.TurnManager, expected session.Revision, rec session.Snapshot) (session.Revision, error) {
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

func (a *activities) emitter(ctx context.Context, stream *workflowstreams.Client, sessionID session.SessionID) func(tacklr.StreamEvent) {
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
		_ = a.publish(ctx, stream, sessionID, session.TopicEvents, ev, force)
	}
}

// emitEventInput is the typed EmitEvent activity argument.
type emitEventInput struct {
	SessionID session.SessionID
	Event     tacklr.StreamEvent
}

// EmitEvent publishes a turn-finished stream event after SessionWorkflow has
// committed Status (complete, failed, or yield).
func (a *activities) EmitEvent(ctx context.Context, in emitEventInput) error {
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	return activityError(ctx, a.publish(ctx, stream, in.SessionID, session.TopicEvents, in.Event, true))
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

func (a *activities) publish(ctx context.Context, stream *workflowstreams.Client, sessionID session.SessionID, topic string, ev tacklr.StreamEvent, force bool) error {
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
