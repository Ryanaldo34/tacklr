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

// activities are the step bodies registered on the worker.
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
			"agent_id", a.Agent.Name)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, session.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers, in.State)
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
	extra := in.Extra
	if in.User != nil {
		extra = append(extra, in.User)
	}
	for _, msg := range extra {
		if err := eng.AbsorbUser(ctx, msg, out); err != nil {
			slog.ErrorContext(ctx, "inference absorb", "area", telemetry.AreaRuntime, "error", err)
			return session.InferenceOutput{}, activityError(ctx, err)
		}
	}
	step, err := eng.RunInference(ctx, out)
	if err != nil {
		if ctx.Err() != nil {
			slog.WarnContext(ctx, "inference cancelled", "area", telemetry.AreaRuntime)
		} else {
			slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
		}
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	if _, err = a.save(ctx, in.SessionID, h, rev); err != nil {
		slog.ErrorContext(ctx, "inference persist", "area", telemetry.AreaRuntime, "error", err)
		return session.InferenceOutput{}, activityError(ctx, err)
	}
	slog.InfoContext(ctx, "inference completed", "area", telemetry.AreaRuntime, "complete", step.Complete)
	return session.InferenceOutput{Complete: step.Complete, Result: step.Result}, nil
}

// Tools runs the open round's runnable calls (after resume answers, when
// set): the harness runs them concurrently and records them in call order,
// then the snapshot is saved once.
func (a *activities) Tools(ctx context.Context, in session.ToolsInput) ([]session.ToolOutput, error) {
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
		slog.WarnContext(ctx, "tools retry",
			"area", telemetry.AreaHarness, "session_id", in.SessionID,
			"agent_id", a.Agent.Name, "attempt", attempt)
	}
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	if attempt > 1 {
		_ = a.publish(ctx, stream, in.SessionID, session.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
	}
	// RunTools applies in.State: a retried step must not apply it again.
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers, nil)
	if err != nil {
		slog.ErrorContext(ctx, "tools harness", "area", telemetry.AreaHarness, "error", err)
		return nil, activityError(ctx, err)
	}
	defer func() {
		h.Close()
		adapter.CloseTurnVFS(ms)
	}()
	h.BindJobHost(activityChildren{parent: in.SessionID, agent: a.Agent, jobs: a.Jobs, known: in.Children})
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	steps, runErr := h.Drive().RunTools(ctx, in.Seq, in.Resume, in.State, out)
	// Save what the calls recorded even when the commit failed, so a retry
	// commits it instead of running the tools again.
	_, err = a.save(ctx, in.SessionID, h, rev)
	if err = errors.Join(runErr, err); err != nil {
		slog.ErrorContext(ctx, "tools failed", "area", telemetry.AreaHarness, "error", err)
		return nil, activityError(ctx, err)
	}
	outs := make([]session.ToolOutput, len(steps))
	for i, step := range steps {
		outs[i] = session.ToolOutput{
			Call:          step.Call,
			Interrupted:   step.Interrupted,
			InterruptID:   step.InterruptID,
			InterruptData: step.InterruptData,
			AwaitID:       session.SessionID(step.AwaitJobID),
			Starts:        step.Starts,
			Cancels:       step.Cancels,
		}
	}
	return outs, nil
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

// CommitToolOutput records an awaited child's output as its call's result
// and commits the round when that was its last result.
func (a *activities) CommitToolOutput(ctx context.Context, in session.CommitInput) error {
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers, in.State)
	if err != nil {
		return activityError(ctx, err)
	}
	defer func() {
		h.Close()
		adapter.CloseTurnVFS(ms)
	}()
	stream := a.openStream(ctx)
	defer closeStream(ctx, stream)
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	if err := h.Drive().RecordToolResult(ctx, in.Call, in.Output, in.Failed, out); err != nil {
		return activityError(ctx, err)
	}
	_, err = a.save(ctx, in.SessionID, h, rev)
	return activityError(ctx, err)
}

func (a *activities) harness(ctx context.Context, id session.SessionID, rec session.Identity, extraMCP []mcp.MCPConfig, state map[string]any) (*tacklr.TurnManager, *vfs.MountSession, session.Revision, error) {
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

func (a *activities) save(ctx context.Context, id session.SessionID, h *tacklr.TurnManager, expected session.Revision) (session.Revision, error) {
	cp, err := h.Checkpoint()
	if err != nil {
		telemetry.RecordCheckpointAttempt(ctx, err)
		return "", err
	}
	rev, err := a.Snapshots.Save(ctx, id, cp, expected)
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
