package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
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

// stepSession is the session an activity builds its world for. The
// workflow holds it for the session; the generic steps do not carry it.
type stepSession struct {
	SessionID  session.SessionID
	Identity   session.Identity
	MCPServers []mcp.MCPConfig
}

// toolInput is one Tool activity: one run of one call.
type toolInput struct {
	stepSession
	Run tacklr.ToolRun
}

// commitInput is one Commit activity. Base is the revision the previous
// Commit returned (see session.Driver).
type commitInput struct {
	stepSession
	Base session.Revision
	In   tacklr.CommitIn
}

type runJobInput struct {
	Name string
	Task string
}

// begin starts a step's activity: a heartbeat, the same-process cancel
// hook, turn telemetry, and the event stream; a retry is logged and
// published on the retry topic.
func (a *activities) begin(ctx context.Context, id session.SessionID, name string) (context.Context, *workflowstreams.Client, func()) {
	ctx, cancel := context.WithCancel(ctx)
	unbind := bindLiveTurn(id, cancel)
	stopBeat := startHeartbeat(ctx)
	ctx = telemetry.BindTurnContext(ctx, a.Agent.Name, string(id))
	stream := a.openStream(ctx)
	if activity.IsActivity(ctx) {
		if attempt := activity.GetInfo(ctx).Attempt; attempt > 1 {
			slog.WarnContext(ctx, name+" retry", "area", telemetry.AreaRuntime, "session_id", id, "agent_id", a.Agent.Name, "attempt", attempt)
			_ = a.publish(ctx, stream, id, session.TopicRetry, tacklr.StreamEvent{Type: tacklr.StreamEventError, Content: "retry"}, true)
		}
	}
	return ctx, stream, func() {
		closeStream(ctx, stream)
		stopBeat()
		unbind()
		cancel()
	}
}

// Inference is one model request on the committed window. It saves
// nothing: the reply commits once its calls have their results.
func (a *activities) Inference(ctx context.Context, in stepSession) (tacklr.Reply, error) {
	ctx, stream, end := a.begin(ctx, in.SessionID, "inference")
	defer end()
	h, ms, _, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers)
	if err != nil {
		slog.ErrorContext(ctx, "inference harness", "area", telemetry.AreaRuntime, "error", err)
		return tacklr.Reply{}, activityError(ctx, err)
	}
	defer adapter.AbandonTurn(h, ms)
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	reply, err := h.Infer(ctx, out)
	if err != nil {
		slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
	}
	return reply, activityError(ctx, err)
}

// Tool runs one tool call. It saves nothing: its outcome is this
// activity's result, which Commit applies once the round is complete.
func (a *activities) Tool(ctx context.Context, in toolInput) (tacklr.CallOutcome, error) {
	ctx, stream, end := a.begin(ctx, in.SessionID, "tool")
	defer end()
	h, ms, _, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers)
	if err != nil {
		slog.ErrorContext(ctx, "tool harness", "area", telemetry.AreaHarness, "error", err)
		return tacklr.CallOutcome{}, activityError(ctx, err)
	}
	defer adapter.AbandonTurn(h, ms)
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	o, err := h.RunTool(ctx, in.Run, out)
	if err == nil {
		// A call the turn cancelled reports the cancel, not what its tool
		// returned.
		err = ctx.Err()
	}
	if err != nil {
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", in.Run.Call.Name, "error", err)
	}
	return o, activityError(ctx, err)
}

// Commit is the only activity that saves the snapshot.
func (a *activities) Commit(ctx context.Context, in commitInput) (session.Revision, error) {
	ctx, stream, end := a.begin(ctx, in.SessionID, "commit")
	defer end()
	h, ms, rev, err := a.harness(ctx, in.SessionID, in.Identity, in.MCPServers)
	if err != nil {
		slog.ErrorContext(ctx, "commit harness", "area", telemetry.AreaRuntime, "error", err)
		return "", activityError(ctx, err)
	}
	defer adapter.AbandonTurn(h, ms)
	out, stop := tacklr.PipeStreamEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	rev, err = adapter.CommitTurn(ctx, a.Snapshots, in.SessionID, h, rev, in.Base, in.In, out)
	telemetry.RecordCheckpointAttempt(ctx, err)
	if err != nil {
		slog.ErrorContext(ctx, "commit failed", "area", telemetry.AreaRuntime, "error", err)
	}
	return rev, activityError(ctx, err)
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

func (a *activities) harness(ctx context.Context, id session.SessionID, rec session.Identity, extraMCP []mcp.MCPConfig) (*tacklr.TurnManager, *vfs.MountSession, session.Revision, error) {
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
	h, ms, err := adapter.ConstructTurn(ctx, spec, string(id), session.BindingsForTurn(rec.Mounts, sec.Auth), a.Projection, extraMCP, slices.Sorted(maps.Keys(a.Jobs)))
	if err != nil {
		return nil, nil, "", err
	}
	rev, err := adapter.RestoreTurn(ctx, a.Snapshots, id, h)
	if err != nil {
		adapter.AbandonTurn(h, ms)
		return nil, nil, "", err
	}
	return h, ms, rev, nil
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
