package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
	"github.com/ryanaldo34/tacklr/workspace"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/loop"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

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
	// A failure a client acts on crosses the boundary typed with its kind,
	// which the workflow maps back to the sentinel (executor.stepErr).
	switch kind := session.KindOf(err); kind {
	case session.FailRefused:
		return temporal.NewApplicationErrorWithCause(err.Error(), string(kind), err)
	case session.FailMaxTokens, session.FailContextLength:
		return temporal.NewNonRetryableApplicationError(err.Error(), string(kind), err)
	}
	if errors.Is(err, tacklr.ErrNetwork) || errors.Is(err, session.ErrStaleCheckpoint) {
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), "", err)
}

// activities are the step bodies registered on the worker. queue is this
// worker's own task queue: the workflow sends the steps of a session whose
// harness this worker holds there.
type activities struct {
	Agent          tacklr.AgentOptions
	Store          session.Store
	Workspace      workspace.Config
	FuseRoot       string
	Fallback       session.EventLog
	DisableStreams bool
	Secrets        session.SecretStorage
	Jobs           map[string]session.JobHandler
	harnesses      *harnessCache
	queue          string
	// live sends the deltas of this worker's model calls to the clients
	// on its live endpoint, at liveURL.
	live    *session.Live
	liveURL string
}

// stepSession is the session an activity builds its harness for. The
// workflow holds it for the session; the generic steps do not carry it.
type stepSession struct {
	SessionID  session.ID
	Identity   loop.Identity
	MCPServers []session.MCPServer
}

// toolInput is one Tool activity: one run of one call.
type toolInput struct {
	stepSession
	Call tacklr.Call
}

// commitInput is one Commit activity. Base is the revision the previous
// Commit returned (see loop.Executor).
type commitInput struct {
	stepSession
	Base session.Revision
	In   tacklr.CommitIn
	// NewTurn says this is a new turn's first Commit: once it saved, the
	// turns before it are committed, and what they left goes.
	NewTurn bool `json:",omitempty"`
}

type runJobInput struct {
	Name string
	Task string
}

// begin starts a step's activity: a heartbeat (which delivers the
// workflow's cancel), turn telemetry, and the event stream; a retry is
// logged and published on the retry topic. A stream that cannot open fails
// the step, so its events are never dropped.
func (a *activities) begin(ctx context.Context, id session.ID, name string) (context.Context, *workflowstreams.Client, func(), error) {
	stream, err := a.openStream(ctx)
	if err != nil {
		return ctx, nil, func() {}, activityError(ctx, err)
	}
	stopBeat := startHeartbeat(ctx)
	ctx = telemetry.BindTurnContext(ctx, a.Agent.Name, string(id))
	if activity.IsActivity(ctx) {
		if attempt := activity.GetInfo(ctx).Attempt; attempt > 1 {
			slog.WarnContext(ctx, name+" retry", "area", telemetry.AreaRuntime, "session_id", id, "agent_id", a.Agent.Name, "attempt", attempt)
			_ = a.publish(ctx, stream, id, session.TopicRetry, session.Event{Type: session.EventError, Content: "retry"}, "", true)
		}
	}
	return ctx, stream, func() {
		closeStream(ctx, stream)
		stopBeat()
	}, nil
}

// Inference is one model request on the committed window. It saves
// nothing: the reply commits once its calls have their results.
func (a *activities) Inference(ctx context.Context, in stepSession) (tacklr.Reply, error) {
	ctx, stream, end, err := a.begin(ctx, in.SessionID, "inference")
	defer end()
	if err != nil {
		return tacklr.Reply{}, err
	}
	w, err := a.harness(ctx, in)
	if err != nil {
		return tacklr.Reply{}, activityError(ctx, err)
	}
	out, stop := tacklr.PipeEvents(a.emitter(ctx, stream, in.SessionID))
	defer stop()
	var reply tacklr.Reply
	err = a.harnesses.Read(ctx, a.Store, w, func(h *tacklr.Harness) error {
		reply, err = h.Infer(ctx, out)
		return err
	})
	if err != nil {
		slog.ErrorContext(ctx, "inference failed", "area", telemetry.AreaRuntime, "error", err)
	}
	return reply, activityError(ctx, err)
}

// Tool runs one tool call. The call as its run left it is this activity's
// result, which Commit applies once the round is complete; the call's body
// (its result, park and writes) is kept in the session.Store, so history
// holds it nowhere. The tool-result event goes on the stream once the body
// is stored, as a reference Subscribe resolves.
func (a *activities) Tool(ctx context.Context, in toolInput) (tacklr.Call, error) {
	c := in.Call
	ctx, stream, end, err := a.begin(ctx, in.SessionID, "tool")
	defer end()
	if err != nil {
		return c, err
	}
	w, err := a.harness(ctx, in.stepSession)
	if err != nil {
		return c, activityError(ctx, err)
	}
	emit := a.emitter(ctx, stream, in.SessionID)
	var results []session.Event
	out, stop := tacklr.PipeEvents(func(ev session.Event) {
		if ev.Type == session.EventToolResult {
			results = append(results, ev)
			return
		}
		emit(ev)
	})
	err = a.harnesses.Read(ctx, a.Store, w, func(h *tacklr.Harness) error {
		c, err = adapter.RunCall(ctx, a.Store, in.SessionID, h, in.Call, out)
		return err
	})
	stop()
	ref := ""
	if err == nil {
		ref = c.Key()
	}
	for _, ev := range results {
		_ = a.publish(ctx, stream, in.SessionID, session.TopicEvents, ev, ref, true)
	}
	if err == nil {
		// A call the turn cancelled reports the cancel, not what its tool
		// returned.
		err = ctx.Err()
	}
	if err != nil {
		slog.ErrorContext(ctx, "tool failed", "area", telemetry.AreaHarness, "tool", c.Tool.Name, "error", err)
	}
	return c, activityError(ctx, err)
}

// Commit is the only activity that saves the snapshot.
func (a *activities) Commit(ctx context.Context, in commitInput) (session.Revision, error) {
	ctx, stream, end, err := a.begin(ctx, in.SessionID, "commit")
	defer end()
	if err != nil {
		return "", err
	}
	w, err := a.harness(ctx, in.stepSession)
	if err != nil {
		return "", activityError(ctx, err)
	}
	var head session.Seq
	if in.NewTurn && a.Fallback != nil {
		if head, err = a.Fallback.Head(ctx, in.SessionID); err != nil {
			return "", activityError(ctx, err)
		}
	}
	out, stop := tacklr.PipeEvents(a.emitter(ctx, stream, in.SessionID))
	rev, err := a.harnesses.Commit(ctx, a.Store, w, in.Base, in.In, out)
	stop()
	telemetry.RecordCheckpointAttempt(ctx, err)
	if err != nil {
		slog.ErrorContext(ctx, "commit failed", "area", telemetry.AreaRuntime, "error", err)
		return rev, activityError(ctx, err)
	}
	if in.NewTurn {
		err = a.endTurns(ctx, in.SessionID, head)
	}
	return rev, activityError(ctx, err)
}

// endTurns drops what the turns before a new one left: their call bodies
// and, when the steps write to it, the fallback log's events
// through head. A retry of the Commit drops them again.
func (a *activities) endTurns(ctx context.Context, id session.ID, head session.Seq) error {
	err := a.Store.ClearCalls(ctx, id)
	if a.Fallback != nil {
		err = errors.Join(err, a.Fallback.Trim(ctx, id, head))
	}
	return err
}

func (a *activities) RunJob(ctx context.Context, in runJobInput) (string, error) {
	fn, ok := a.Jobs[in.Name]
	if !ok {
		return "", activityError(ctx, fmt.Errorf("%w: %s", tacklr.ErrNotFound, in.Name))
	}
	out, err := fn(ctx, in.Task)
	return out, activityError(ctx, err)
}

// harness names in's harness: its key covers the specialist, the mounts and
// their auth, the MCP servers and the job names, so a change to any of them
// builds a new harness. Building it opens the VFS, connects every MCP server
// and lists its tools (the model's tool list needs them), and loads skills.
func (a *activities) harness(ctx context.Context, in stepSession) (harnessSpec, error) {
	sec, err := a.Secrets.Get(ctx, in.SessionID)
	if err != nil {
		return harnessSpec{}, err
	}
	if len(sec.Auth.Bindings) == 0 && in.Identity.Parent != "" {
		if sec, err = a.Secrets.Get(ctx, in.Identity.Parent); err != nil {
			return harnessSpec{}, err
		}
	}
	bindings := adapter.BindingsForTurn(in.Identity.Mounts, sec.Auth)
	jobs := slices.Sorted(maps.Keys(a.Jobs))
	// Strings and slices of them: the key always encodes.
	key, _ := json.Marshal(struct {
		Specialist string
		Bindings   []mount.Binding
		MCP        []session.MCPServer
		Jobs       []string
	}{in.Identity.Specialist, bindings, in.MCPServers, jobs})
	sum := sha256.Sum256(key)
	return harnessSpec{ID: in.SessionID, Key: hex.EncodeToString(sum[:]), Build: func(ctx context.Context) (*tacklr.Harness, *fuse.Server, error) {
		spec := a.Agent
		if in.Identity.Specialist != "" {
			over, err := adapter.OverlaySpecialist(spec, in.Identity.Specialist)
			if err != nil {
				return nil, nil, err
			}
			spec = over
		}
		return constructTurn(ctx, spec, a.Workspace, a.FuseRoot, string(in.SessionID), bindings, in.MCPServers, jobs)
	}}, nil
}

// WorkerQueue is this worker's route: its own task queue and live URL. The
// workflow asks a worker on the shared queue, then sends the session's
// steps to the answer, and clients dial its live URL.
func (a *activities) WorkerQueue(context.Context) (protocol.Route, error) {
	return protocol.Route{Queue: a.queue, Live: a.liveURL}, nil
}

// Forget deletes what the harnesses of a closed session and of its jobs
// saved: their messages in the agent's brain. An agent without a brain
// saved none.
func (a *activities) Forget(ctx context.Context, ids []session.ID) error {
	if a.Agent.Brain == nil {
		return nil
	}
	errs := make([]error, 0, len(ids))
	for _, id := range ids {
		errs = append(errs, a.Agent.Brain.DeleteSessionMessages(ctx, a.Agent.SearchNamespace, string(id)))
	}
	return errors.Join(errs...)
}

// CloseHarness closes the session's harness when the session ends.
func (a *activities) CloseHarness(_ context.Context, id session.ID) error {
	a.harnesses.Close(id)
	return nil
}

const streamBatchInterval = 200 * time.Millisecond

// openStream is the activity's client for the workflow's event stream; nil
// when streams are off or ctx is not an activity (the fallback EventLog
// takes the events).
func (a *activities) openStream(ctx context.Context) (*workflowstreams.Client, error) {
	if a.DisableStreams || !activity.IsActivity(ctx) {
		return nil, nil
	}
	return workflowstreams.NewClientFromActivity(ctx, workflowstreams.Options{
		BatchInterval: streamBatchInterval,
	})
}

func closeStream(ctx context.Context, c *workflowstreams.Client) {
	if c != nil {
		_ = c.Close(publishContext(ctx))
	}
}

func (a *activities) emitter(ctx context.Context, stream *workflowstreams.Client, sessionID session.ID) func(session.Event) {
	first := true
	return func(ev session.Event) {
		switch ev.Type {
		case session.EventComplete, session.EventInterrupt, session.EventError:
			// Turn-finished signals. SessionWorkflow commits Status, then EmitEvent.
			return
		}
		if ctx.Err() != nil {
			return
		}
		if ev.Live() {
			// Live only: a message's complete event carries its text, and a
			// tool's result its body. A worker with no live endpoint
			// sends none.
			if a.live != nil {
				a.live.Publish(sessionID, ev)
			}
			return
		}
		force := first
		first = false
		_ = a.publish(ctx, stream, sessionID, session.TopicEvents, ev, "", force)
	}
}

// emitEventInput is the typed EmitEvent activity argument.
type emitEventInput struct {
	SessionID session.ID
	Event     session.Event
}

// EmitEvent publishes a turn-finished stream event after SessionWorkflow has
// committed Status (complete, failed, or yield).
func (a *activities) EmitEvent(ctx context.Context, in emitEventInput) error {
	stream, err := a.openStream(ctx)
	if err != nil {
		return activityError(ctx, err)
	}
	defer closeStream(ctx, stream)
	return activityError(ctx, a.publish(ctx, stream, in.SessionID, session.TopicEvents, in.Event, "", true))
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
	if ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

// publish sends a durable event on the stream, or to the fallback log when
// the step has no stream. With call set, the stream carries ev without
// its content; the fallback log, which is not history, gets it whole.
func (a *activities) publish(ctx context.Context, stream *workflowstreams.Client, sessionID session.ID, topic string, ev session.Event, call string, force bool) error {
	if ev.Error != nil && ev.Fail == "" {
		ev.Fail = ev.Error.Error()
	}
	pubCtx := publishContext(ctx)
	if stream != nil {
		item := protocol.StreamItem{Event: ev, Call: call}
		if call != "" {
			item.Content = ""
		}
		stream.Topic(topic).Publish(item, force)
		if force {
			return stream.Flush(pubCtx)
		}
		return nil
	}
	// A step without a stream has streams off, and New gave it a fallback.
	return a.Fallback.Append(pubCtx, sessionID, topic, ev)
}
