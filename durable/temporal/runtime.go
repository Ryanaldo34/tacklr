package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/converter"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Runtime implements durable.Runtime with one Temporal workflow per session.
type Runtime struct {
	client         client.Client
	taskQueue      string
	catalog        durable.Catalog
	fallback       *inprocess.MemoryEventLog
	snapshots      durable.SnapshotStore
	disableStreams bool

	mu     sync.Mutex
	closed map[durable.SessionID]struct{}
}

// Option configures New.
type Option func(*Runtime)

// WithSnapshotStore replaces the memory SnapshotStore.
func WithSnapshotStore(s durable.SnapshotStore) Option {
	return func(r *Runtime) {
		if s != nil {
			r.snapshots = s
		}
	}
}

// WithEventLog sets the fallback EventLog used when Workflow Streams is disabled.
func WithEventLog(l *inprocess.MemoryEventLog) Option {
	return func(r *Runtime) {
		if l != nil {
			r.fallback = l
		}
	}
}

// WithDisableStreams publishes and subscribes only via the fallback EventLog.
// The Temporal testsuite mock cannot serve Workflow Streams (GetSystemInfo);
// use this in unit tests. The dev-server integration test leaves streams on.
func WithDisableStreams() Option {
	return func(r *Runtime) { r.disableStreams = true }
}

// New constructs a Temporal Runtime. The host must also run Worker on the same
// task queue with EnableSessionWorker. Tokens travel on Prompt/Resume payloads.
func New(c client.Client, taskQueue string, catalog durable.Catalog, opts ...Option) *Runtime {
	if c == nil {
		panic("temporal: Client is required")
	}
	if catalog == nil {
		panic("temporal: Catalog is required")
	}
	if taskQueue == "" {
		taskQueue = "tacklr"
	}
	r := &Runtime{
		client:    c,
		taskQueue: taskQueue,
		catalog:   catalog,
		fallback:  inprocess.NewMemoryEventLog(),
		snapshots: inprocess.NewMemorySnapshot(),
		closed:    make(map[durable.SessionID]struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *Runtime) workflowID(id durable.SessionID) string { return string(id) }

// CreateSession implements durable.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req durable.CreateSession) (durable.SessionID, error) {
	agentID := req.AgentID
	if agentID == "" {
		agentID = r.catalog.DefaultID()
	}
	if agentID != "" {
		if _, ok := r.catalog.Lookup(agentID); !ok {
			return "", durable.ErrAgentNotFound
		}
	}
	id := req.SessionID
	if id == "" {
		id = durable.SessionID(uuid.NewString())
	}
	_, err := r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        r.workflowID(id),
		TaskQueue: r.taskQueue,
	}, SessionWorkflow, WorkflowInput{SessionID: id, AgentID: agentID, MCPServers: req.MCPServers, Mounts: req.Mounts})
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return "", fmt.Errorf("%w: %s", durable.ErrSessionExists, id)
		}
		return "", err
	}
	return id, nil
}

func (r *Runtime) markClosed(id durable.SessionID) {
	r.mu.Lock()
	r.closed[id] = struct{}{}
	r.mu.Unlock()
}

func (r *Runtime) isClosed(id durable.SessionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.closed[id]
	return ok
}

func (r *Runtime) signal(ctx context.Context, id durable.SessionID, name string, arg any) error {
	if name != signalClose && r.isClosed(id) {
		return durable.ErrSessionNotFound
	}
	err := r.client.SignalWorkflow(ctx, r.workflowID(id), "", name, arg)
	if err == nil {
		return nil
	}
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return durable.ErrSessionNotFound
	}
	return err
}

// Prompt implements durable.Runtime.
func (r *Runtime) Prompt(ctx context.Context, sessionID durable.SessionID, msg durable.Prompt) error {
	return r.signal(ctx, sessionID, signalPrompt, promptSignal{
		Text:        msg.Text,
		UserMessage: msg.UserMessage,
		AgentID:     msg.AgentID,
		MCPServers:  msg.MCPServers,
		Auth:        msg.Auth,
	})
}

// Resume implements durable.Runtime.
func (r *Runtime) Resume(ctx context.Context, sessionID durable.SessionID, resume durable.Resume) error {
	return r.signal(ctx, sessionID, signalResume, resumeSignal{Responses: resume.Responses, Auth: resume.Auth})
}

// Cancel implements durable.Runtime.
func (r *Runtime) Cancel(ctx context.Context, sessionID durable.SessionID) error {
	cancelLiveTurn(sessionID)
	_ = r.fallback.Append(context.WithoutCancel(ctx), sessionID, durable.TopicEvents, streaming.StreamEvent{
		Type:    streaming.StreamEventError,
		Error:   context.Canceled,
		Fail:    context.Canceled.Error(),
		Content: context.Canceled.Error(),
	})
	return r.signal(ctx, sessionID, signalCancel, nil)
}

// Close implements durable.Runtime.
func (r *Runtime) Close(ctx context.Context, sessionID durable.SessionID) error {
	r.markClosed(sessionID)
	sigErr := r.signal(ctx, sessionID, signalClose, nil)
	_ = r.snapshots.Delete(ctx, sessionID)
	_ = r.fallback.CloseSession(ctx, sessionID)
	durable.ClearSessionVFS(r.catalog, sessionID)
	if sigErr != nil && !errors.Is(sigErr, durable.ErrSessionNotFound) {
		return sigErr
	}
	return nil
}

type sub struct {
	ch     <-chan streaming.StreamEvent
	cancel context.CancelFunc
}

func (s *sub) Events() <-chan streaming.StreamEvent { return s.ch }
func (s *sub) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Head implements durable.Runtime. When Workflow Streams is on, this is the
// stream's next offset so Subscribe(after Head) skips prior-turn events.
func (r *Runtime) Head(ctx context.Context, sessionID durable.SessionID) (durable.Seq, error) {
	if !r.disableStreams {
		val, err := r.client.QueryWorkflow(ctx, r.workflowID(sessionID), "", workflowstreams.OffsetQueryName)
		if err == nil {
			var n int64
			if err := val.Get(&n); err == nil && n >= 0 {
				return durable.Seq(n), nil //nolint:gosec // G115: stream offsets are well below MaxUint64
			}
		}
	}
	return r.fallback.Head(ctx, sessionID)
}

// Subscribe implements durable.Runtime.
func (r *Runtime) Subscribe(ctx context.Context, sessionID durable.SessionID, after durable.Seq) (durable.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	if r.disableStreams {
		ch, err := r.fallback.Subscribe(subCtx, sessionID, after)
		if err != nil {
			cancel()
			return nil, err
		}
		return &sub{ch: ch, cancel: cancel}, nil
	}
	c := workflowstreams.NewClient(r.client, r.workflowID(sessionID), workflowstreams.Options{})
	ch := make(chan streaming.StreamEvent, 64)
	dc := converter.GetDefaultDataConverter()
	go func() {
		defer close(ch)
		defer func() { _ = c.Close(subCtx) }()
		off := int64(after) //nolint:gosec // G115: EventLog seq is well below MaxInt64
		for item, err := range c.Subscribe(subCtx, workflowstreams.SubscribeOptions{
			Topics:     []string{durable.TopicEvents},
			FromOffset: off,
		}) {
			if err != nil {
				return
			}
			var ev streaming.StreamEvent
			if err := dc.FromPayload(item.Data, &ev); err != nil {
				return
			}
			if ev.Error == nil && ev.Fail != "" {
				ev.Error = failFromWire(ev.Fail)
			}
			select {
			case ch <- ev:
			case <-subCtx.Done():
				return
			}
		}
	}()
	return &sub{ch: ch, cancel: cancel}, nil
}

// FallbackLog is the in-process EventLog used when Workflow Streams is unavailable (tests).
func (r *Runtime) FallbackLog() durable.EventLog { return r.fallback }

func failFromWire(s string) error {
	switch {
	case strings.Contains(s, tacklr.ErrModelRefused.Error()):
		return tacklr.ErrModelRefused
	case strings.Contains(s, tacklr.ErrMaxTokens.Error()):
		return tacklr.ErrMaxTokens
	case strings.Contains(s, tacklr.ErrMaxTurnRequests.Error()):
		return tacklr.ErrMaxTurnRequests
	case strings.Contains(s, context.Canceled.Error()), strings.Contains(s, "context cancelled"):
		return context.Canceled
	default:
		return errors.New(s)
	}
}

var _ durable.Runtime = (*Runtime)(nil)
