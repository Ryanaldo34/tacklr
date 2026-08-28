package temporal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	client              client.Client
	taskQueue           string
	catalog             durable.Catalog
	fallback            *inprocess.MemoryEventLog
	snapshots           durable.SnapshotStore
	disableStreams      bool
	turnLocalityTimeout time.Duration

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

// WithTurnLocality keeps a turn's activities on one Temporal worker for that
// duration so the turn's VFS stays on the same process. Zero (default) does
// not pin activities; they may run on any worker.
func WithTurnLocality(d time.Duration) Option {
	return func(r *Runtime) { r.turnLocalityTimeout = d }
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

// CreateSession implements durable.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req durable.CreateSession) (durable.SessionID, error) {
	agentID := req.AgentID
	if agentID == "" {
		agentID = r.catalog.DefaultID()
	}
	if _, ok := r.catalog.Lookup(agentID); !ok {
		return "", durable.ErrAgentNotFound
	}
	id := req.SessionID
	if id == "" {
		id = durable.SessionID(uuid.NewString())
	}
	seed, err := durable.EncodeUserState(req.State)
	if err != nil {
		return "", err
	}
	_, err = r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        string(id),
		TaskQueue: r.taskQueue,
	}, SessionWorkflow, WorkflowInput{
		SessionID:           id,
		AgentID:             agentID,
		MCPServers:          req.MCPServers,
		Mounts:              req.Mounts,
		TurnLocalityTimeout: r.turnLocalityTimeout,
		State:               seed,
	})
	if err != nil {
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
	if err := r.client.SignalWorkflow(ctx, string(id), "", name, arg); err != nil {
		return durable.ErrSessionNotFound
	}
	return nil
}

// Prompt implements durable.Runtime.
func (r *Runtime) Prompt(ctx context.Context, sessionID durable.SessionID, msg durable.Prompt) error {
	encoded, err := durable.EncodeUserState(msg.State)
	if err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalPrompt, promptSignal{
		Text:        msg.Text,
		UserMessage: msg.UserMessage,
		AgentID:     msg.AgentID,
		MCPServers:  msg.MCPServers,
		Auth:        msg.Auth,
		State:       encoded,
	})
}

// Resume implements durable.Runtime.
func (r *Runtime) Resume(ctx context.Context, sessionID durable.SessionID, resume durable.Resume) error {
	encoded, err := durable.EncodeUserState(resume.State)
	if err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalResume, resumeSignal{
		Responses: resume.Responses,
		Auth:      resume.Auth,
		State:     encoded,
	})
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
	_ = r.signal(ctx, sessionID, signalClose, nil)
	_ = r.snapshots.Delete(ctx, sessionID)
	_ = r.fallback.CloseSession(ctx, sessionID)
	durable.ClearSessionVFS(r.catalog, sessionID)
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
		val, err := r.client.QueryWorkflow(ctx, string(sessionID), "", workflowstreams.OffsetQueryName)
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
	c := workflowstreams.NewClient(r.client, string(sessionID), workflowstreams.Options{})
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

func failFromWire(s string) error {
	for _, sent := range []error{
		tacklr.ErrModelRefused,
		tacklr.ErrMaxTokens,
		tacklr.ErrMaxTurnRequests,
		context.Canceled,
	} {
		if strings.Contains(s, sent.Error()) {
			return sent
		}
	}
	return errors.New(s)
}

// Children implements durable.Runtime.
func (r *Runtime) Children(ctx context.Context, parent durable.SessionID) ([]durable.SessionID, error) {
	if r.isClosed(parent) {
		return nil, durable.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(parent), "", queryChildren)
	if err != nil {
		return nil, durable.ErrSessionNotFound
	}
	var ids []durable.SessionID
	_ = val.Get(&ids)
	return ids, nil
}

// Status implements durable.Runtime.
func (r *Runtime) Status(ctx context.Context, id durable.SessionID) (durable.SessionStatus, error) {
	st := durable.SessionStatus{ID: id, State: durable.SessionUnknown}
	if r.isClosed(id) {
		return st, durable.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(id), "", queryStatus)
	if err != nil {
		return st, durable.ErrSessionNotFound
	}
	_ = val.Get(&st)
	return st, nil
}

var _ durable.Runtime = (*Runtime)(nil)
