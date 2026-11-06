// Package temporal is the client side of the Temporal runtime: Client
// implements session.Client with one Temporal workflow per session. It
// starts, signals and queries those workflows and reads their event
// streams; a worker from package session/temporal/worker runs them. This
// package imports no worker code, so a host that only talks to sessions
// does not build the harness.
package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/converter"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
)

// Client implements session.Client with one Temporal workflow per session.
type Client struct {
	client         client.Client
	taskQueue      string
	store          session.Store
	secrets        session.SecretStorage
	fallback       session.EventLog
	disableStreams bool
	live           Live
	// policy is the execution policy every session this client starts
	// carries in its workflow input.
	policy protocol.Input

	mu     sync.Mutex
	closed map[session.ID]struct{}
}

// Config is the host config for Open. Store, Secrets, Fallback,
// DisableStreams and TaskQueue name what the workers use too: give a
// worker's Config the same ones.
type Config struct {
	// TaskQueue is the queue the session workflows start on. Zero is
	// "tacklr".
	TaskQueue string
	// Store is the session record the workers save; Close deletes it. It
	// is required. Tokens never go here.
	Store session.Store
	// Secrets holds work-item credentials: Prompt and Resume write them,
	// Close deletes them. It is required. Tokens never enter event history.
	Secrets session.SecretStorage
	// Fallback is the EventLog Subscribe reads when DisableStreams is set,
	// and Head reads when the stream does not answer. Zero is an in-memory
	// log, which reaches no worker.
	Fallback session.EventLog
	// DisableStreams uses the fallback EventLog instead of Workflow Streams.
	DisableStreams bool
	// Live is how Subscribe reaches the live endpoint of the worker that
	// holds a session's harness, for its deltas. A zero Live gets whole
	// messages only.
	Live Live
	// WorkerQueueTimeout is how long a step waits to start on the worker
	// that holds its session's harness. A step that did not start in time
	// (that worker is gone) goes to another worker, which builds the harness
	// again. Zero is 10 seconds.
	WorkerQueueTimeout time.Duration
	// ActivityTimeout is Inference/Tool StartToCloseTimeout. Zero is 10 minutes.
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero is 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero is 3. 1 means no
	// retry. Any other activity error stops on the first attempt.
	ActivityAttempts int32
	// MaxConcurrentToolCalls is how many Tool steps of one reply run at
	// once; the rest start as those finish. Zero is 32.
	MaxConcurrentToolCalls int
	// MaxChildren is how many children (specialists and jobs) a session
	// runs at once; a start past it is a correction for the model. Zero is
	// 1,000, under Temporal's default limit of 2,000.
	MaxChildren int
	// ContinueAsNewEvents, when > 0, continues a session as new once its
	// history reaches that many events, at an idle point. Zero follows
	// Temporal's own suggestion (GetContinueAsNewSuggested).
	ContinueAsNewEvents int
}

// Open returns a Client over c. It panics without c, Store or Secrets.
func Open(c client.Client, cfg Config) *Client {
	switch {
	case c == nil:
		panic("temporal: Client is required")
	case cfg.Store == nil:
		panic("temporal: Store is required")
	case cfg.Secrets == nil:
		panic("temporal: Secrets is required")
	}
	fallback := cfg.Fallback
	if fallback == nil {
		fallback = session.NewMemoryEventLog()
	}
	taskQueue := cfg.TaskQueue
	if taskQueue == "" {
		taskQueue = protocol.TaskQueue
	}
	return &Client{
		client:         c,
		taskQueue:      taskQueue,
		store:          cfg.Store,
		secrets:        cfg.Secrets,
		fallback:       fallback,
		disableStreams: cfg.DisableStreams,
		live:           cfg.Live,
		policy: protocol.Input{
			WorkerQueueTimeout:  cfg.WorkerQueueTimeout,
			ActivityTimeout:     cfg.ActivityTimeout,
			HeartbeatTimeout:    cfg.HeartbeatTimeout,
			ActivityAttempts:    cfg.ActivityAttempts,
			MaxToolCalls:        cfg.MaxConcurrentToolCalls,
			MaxChildren:         cfg.MaxChildren,
			ContinueAsNewEvents: cfg.ContinueAsNewEvents,
		},
		closed: make(map[session.ID]struct{}),
	}
}

// CreateSession implements session.Client. A worker name no worker
// registered fails the session when it runs.
func (r *Client) CreateSession(ctx context.Context, req session.CreateSession) (session.ID, error) {
	if req.Worker != "" && req.Specialist != "" {
		return "", fmt.Errorf("%w: specialist and worker are exclusive", session.ErrInvalid)
	}
	id := req.SessionID
	if id == "" {
		id = session.ID(uuid.NewString())
	}
	seed, err := adapter.EncodeState(req.State)
	if err != nil {
		return "", err
	}
	in := r.policy
	in.SessionID = id
	in.MCPServers = session.DurableMCPServers(req.MCPServers)
	in.Mounts = req.Mounts
	in.State = seed
	in.Parent = req.Parent
	in.Specialist = req.Specialist
	in.Worker = req.Worker
	_, err = r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        string(id),
		TaskQueue: r.taskQueue,
	}, protocol.Workflow, in)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *Client) markClosed(id session.ID) {
	r.mu.Lock()
	r.closed[id] = struct{}{}
	r.mu.Unlock()
}

func (r *Client) isClosed(id session.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.closed[id]
	return ok
}

// wake sends one session.Wake. All wakes share protocol.SignalWake so the
// workflow sees them in the order they were sent.
func (r *Client) wake(ctx context.Context, id session.ID, wake session.Wake) error {
	if wake.Kind != session.WakeClose && r.isClosed(id) {
		return session.ErrNotFound
	}
	return missing(r.client.SignalWorkflow(ctx, string(id), "", protocol.SignalWake, wake))
}

// missing is err with Temporal's not-found (no such workflow) as
// session.ErrNotFound; any other error stays what it is.
func missing(err error) error {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return session.ErrNotFound
	}
	return err
}

// Prompt implements session.Client.
func (r *Client) Prompt(ctx context.Context, sessionID session.ID, msg session.Prompt) error {
	encoded, err := adapter.EncodeState(msg.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, session.Secrets{Auth: msg.Auth}); err != nil {
		return err
	}
	return r.wake(ctx, sessionID, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{
		Text:        msg.Text,
		UserMessage: msg.UserMessage,
		MCPServers:  session.DurableMCPServers(msg.MCPServers),
		Auth:        msg.Auth.WithoutSecrets(),
		State:       encoded,
	}})
}

// Resume implements session.Client.
func (r *Client) Resume(ctx context.Context, sessionID session.ID, resume session.Resume) error {
	encoded, err := adapter.EncodeState(resume.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, session.Secrets{Auth: resume.Auth}); err != nil {
		return err
	}
	return r.wake(ctx, sessionID, session.Wake{Kind: session.WakeResume, Resume: session.Resume{
		Responses: resume.Responses,
		Auth:      resume.Auth.WithoutSecrets(),
		State:     encoded,
	}})
}

// Cancel implements session.Client.
func (r *Client) Cancel(ctx context.Context, sessionID session.ID) error {
	return r.wake(ctx, sessionID, session.Wake{Kind: session.WakeCancel})
}

// Close implements session.Client. The session's workflow deletes what its
// harness saved (its messages in the brain) as it closes; Close deletes the
// session's credentials, its jobs' credentials, its record and its log.
// Every step runs even when an earlier one fails, and the failures are
// returned together. A part already gone (session.ErrNotFound) is not a
// failure, so Close is safe to repeat.
func (r *Client) Close(ctx context.Context, sessionID session.ID) error {
	kids, err := r.jobIDs(ctx, sessionID)
	errs := []error{err}
	r.markClosed(sessionID)
	errs = append(errs, r.wake(ctx, sessionID, session.Wake{Kind: session.WakeClose}))
	for _, id := range append(kids, sessionID) {
		errs = append(errs, r.secrets.Delete(ctx, id))
	}
	errs = append(errs, r.store.Delete(ctx, sessionID), r.fallback.CloseSession(ctx, sessionID))
	return errors.Join(slices.DeleteFunc(errs, func(err error) bool { return errors.Is(err, session.ErrNotFound) })...)
}

type sub struct {
	ch     <-chan session.Event
	cancel context.CancelFunc
	// err is set before ch closes, so a read after Events closed sees it.
	err error
}

func (s *sub) Events() <-chan session.Event { return s.ch }

func (s *sub) Err() error { return s.err }

func (s *sub) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Head implements session.Client. When Workflow Streams is on, this is the
// stream's next offset so Subscribe(after Head) skips prior-turn events.
func (r *Client) Head(ctx context.Context, sessionID session.ID) (session.Seq, error) {
	if !r.disableStreams {
		val, err := r.client.QueryWorkflow(ctx, string(sessionID), "", workflowstreams.OffsetQueryName)
		if err == nil {
			var n int64
			if err := val.Get(&n); err == nil && n >= 0 {
				return session.Seq(n), nil //nolint:gosec // G115: stream offsets are well below MaxUint64
			}
		}
	}
	return r.fallback.Head(ctx, sessionID)
}

// Subscribe implements session.Client: the session's durable events, from
// its stream or the fallback log, with its deltas from the live endpoint
// of the worker that holds its harness between them.
func (r *Client) Subscribe(ctx context.Context, sessionID session.ID, after session.Seq) (session.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	s := &sub{cancel: cancel}
	ch := make(chan session.Event, 64)
	if r.disableStreams {
		src, err := r.fallback.Subscribe(subCtx, sessionID, after)
		if err != nil {
			cancel()
			return nil, err
		}
		go func() {
			defer close(ch)
			for ev := range src {
				if !deliver(subCtx, ch, ev) {
					return
				}
			}
		}()
	} else {
		c := workflowstreams.NewClient(r.client, string(sessionID), workflowstreams.Options{})
		dc := converter.GetDefaultDataConverter()
		go func() {
			defer close(ch)
			defer func() { _ = c.Close(subCtx) }()
			s.err = r.stream(subCtx, c, dc, sessionID, after, ch)
		}()
	}
	var deltas <-chan session.Event
	if len(r.live.Key) > 0 {
		deltas = r.deltas(subCtx, sessionID)
	}
	s.ch = session.Merge(subCtx, ch, deltas)
	return s, nil
}

// stream delivers the session's events from after into ch until ctx ends
// (nil) or delivery fails (that error).
func (r *Client) stream(ctx context.Context, c *workflowstreams.Client, dc converter.DataConverter, id session.ID, after session.Seq, ch chan<- session.Event) error {
	off := int64(after) //nolint:gosec // G115: EventLog seq is well below MaxInt64
	for item, err := range c.Subscribe(ctx, workflowstreams.SubscribeOptions{
		Topics:     []string{session.TopicEvents},
		FromOffset: off,
	}) {
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", id, err)
		}
		var it protocol.StreamItem
		if err := dc.FromPayload(item.Data, &it); err != nil {
			return fmt.Errorf("subscribe %s: decode event: %w", id, err)
		}
		ev, err := r.resolve(ctx, id, it)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", id, err)
		}
		if !deliver(ctx, ch, ev) {
			return nil
		}
	}
	return nil
}

// resolve is the event an item stands for: a tool result's content is read
// back from its call's body in the Store. A call whose body is gone (its
// turn ended, or its session was deleted) took its content with it.
func (r *Client) resolve(ctx context.Context, id session.ID, it protocol.StreamItem) (session.Event, error) {
	if it.Call == "" {
		return it.Event, nil
	}
	bodies, err := r.store.Calls(ctx, id, []string{it.Call})
	if err != nil {
		return it.Event, err
	}
	ev := it.Event
	if raw, ok := bodies[it.Call]; ok {
		if ev.Content, err = tacklr.ResultText(raw); err != nil {
			return ev, fmt.Errorf("tool call %s: %w", it.Call, err)
		}
	}
	return ev, nil
}

func deliver(ctx context.Context, ch chan<- session.Event, ev session.Event) bool {
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// jobIDs are the ids of the jobs parent runs, in start order.
func (r *Client) jobIDs(ctx context.Context, parent session.ID) ([]session.ID, error) {
	if r.isClosed(parent) {
		return nil, session.ErrNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(parent), "", protocol.QueryJobs)
	if err != nil {
		return nil, missing(err)
	}
	var ids []session.ID
	_ = val.Get(&ids)
	return ids, nil
}

// Jobs implements session.Client.
func (r *Client) Jobs(ctx context.Context, parent session.ID) ([]session.Status, error) {
	ids, err := r.jobIDs(ctx, parent)
	if err != nil {
		return nil, err
	}
	out := make([]session.Status, 0, len(ids))
	for _, id := range ids {
		st, err := r.Status(ctx, id)
		if errors.Is(err, session.ErrNotFound) {
			// The child ended and is gone since jobIDs listed it.
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// Status implements session.Client.
func (r *Client) Status(ctx context.Context, id session.ID) (session.Status, error) {
	st := session.Status{ID: id, State: session.Unknown}
	if r.isClosed(id) {
		return st, session.ErrNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(id), "", protocol.QueryStatus)
	if err != nil {
		return st, missing(err)
	}
	_ = val.Get(&st)
	return st, nil
}
