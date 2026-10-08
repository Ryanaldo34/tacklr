// Package temporal is the Temporal adapter for session.Runtime.
// Hosts use Dial and Open. Zero Snapshots and Secrets use in-memory
// stores kept on the runtime. StartWorker registers SessionWorkflow
// and the turn activities against those stores.
package temporal

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/telemetry"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Runtime implements session.Runtime with one Temporal workflow per session.
type Runtime struct {
	client              client.Client
	taskQueue           string
	agent               tacklr.AgentOptions
	fallback            session.EventLog
	snapshots           session.SnapshotStore
	disableStreams      bool
	turnLocalityTimeout time.Duration
	activityTimeout     time.Duration
	heartbeatTimeout    time.Duration
	activityAttempts    int32
	secrets             session.SecretStorage
	projection          vfs.Projection
	jobs                map[string]session.JobHandler

	mu     sync.Mutex
	closed map[session.SessionID]struct{}
}

const (
	defaultActivityTimeout  = 10 * time.Minute
	defaultHeartbeatTimeout = 30 * time.Second
	defaultActivityAttempts = 3
)

// Config is the host config for Open. StartWorker uses the stores Open kept.
type Config struct {
	// Agent is the one agent this runtime runs. Specialists on Agent are
	// assistants it can spawn. Jobs are named handlers, not agents.
	Agent     tacklr.AgentOptions
	TaskQueue string
	// Snapshots is the session record. Zero uses an in-memory store.
	// Tokens never go here. StartWorker uses this same store.
	Snapshots session.SnapshotStore
	Fallback  session.EventLog
	// Projection mounts the turn tree on the host kernel.
	// Nil leaves the MountSession in-process.
	Projection vfs.Projection
	// DisableStreams uses the fallback EventLog instead of Workflow Streams.
	DisableStreams bool
	// TurnLocality, when > 0, pins a turn's activities to one worker.
	TurnLocality time.Duration
	// ActivityTimeout is Inference/Tool StartToCloseTimeout. Zero is 10 minutes.
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero is 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero is 3. 1 means no
	// retry. Any other activity error stops on the first attempt.
	ActivityAttempts int32
	// Secrets holds work-item credentials for activities. Zero uses an
	// in-memory store. StartWorker uses this same store. Tokens never
	// enter event history.
	Secrets session.SecretStorage
	// Jobs are named background workers Schedule can start. A specialist
	// of the same name takes precedence.
	Jobs map[string]session.JobHandler
}

func (c Config) queue() string {
	if c.TaskQueue == "" {
		return "tacklr"
	}
	return c.TaskQueue
}

func applyDefaults(cfg Config) Config {
	if cfg.Snapshots == nil {
		cfg.Snapshots = session.NewMemorySnapshot()
	}
	if cfg.Secrets == nil {
		cfg.Secrets = session.NewMemorySecretStorage()
	}
	return cfg
}

func (c Config) eventLog() session.EventLog {
	if c.Fallback != nil {
		return c.Fallback
	}
	return session.NewMemoryEventLog()
}

// Open constructs a Temporal Runtime. Zero Snapshots and Secrets use
// in-memory stores kept on the runtime. StartWorker registers the worker
// against those same stores.
func Open(c client.Client, cfg Config) *Runtime {
	if c == nil {
		panic("temporal: Client is required")
	}
	cfg = applyDefaults(cfg)
	return &Runtime{
		client:              c,
		taskQueue:           cfg.queue(),
		agent:               cfg.Agent,
		fallback:            cfg.eventLog(),
		snapshots:           cfg.Snapshots,
		disableStreams:      cfg.DisableStreams,
		turnLocalityTimeout: cfg.TurnLocality,
		activityTimeout:     cmp.Or(cfg.ActivityTimeout, defaultActivityTimeout),
		heartbeatTimeout:    cmp.Or(cfg.HeartbeatTimeout, defaultHeartbeatTimeout),
		activityAttempts:    cmp.Or(cfg.ActivityAttempts, defaultActivityAttempts),
		secrets:             cfg.Secrets,
		projection:          cfg.Projection,
		jobs:                cfg.Jobs,
		closed:              make(map[session.SessionID]struct{}),
	}
}

// CreateSession implements session.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req session.CreateSession) (session.SessionID, error) {
	if req.Worker != "" && req.Specialist != "" {
		return "", fmt.Errorf("specialist and worker are exclusive: %w", tacklr.ErrInvalid)
	}
	if req.Worker != "" {
		if _, ok := r.jobs[req.Worker]; !ok {
			return "", fmt.Errorf("%w: %s", tacklr.ErrNotFound, req.Worker)
		}
	}
	id := req.SessionID
	if id == "" {
		id = session.SessionID(uuid.NewString())
	}
	seed, err := adapter.EncodeUserState(req.State)
	if err != nil {
		return "", err
	}
	_, err = r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        string(id),
		TaskQueue: r.taskQueue,
	}, SessionWorkflow, workflowInput{
		SessionID:           id,
		MCPServers:          mcp.DurableConfigs(req.MCPServers),
		Mounts:              req.Mounts,
		TurnLocalityTimeout: r.turnLocalityTimeout,
		ActivityTimeout:     r.activityTimeout,
		HeartbeatTimeout:    r.heartbeatTimeout,
		ActivityAttempts:    r.activityAttempts,
		State:               seed,
		Parent:              req.Parent,
		Specialist:          req.Specialist,
		Worker:              req.Worker,
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *Runtime) markClosed(id session.SessionID) {
	r.mu.Lock()
	r.closed[id] = struct{}{}
	r.mu.Unlock()
}

func (r *Runtime) isClosed(id session.SessionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.closed[id]
	return ok
}

func (r *Runtime) signal(ctx context.Context, id session.SessionID, name string, arg any) error {
	if name != signalClose && r.isClosed(id) {
		return session.ErrSessionNotFound
	}
	if err := r.client.SignalWorkflow(ctx, string(id), "", name, arg); err != nil {
		return session.ErrSessionNotFound
	}
	return nil
}

func (r *Runtime) Prompt(ctx context.Context, sessionID session.SessionID, msg session.Prompt) error {
	encoded, err := adapter.EncodeUserState(msg.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, session.Secrets{Auth: msg.Auth}); err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalPrompt, session.PromptIn{
		Text:        msg.Text,
		UserMessage: msg.UserMessage,
		MCPServers:  mcp.DurableConfigs(msg.MCPServers),
		Auth:        msg.Auth.WithoutSecrets(),
		State:       encoded,
	})
}

func (r *Runtime) Resume(ctx context.Context, sessionID session.SessionID, resume session.Resume) error {
	encoded, err := adapter.EncodeUserState(resume.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, session.Secrets{Auth: resume.Auth}); err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalResume, session.ResumeIn{
		Responses: resume.Responses,
		Auth:      resume.Auth.WithoutSecrets(),
		State:     encoded,
	})
}

// Cancel implements session.Runtime.
func (r *Runtime) Cancel(ctx context.Context, sessionID session.SessionID) error {
	cancelLiveTurn(sessionID)
	return r.signal(ctx, sessionID, signalCancel, nil)
}

// Close implements session.Runtime.
func (r *Runtime) Close(ctx context.Context, sessionID session.SessionID) error {
	kids, _ := r.Children(ctx, sessionID)
	r.markClosed(sessionID)
	_ = r.signal(ctx, sessionID, signalClose, nil)
	for _, k := range kids {
		session.DeleteSessionMessages(ctx, r.agent, k)
		_ = r.secrets.Delete(ctx, k)
	}
	session.DeleteSessionMessages(ctx, r.agent, sessionID)
	_ = r.secrets.Delete(ctx, sessionID)
	_ = r.snapshots.Delete(ctx, sessionID)
	_ = r.fallback.CloseSession(ctx, sessionID)
	return nil
}

type sub struct {
	ch     <-chan tacklr.StreamEvent
	cancel context.CancelFunc
}

func (s *sub) Events() <-chan tacklr.StreamEvent { return s.ch }
func (s *sub) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Head implements session.Runtime. When Workflow Streams is on, this is the
// stream's next offset so Subscribe(after Head) skips prior-turn events.
func (r *Runtime) Head(ctx context.Context, sessionID session.SessionID) (session.Seq, error) {
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

// Subscribe implements session.Runtime.
func (r *Runtime) Subscribe(ctx context.Context, sessionID session.SessionID, after session.Seq) (session.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	if r.disableStreams {
		src, err := r.fallback.Subscribe(subCtx, sessionID, after)
		if err != nil {
			cancel()
			return nil, err
		}
		ch := make(chan tacklr.StreamEvent)
		go func() {
			defer close(ch)
			for ev := range src {
				if !deliver(subCtx, ch, ev) {
					return
				}
			}
		}()
		return &sub{ch: ch, cancel: cancel}, nil
	}
	c := workflowstreams.NewClient(r.client, string(sessionID), workflowstreams.Options{})
	ch := make(chan tacklr.StreamEvent, 64)
	dc := converter.GetDefaultDataConverter()
	go func() {
		defer close(ch)
		defer func() { _ = c.Close(subCtx) }()
		off := int64(after) //nolint:gosec // G115: EventLog seq is well below MaxInt64
		for item, err := range c.Subscribe(subCtx, workflowstreams.SubscribeOptions{
			Topics:     []string{session.TopicEvents},
			FromOffset: off,
		}) {
			if err != nil {
				return
			}
			var ev tacklr.StreamEvent
			if err := dc.FromPayload(item.Data, &ev); err != nil {
				return
			}
			if !deliver(subCtx, ch, ev) {
				return
			}
		}
	}()
	return &sub{ch: ch, cancel: cancel}, nil
}

func deliver(ctx context.Context, ch chan<- tacklr.StreamEvent, ev tacklr.StreamEvent) bool {
	if ev.Error == nil && ev.Fail != "" {
		ev.Error = failFromWire(ev.Fail)
	}
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
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

// Children implements session.Runtime.
func (r *Runtime) Children(ctx context.Context, parent session.SessionID) ([]session.SessionID, error) {
	if r.isClosed(parent) {
		return nil, session.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(parent), "", queryChildren)
	if err != nil {
		return nil, session.ErrSessionNotFound
	}
	var ids []session.SessionID
	_ = val.Get(&ids)
	return ids, nil
}

// Jobs implements session.Runtime.
func (r *Runtime) Jobs(ctx context.Context, parent session.SessionID) ([]session.SessionStatus, error) {
	ids, err := r.Children(ctx, parent)
	if err != nil {
		return nil, err
	}
	out := make([]session.SessionStatus, 0, len(ids))
	for _, id := range ids {
		st, err := r.Status(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

// Status implements session.Runtime.
func (r *Runtime) Status(ctx context.Context, id session.SessionID) (session.SessionStatus, error) {
	st := session.SessionStatus{ID: id, State: session.SessionUnknown}
	if r.isClosed(id) {
		return st, session.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(id), "", queryStatus)
	if err != nil {
		return st, session.ErrSessionNotFound
	}
	_ = val.Get(&st)
	return st, nil
}

// Dial is client.Dial with Temporal's OpenTelemetry v2 plugin prepended.
// It installs a replay-safe tracer on the process. When telemetry.Init has
// already run, that tracer keeps the same export configuration.
func Dial(opts client.Options) (client.Client, error) {
	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		if !telemetry.TracerInstalled() {
			otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider())
		} else if err := telemetry.ReinstallTracer(context.Background(), func(o ...sdktrace.TracerProviderOption) (trace.TracerProvider, func(context.Context) error) {
			tp := temporalotel.NewReplaySafeTracerProvider(o...)
			return tp, tp.Shutdown
		}); err != nil {
			return nil, err
		}
	}
	plugin, err := temporalotel.NewPlugin(temporalotel.PluginOptions{})
	if err != nil {
		return nil, err
	}
	opts.Plugins = append([]client.Plugin{plugin}, opts.Plugins...)
	return client.Dial(opts)
}

// StartWorker returns a Temporal worker for this runtime. It registers
// SessionWorkflow and the turn activities against the runtime's snapshot
// store, secret store, and agent.
func (r *Runtime) StartWorker() worker.Worker {
	w := worker.New(r.client, r.taskQueue, worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1000,
	})
	acts := &activities{
		Agent:          r.agent,
		Snapshots:      r.snapshots,
		Projection:     r.projection,
		Fallback:       r.fallback,
		DisableStreams: r.disableStreams,
		Secrets:        r.secrets,
		Jobs:           r.jobs,
	}
	w.RegisterWorkflow(SessionWorkflow)
	w.RegisterActivity(acts)
	return w
}
