// Package worker runs tacklr sessions on Temporal: it registers the session
// workflow and its turn activities, and keeps each session's harness (VFS,
// MCP connections, tools) between steps. A host that only starts and talks
// to sessions uses package session/temporal instead.
package worker

import (
	"cmp"
	"context"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"
	"github.com/ryanaldo34/tacklr/vfs"
)

// JobHandler runs a named background job. A worker registers handlers by
// name on Config.Jobs; a tool's Schedule starts one.
type JobHandler func(ctx context.Context, task string) (string, error)

// The defaults of a session's step policy, which its client sends in the
// workflow input; zero there means these.
const (
	defaultActivityTimeout  = 10 * time.Minute
	defaultHeartbeatTimeout = 30 * time.Second
	defaultActivityAttempts = 3
	// defaultWorkerQueueTimeout is how long a step waits to start on the
	// worker that holds its session's harness before it goes to another.
	defaultWorkerQueueTimeout = 10 * time.Second
	// defaultMaxToolCalls bounds the Tool steps of one reply that run at
	// once.
	defaultMaxToolCalls = 32
	// defaultMaxChildren keeps a session's running children well under
	// Temporal's default limit of 2,000 incomplete child workflows.
	defaultMaxChildren = 1000
)

const (
	// defaultHeartbeatThrottle bounds how long a cancel takes to reach a
	// running step.
	defaultHeartbeatThrottle = 2 * time.Second
	defaultHarnessIdle       = 10 * time.Minute
	// defaultMaxHarnesses bounds the harnesses one worker keeps: each holds a
	// window, MCP connections (a process per stdio server), and a VFS that
	// may be a FUSE mount.
	defaultMaxHarnesses = 256
)

// Config is the host config for New. Store, Secrets, Fallback,
// DisableStreams and TaskQueue must be the ones the session clients use.
type Config struct {
	// Agent is the one agent the sessions run. Specialists on Agent are
	// assistants it can spawn. Jobs are named handlers, not agents.
	Agent tacklr.AgentOptions
	// TaskQueue is the queue the session workflows start on. Zero is
	// "tacklr".
	TaskQueue string
	// Store is the session record the steps save. It is required. Tokens
	// never go here.
	Store session.Store
	// Secrets holds work-item credentials for the steps. It is required.
	// Tokens never enter event history.
	Secrets session.SecretStorage
	// Fallback is the EventLog the steps write every event to. Zero is an
	// in-memory log, which reaches no client.
	Fallback session.EventLog
	// DisableStreams sends events only to Fallback, not Workflow Streams.
	DisableStreams bool
	// Projection mounts the turn tree on the host kernel.
	// Nil leaves the MountSession in-process.
	Projection vfs.Projection
	// Jobs are named background workers Schedule can start. A specialist
	// of the same name takes precedence.
	Jobs map[string]JobHandler
	// HarnessIdle is how long the worker keeps a session's harness after
	// its last step. Zero is 10 minutes.
	HarnessIdle time.Duration
	// MaxHarnesses is how many harnesses the worker keeps; past it, the
	// harness unused longest closes. Zero is 256.
	MaxHarnesses int
	// MaxConcurrentActivityExecutionSize caps the steps the worker runs at
	// once (Temporal worker.Options). Zero is Temporal's default.
	MaxConcurrentActivityExecutionSize int
	// HeartbeatThrottle is the longest a running step waits to send its
	// heartbeat (worker MaxHeartbeatThrottleInterval). A cancel reaches a
	// running step on a heartbeat's reply, so this bounds how long a cancel
	// takes; lower is faster and sends more heartbeats. Zero is 2 seconds.
	HeartbeatThrottle time.Duration
}

// New returns a Temporal worker that registers the session workflow and
// the turn activities against cfg. It panics without c, Store or Secrets.
func New(c client.Client, cfg Config) sdkworker.Worker {
	switch {
	case c == nil:
		panic("worker: Client is required")
	case cfg.Store == nil:
		panic("worker: Store is required")
	case cfg.Secrets == nil:
		panic("worker: Secrets is required")
	}
	fallback := cfg.Fallback
	if fallback == nil {
		fallback = session.NewMemoryEventLog()
	}
	taskQueue := cmp.Or(cfg.TaskQueue, protocol.TaskQueue)
	acts := &activities{
		Agent:          cfg.Agent,
		Store:          cfg.Store,
		Projection:     cfg.Projection,
		Fallback:       fallback,
		DisableStreams: cfg.DisableStreams,
		Secrets:        cfg.Secrets,
		Jobs:           cfg.Jobs,
		harnesses:      newHarnessCache(cmp.Or(cfg.HarnessIdle, defaultHarnessIdle), cmp.Or(cfg.MaxHarnesses, defaultMaxHarnesses)),
		queue:          taskQueue + "/" + uuid.NewString(),
	}
	opts := sdkworker.Options{
		MaxConcurrentActivityExecutionSize: cfg.MaxConcurrentActivityExecutionSize,
		MaxHeartbeatThrottleInterval:       cmp.Or(cfg.HeartbeatThrottle, defaultHeartbeatThrottle),
	}
	shared := sdkworker.New(c, taskQueue, opts)
	shared.RegisterWorkflowWithOptions(sessionWorkflow, workflow.RegisterOptions{Name: protocol.Workflow})
	shared.RegisterActivity(acts)
	own := sdkworker.New(c, acts.queue, opts)
	own.RegisterActivity(acts)
	return &stickyWorker{Worker: shared, own: own, harnesses: acts.harnesses}
}

// stickyWorker polls the shared task queue and this worker's own queue,
// where the steps of the sessions whose harnesses it holds arrive. Stopping it
// closes those harnesses.
type stickyWorker struct {
	sdkworker.Worker
	own       sdkworker.Worker
	harnesses *harnessCache
}

func (w *stickyWorker) Start() error {
	if err := w.own.Start(); err != nil {
		return err
	}
	if err := w.Worker.Start(); err != nil {
		w.own.Stop()
		return err
	}
	return nil
}

func (w *stickyWorker) Run(interruptCh <-chan any) error {
	if err := w.own.Start(); err != nil {
		return err
	}
	defer w.stopOwn()
	return w.Worker.Run(interruptCh)
}

func (w *stickyWorker) Stop() {
	w.Worker.Stop()
	w.stopOwn()
}

func (w *stickyWorker) stopOwn() {
	w.own.Stop()
	w.harnesses.CloseAll()
}
