package temporal

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/vfs"
)

// WorkerOptions configures NewWorker.
type WorkerOptions struct {
	Catalog        durable.Catalog
	Snapshots      durable.SnapshotStore
	Projection     vfs.Projection
	Fallback       durable.EventLog
	DisableStreams bool
}

// NewWorker returns a Temporal worker with EnableSessionWorker and
// SessionWorkflow plus Inference, Tool, and CommitToolOutput activities.
// Worker sessions are created only when WorkflowInput.TurnLocalityTimeout
// is set (Runtime option WithTurnLocality). Create the client with
// Dial so Temporal's OTEL v2 plugin propagates trace context.
func NewWorker(c client.Client, taskQueue string, opts WorkerOptions) worker.Worker {
	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1000,
	})
	acts := &Activities{
		Catalog:        opts.Catalog,
		Snapshots:      opts.Snapshots,
		Projection:     opts.Projection,
		Fallback:       opts.Fallback,
		DisableStreams: opts.DisableStreams,
	}
	w.RegisterWorkflow(SessionWorkflow)
	w.RegisterActivity(acts)
	return w
}
