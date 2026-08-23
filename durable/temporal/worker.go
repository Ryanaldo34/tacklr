package temporal

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
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

// NewWorker returns a Temporal worker with EnableSessionWorker and the
// Inference/Tool activities plus SessionWorkflow.
func NewWorker(c client.Client, taskQueue string, opts WorkerOptions) worker.Worker {
	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1000,
	})
	if opts.Snapshots == nil {
		opts.Snapshots = inprocess.NewMemorySnapshot()
	}
	if opts.Projection == nil {
		opts.Projection = vfs.FuseProjection{}
	}
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
