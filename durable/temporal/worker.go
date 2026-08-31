package temporal

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/vfs"
)

// NewWorker returns a Temporal worker with EnableSessionWorker and
// SessionWorkflow plus Inference, Tool, CommitToolOutput, and EmitEvent
// activities.
// Pass the same Config as New. Worker sessions exist only when
// Config.TurnLocality > 0.
func NewWorker(c client.Client, cfg Config) worker.Worker {
	if cfg.Secrets == nil {
		panic("temporal: Secrets is required")
	}
	w := worker.New(c, cfg.queue(), worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1000,
	})
	proj := cfg.Projection
	if proj == nil {
		proj = vfs.FuseProjection{}
	}
	fallback := cfg.Fallback
	if fallback == nil {
		fallback = cfg.memoryLog()
	}
	acts := &Activities{
		Catalog:        cfg.Catalog,
		Snapshots:      cfg.snaps(),
		Projection:     proj,
		Fallback:       fallback,
		DisableStreams: cfg.DisableStreams,
		Secrets:        cfg.Secrets,
	}
	w.RegisterWorkflow(SessionWorkflow)
	w.RegisterActivity(acts)
	return w
}
