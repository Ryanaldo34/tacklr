package telemetry

import (
	"context"

	"github.com/ryanaldo34/tacklr/brain"
)

// BrainObserver maps brain.Observer to tacklr.brain OTel spans/metrics.
type BrainObserver struct{}

// NewBrainObserver returns an Observer for brain.WithObserver.
func NewBrainObserver() brain.Observer { return BrainObserver{} }

// StartOp implements brain.Observer.
func (BrainObserver) StartOp(ctx context.Context, op brain.Op) (context.Context, brain.OpSpan) {
	ctx, span := StartBrainSpan(ctx, string(op))
	return ctx, brainOpSpan{span: span}
}

type brainOpSpan struct {
	span *BrainSpan
}

func (s brainOpSpan) End(hits int, degrade brain.DegradeMode, err error) {
	s.span.End(hits, degrade.String(), err)
}
