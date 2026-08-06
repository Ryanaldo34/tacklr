package brain

import "context"

// Op is a closed enum for retrieval operations (matches telemetry label values).
// Keep in sync with telemetry.BrainOp* closed enum.
type Op string

const (
	OpSearch      Op = "search"
	OpFindExact   Op = "find_exact"
	OpFindObjects Op = "find_objects"
	OpFindLinks   Op = "find_links"
	OpContinue    Op = "continue"
	OpExpand      Op = "expand"
	OpExpandMany  Op = "expand_many"
)

// OpSpan ends one retrieval operation. Implementations must be safe for a single End call.
type OpSpan interface {
	End(hits int, degrade DegradeMode, err error)
}

// Observer records retrieval ops. Nil/noop is fine for tests and offline hosts.
type Observer interface {
	StartOp(ctx context.Context, op Op) (context.Context, OpSpan)
}

type noopObserver struct{}

func (noopObserver) StartOp(ctx context.Context, _ Op) (context.Context, OpSpan) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End(int, DegradeMode, error) {}

func observerOrNoop(o Observer) Observer {
	if o == nil {
		return noopObserver{}
	}
	return o
}
