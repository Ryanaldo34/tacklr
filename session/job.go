package session

import "context"

// ChildResult is a blocking specialist's output for the call that waited
// on it. Failed is a child that failed: Output is then its error.
type JobResult struct {
	Output string `json:"output"`
	Failed bool   `json:"failed,omitempty"`
}

// JobHandler runs a named background job: the body of a job session that a
// tool starts with CallContext.Schedule. A runtime's worker registers
// handlers by name (its Config.Jobs).
type JobHandler func(ctx context.Context, task string) (string, error)
