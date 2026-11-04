package adapter

import (
	"context"
	"fmt"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// RunTool is the Tool step on h. The outcome's record (its result, writes,
// park and layers) goes to store under the call key, and the outcome keeps
// only that it is stored: the step's result, which a durable runtime keeps
// in its history, stays small. A call's earlier record, from a run that
// parked or waited on a child, is loaded back first.
func RunTool(ctx context.Context, store session.Store, id session.ID, h *tacklr.Harness, run tacklr.ToolRun, out chan session.Event) (tacklr.CallOutcome, error) {
	if p := run.Prior; p != nil && p.Stored {
		prior := []tacklr.CallOutcome{*p}
		if err := LoadRecords(ctx, store, id, prior); err != nil {
			return tacklr.CallOutcome{Call: run.Call}, err
		}
		run.Prior = &prior[0]
	}
	o, err := h.RunTool(ctx, run, out)
	if err != nil {
		return o, err
	}
	result, err := tacklr.RecordResult(o.Record)
	if err != nil {
		return o, err
	}
	if err := store.PutOutcome(ctx, id, o.Call.Key(), session.Outcome{Result: result, Record: o.Record}); err != nil {
		return o, err
	}
	o.Record, o.Stored = nil, true
	return o, nil
}

// LoadRecords puts each stored outcome's record back from store.
func LoadRecords(ctx context.Context, store session.Store, id session.ID, outcomes []tacklr.CallOutcome) error {
	var keys []string
	for _, o := range outcomes {
		if o.Stored {
			keys = append(keys, o.Call.Key())
		}
	}
	if len(keys) == 0 {
		return nil
	}
	records, err := store.Outcomes(ctx, id, keys)
	if err != nil {
		return err
	}
	for i, o := range outcomes {
		if !o.Stored {
			continue
		}
		rec, ok := records[o.Call.Key()]
		if !ok {
			return fmt.Errorf("outcome of tool call %s: %w", o.Call.Key(), session.ErrNotFound)
		}
		outcomes[i].Record, outcomes[i].Stored = rec.Record, false
	}
	return nil
}
