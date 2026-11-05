package adapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// RunCall is the Tool step on h. A call that ran before gets its body back
// from store first. After the run its body goes to store under its key and
// leaves the call, so the call a runtime keeps (in its history or its
// journal) stays small.
func RunCall(ctx context.Context, store session.Store, id session.ID, h *tacklr.Harness, c tacklr.Call, out chan session.Event) (tacklr.Call, error) {
	calls := []tacklr.Call{c}
	if err := LoadBodies(ctx, store, id, calls); err != nil {
		return c, err
	}
	c, err := h.RunCall(ctx, calls[0], out)
	if err != nil {
		return c, err
	}
	body, _ := json.Marshal(c.Body) // a body always encodes: its writes are JSON and its park a registered interrupt
	if err := store.PutCall(ctx, id, c.Key(), body); err != nil {
		return c, err
	}
	c.Body = nil
	return c, nil
}

// LoadBodies puts back from store the body of each call that ran.
func LoadBodies(ctx context.Context, store session.Store, id session.ID, calls []tacklr.Call) error {
	var keys []string
	for _, c := range calls {
		if c.Body == nil && !c.Pending() {
			keys = append(keys, c.Key())
		}
	}
	if len(keys) == 0 {
		return nil
	}
	bodies, err := store.Calls(ctx, id, keys)
	if err != nil {
		return err
	}
	for i, c := range calls {
		if c.Body != nil || c.Pending() {
			continue
		}
		raw, ok := bodies[c.Key()]
		if !ok {
			return fmt.Errorf("tool call %s: %w", c.Key(), session.ErrNotFound)
		}
		calls[i].Body = new(tacklr.CallBody)
		if err := json.Unmarshal(raw, calls[i].Body); err != nil {
			return fmt.Errorf("tool call %s: %w", c.Key(), err)
		}
	}
	return nil
}
