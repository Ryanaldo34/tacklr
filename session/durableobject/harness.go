package durableobject

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"sync"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// harnessState is the object's one harness. It is built on the first step
// that needs it, and again when what it is built from changes (the
// session's MCP servers) or a failed Commit left it ahead of the store.
// snap keeps it at the store's revision.
type harnessState struct {
	mu   sync.Mutex
	h    *tacklr.Harness
	key  string
	snap adapter.Snapshot
}

// read is the harness at the store's revision, for a step that reads
// committed state (Infer, the tool calls of a reply).
func (hs *harnessState) read(ctx context.Context, e *executor) (*tacklr.Harness, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if err := hs.build(ctx, e); err != nil {
		return nil, err
	}
	return hs.h, hs.snap.Sync(ctx, e.o.snapshots, e.init.SessionID, hs.h)
}

// commit is the Commit step (adapter.Snapshot.Commit) over store, which
// stages its writes for the step's one write; its events go to the step's.
func (hs *harnessState) commit(ctx context.Context, e *executor, store session.Store, base session.Revision, in tacklr.CommitIn) (session.Revision, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if err := hs.build(ctx, e); err != nil {
		return "", err
	}
	out, stop := tacklr.PipeEvents(e.emitter(&e.events))
	defer stop()
	return hs.snap.Commit(ctx, store, e.init.SessionID, hs.h, base, in, out)
}

// build builds or rebuilds the harness as needed. Call it with hs.mu held.
func (hs *harnessState) build(ctx context.Context, e *executor) error {
	t := e.turn
	// Strings and slices of them: the key always encodes.
	key, _ := json.Marshal(struct {
		Specialist string
		MCP        []session.MCPServer
	}{t.Specialist, t.MCP})
	if hs.h != nil && (hs.key != string(key) || hs.snap.Stale()) {
		hs.closeLocked()
	}
	if hs.h != nil {
		return nil
	}
	h, err := e.build(ctx)
	if err != nil {
		return err
	}
	hs.h, hs.key = h, string(key)
	return nil
}

// close closes the harness, when the session ends.
func (hs *harnessState) close() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.closeLocked()
}

func (hs *harnessState) closeLocked() {
	if hs.h != nil {
		hs.h.Close()
	}
	hs.h, hs.key, hs.snap = nil, "", adapter.Snapshot{}
}

// build is a harness for the session: the agent, or its specialist, with
// the session's MCP servers and the deployment's job names. A durable
// object has no workspace.
func (e *executor) build(ctx context.Context) (*tacklr.Harness, error) {
	spec := e.o.cfg.Agent
	if s := e.turn.Specialist; s != "" {
		over, err := adapter.OverlaySpecialist(spec, s)
		if err != nil {
			return nil, err
		}
		spec = over
	}
	opts := tacklr.BindTurn(spec, string(e.init.SessionID), slices.Sorted(maps.Keys(e.o.cfg.Jobs)))
	if len(e.turn.MCP) > 0 {
		opts.MCPConfigs = append(slices.Clone(spec.MCPConfigs), e.turn.MCP...)
	}
	return tacklr.NewHarness(ctx, opts)
}
