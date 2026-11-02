package adapter

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

// ConstructTurn opens VFS trees and builds a TurnManager. On NewTurnManager
// failure the trees are closed.
func ConstructTurn(ctx context.Context, agent tacklr.AgentOptions, threadID string, bindings []vfs.Binding, proj vfs.Projection, extraMCP []mcp.MCPConfig) (*tacklr.TurnManager, *vfs.MountSession, error) {
	ms, err := OpenTurnVFS(ctx, threadID, agent, bindings, proj)
	if err != nil {
		return nil, nil, err
	}
	opts := tacklr.BindTurn(agent, threadID, ms)
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]mcp.MCPConfig(nil), agent.MCPConfigs...), extraMCP...)
	}
	h, err := tacklr.NewTurnManager(ctx, opts)
	if err != nil {
		CloseTurnVFS(ms)
		return nil, nil, err
	}
	return h, ms, nil
}

// RestoreTurn loads the session's committed state into h, if a snapshot
// exists, and returns its revision.
func RestoreTurn(ctx context.Context, store session.SnapshotStore, id session.SessionID, h *tacklr.TurnManager) (session.Revision, error) {
	cp, rev, err := store.Load(ctx, id)
	switch {
	case err == nil:
		return rev, h.RestoreCheckpoint(cp)
	case errors.Is(err, session.ErrSessionNotFound):
		return "", nil
	default:
		return "", err
	}
}

// CommitTurn is the Commit step on a harness restored at rev: it applies in
// and saves, unless the snapshot is already past base (an earlier attempt
// of this Commit saved), and returns the snapshot's revision.
func CommitTurn(ctx context.Context, store session.SnapshotStore, id session.SessionID, h *tacklr.TurnManager, rev, base session.Revision, in tacklr.CommitIn, out chan tacklr.StreamEvent) (session.Revision, error) {
	if rev != base {
		return rev, nil
	}
	if err := h.Commit(ctx, in, out); err != nil {
		return "", err
	}
	cp, err := h.Checkpoint()
	if err != nil {
		return "", err
	}
	return store.Save(ctx, id, cp, rev)
}

// AbandonTurn closes a harness and its turn-scoped trees after a failed construct.
func AbandonTurn(h *tacklr.TurnManager, workspace *vfs.MountSession) {
	if h != nil {
		h.Close()
	}
	CloseTurnVFS(workspace)
}
