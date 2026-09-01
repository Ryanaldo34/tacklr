package durable

import (
	"context"
)

// Revision is the SnapshotStore compare-and-swap token for one session row.
// The zero value means no row exists yet; the next Save creates it.
type Revision string

// SnapshotStore is the session record: what the harness needs to think again
// after HITL or a worker recycle. It is not the wait loop and not credentials.
//
// Frozen contents: SessionCheckpoint (window, plan, parked interrupt, userState),
// MountRecipe topology, and session identity (agent, parent, specialist, child
// ids). Tokens, file bytes, leftover unstarted Temporal tool calls, MCP env
// and headers, and child workflow futures never go here.
//
// Save's expected Revision must match the last Load (zero if no row).
// Mismatch means another writer already saved — reload and retry.
type SnapshotStore interface {
	Save(ctx context.Context, sessionID SessionID, snap Snapshot, expected Revision) (Revision, error)
	Load(ctx context.Context, sessionID SessionID) (Snapshot, Revision, error)
	Delete(ctx context.Context, sessionID SessionID) error
}
