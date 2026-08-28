package durable

import (
	"context"
)

// Revision is the SnapshotStore compare-and-swap token for one session row.
// The zero value means no row exists yet; the next Save creates it.
type Revision string

// SnapshotStore holds one session's harness blob (window, plan, parked
// interrupts, VFS recipes). Close deletes the row. Tokens and file bytes
// are never stored here.
//
// Save's expected Revision must match the last Load (zero if no row).
// Mismatch means another writer already saved — reload and retry.
type SnapshotStore interface {
	Save(ctx context.Context, sessionID SessionID, snap Snapshot, expected Revision) (Revision, error)
	Load(ctx context.Context, sessionID SessionID) (Snapshot, Revision, error)
	Delete(ctx context.Context, sessionID SessionID) error
}
