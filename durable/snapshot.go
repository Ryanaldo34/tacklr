package durable

import (
	"context"
)

// SnapshotStore holds one session's harness blob (window, plan, parked
// interrupts, parked-worker checkpoints) and secret-free VFS recipes. The
// workflow/driver keeps the etag only. Close deletes the row. Tokens, file
// bytes, and Temporal leftover unstarted tool calls are never stored here.
type SnapshotStore interface {
	Save(ctx context.Context, sessionID SessionID, snap Snapshot, etag string) (newEtag string, err error)
	Load(ctx context.Context, sessionID SessionID) (Snapshot, string, error)
	Delete(ctx context.Context, sessionID SessionID) error
}
