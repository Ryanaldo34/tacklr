package vfsindex

import "context"

// IndexReason explains why a path was scheduled for re-index.
type IndexReason int

const (
	// ReasonSync is a successful VFS WriteFile or Sync.
	ReasonSync IndexReason = iota
	// ReasonMount is after attaching a mount (host may IndexPrefix).
	ReasonMount
	// ReasonExplicit is a host or tool request (vfs_index).
	ReasonExplicit
	// ReasonPeriodic is a future sweeper / full walk.
	ReasonPeriodic
)

// IndexScheduler receives path invalidations. SyncScheduler runs inline;
// AsyncScheduler (later) enqueues work with the same interface.
type IndexScheduler interface {
	Notify(ctx context.Context, virtualPath string, reason IndexReason) error
}

// SyncScheduler re-indexes immediately on Notify (v1).
type SyncScheduler struct {
	Indexer *MountIndexer
}

// NewSyncScheduler returns an IndexScheduler that calls IndexPath inline.
func NewSyncScheduler(idx *MountIndexer) *SyncScheduler {
	return &SyncScheduler{Indexer: idx}
}

// Notify implements IndexScheduler.
func (s *SyncScheduler) Notify(ctx context.Context, virtualPath string, reason IndexReason) error {
	if s == nil || s.Indexer == nil {
		return nil
	}
	_ = reason
	return s.Indexer.IndexPath(ctx, virtualPath)
}
