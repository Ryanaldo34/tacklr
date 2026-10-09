package vfsindex

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Bridge is the explicit file indexer for one mount session: IndexPath and
// the selective track set. It does not watch the mount, walk it at start,
// or re-index after a write. A host that keeps the brain current does that
// in its own system.
type Bridge struct {
	Indexer *MountIndexer

	sched *AsyncScheduler
	ms    *vfs.MountSession
	track map[string]struct{}
	mu    sync.Mutex
}

// Start builds an indexer and an idle async scheduler. It does not register
// AfterPersist and it does not read the mount.
func Start(ms *vfs.MountSession, eng *brain.Engine, scope brain.Scope) (*Bridge, error) {
	idx, err := NewMountIndexer(ms, eng, scope)
	if err != nil {
		return nil, err
	}
	b := &Bridge{
		Indexer: idx,
		sched:   NewAsyncScheduler(idx),
		ms:      ms,
		track:   make(map[string]struct{}),
	}
	b.sched.SetObserver(func(event SchedulerEvent) {
		slog.ErrorContext(context.Background(), "vfsindex: asynchronous index failed",
			"path", event.Path,
			"reason", event.Reason,
			"error", event.Err,
		)
	})
	return b, nil
}

// SetObserver replaces the asynchronous indexing failure observer.
func (b *Bridge) SetObserver(observer func(SchedulerEvent)) {
	if b == nil || b.sched == nil {
		return
	}
	b.sched.SetObserver(observer)
}

// Close stops the async scheduler.
func (b *Bridge) Close() error {
	if b == nil || b.sched == nil {
		return nil
	}
	err := b.sched.Close()
	b.sched = nil
	return err
}

// PolicyAt is the normalized IndexPolicy for a virtual path (selective if unknown).
func (b *Bridge) PolicyAt(virtualPath string) string {
	spec, err := b.ms.SpecAt(virtualPath)
	if err != nil {
		return PolicySelective
	}
	return NormalizePolicy(spec.IndexPolicy)
}

// ShouldIndex reports whether a host pipeline should enqueue path.
// The turn does not call this after writes.
func (b *Bridge) ShouldIndex(virtualPath string) bool {
	spec, err := b.ms.SpecAt(virtualPath)
	if err != nil {
		return b.tracked(virtualPath)
	}
	switch NormalizePolicy(spec.IndexPolicy) {
	case PolicyNone:
		return false
	case PolicyPrefix, PolicyWatch:
		return true
	default:
		return b.tracked(virtualPath)
	}
}

func (b *Bridge) tracked(virtualPath string) bool {
	b.mu.Lock()
	_, ok := b.track[virtualPath]
	b.mu.Unlock()
	return ok
}

// Track records a selective path. The turn does not re-index it on write.
func (b *Bridge) Track(virtualPath string) {
	if virtualPath == "" {
		return
	}
	b.mu.Lock()
	b.track[virtualPath] = struct{}{}
	b.mu.Unlock()
}

// Untrack drops a path from the selective set.
func (b *Bridge) Untrack(virtualPath string) {
	b.mu.Lock()
	delete(b.track, virtualPath)
	b.mu.Unlock()
}
