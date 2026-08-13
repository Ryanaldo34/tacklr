package vfsindex

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// MemoryPoint is the default scratch knowledge mount when no brain Provider exists.
const MemoryPoint = "/memory"

// Bridge owns mount→brain index lifecycle: indexer, async reindex, selective
// track set, prefix/watch warm-up. Harness holds this; it is not the agent loop.
type Bridge struct {
	Indexer *MountIndexer

	sched  *AsyncScheduler
	ms     *vfs.MountSession
	track  map[string]struct{}
	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Start builds an indexer, wires AfterPersist (composing any existing hook),
// and warms prefix/watch mounts. If attachMemory, mounts /memory (watch) when
// a scratch profile can serve it (caller already checked).
func Start(ms *vfs.MountSession, eng *brain.Engine, scope brain.Scope, attachMemory bool) (*Bridge, error) {
	if attachMemory {
		attachMemoryMount(ms)
	}
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
	prev := ms.GetAfterPersist()
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		if prev != nil {
			_ = prev(ctx, path)
		}
		if !b.ShouldIndex(path) {
			return nil
		}
		return b.sched.Notify(ctx, path, ReasonSync)
	})
	warmCtx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	for _, spec := range ms.Specs() {
		if !AutoIndex(spec.IndexPolicy) {
			continue
		}
		point := spec.Point
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if _, err := idx.IndexPrefix(warmCtx, point, IndexOpts{}); err != nil && !errors.Is(err, context.Canceled) {
				slog.Debug("vfsindex: IndexPrefix warm-up", "point", point, "error", err)
			}
		}()
	}
	return b, nil
}

// Close stops warm-up and the async scheduler.
func (b *Bridge) Close() error {
	if b == nil {
		return nil
	}
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	b.wg.Wait()
	if b.sched != nil {
		err := b.sched.Close()
		b.sched = nil
		return err
	}
	return nil
}

// PolicyAt is the normalized IndexPolicy for a virtual path (selective if unknown).
func (b *Bridge) PolicyAt(virtualPath string) string {
	if b == nil || b.ms == nil {
		return PolicySelective
	}
	raw, err := b.ms.IndexPolicyAt(virtualPath)
	if err != nil {
		return PolicySelective
	}
	return NormalizePolicy(raw)
}

// ShouldIndex reports whether AfterPersist should enqueue path.
func (b *Bridge) ShouldIndex(virtualPath string) bool {
	if b == nil || b.ms == nil {
		return false
	}
	spec, err := b.ms.SpecAt(virtualPath)
	if err != nil {
		return b.tracked(virtualPath)
	}
	if NormalizePolicy(spec.IndexPolicy) == PolicyNone {
		return false
	}
	switch NormalizePolicy(spec.IndexPolicy) {
	case PolicyPrefix, PolicyWatch:
		return true
	case PolicySelective:
		return b.tracked(virtualPath)
	default:
		return false
	}
}

func (b *Bridge) tracked(virtualPath string) bool {
	b.mu.Lock()
	_, ok := b.track[virtualPath]
	b.mu.Unlock()
	return ok
}

// Track records a selective path so later persists reindex it.
func (b *Bridge) Track(virtualPath string) {
	if b == nil || virtualPath == "" {
		return
	}
	b.mu.Lock()
	if b.track == nil {
		b.track = make(map[string]struct{})
	}
	b.track[virtualPath] = struct{}{}
	b.mu.Unlock()
}

// Untrack drops a path from the selective set.
func (b *Bridge) Untrack(virtualPath string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.track, virtualPath)
	b.mu.Unlock()
}

func attachMemoryMount(ms *vfs.MountSession) {
	if ms == nil {
		return
	}
	for _, s := range ms.Infos() {
		if s.Point == MemoryPoint {
			return
		}
	}
	if err := ms.Mount(context.Background(), vfs.MountSpec{
		Point:       MemoryPoint,
		Profile:     "scratch",
		IndexPolicy: PolicyWatch,
		Params:      map[string]string{"subpath": "memory"},
	}); err != nil {
		slog.Debug("vfsindex: knowledge mount skipped", "point", MemoryPoint, "error", err)
	}
}
