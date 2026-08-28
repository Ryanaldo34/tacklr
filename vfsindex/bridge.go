package vfsindex

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// MemoryPoint is the scratch knowledge alias when no brain Provider exists.
const MemoryPoint = "/workspace/memory"

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
// and warms prefix/watch members under /workspace.
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
	prev := ms.GetAfterPersist()
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		if prev != nil {
			if err := prev(ctx, path); err != nil {
				return err
			}
		}
		if !b.ShouldIndex(path) {
			return nil
		}
		if path == MemoryPoint || strings.HasPrefix(path, MemoryPoint+"/") {
			return b.Indexer.IndexPath(ctx, path)
		}
		// Eventual-consistency mounts report queue failures through Observer;
		// the backend write has already committed and is not rolled back.
		_ = b.sched.Notify(ctx, path, ReasonSync)
		return nil
	})
	warmCtx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	for _, point := range autoIndexPoints(ms.Specs()) {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if _, err := idx.IndexPrefix(warmCtx, point, IndexOpts{}); err != nil && !errors.Is(err, context.Canceled) {
				b.sched.report(SchedulerEvent{Path: point, Reason: ReasonSync, Err: err})
			}
		}()
	}
	return b, nil
}

// SetObserver replaces the asynchronous indexing failure observer.
func (b *Bridge) SetObserver(observer func(SchedulerEvent)) {
	if b == nil || b.sched == nil {
		return
	}
	b.sched.SetObserver(observer)
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
	spec, err := b.ms.SpecAt(virtualPath)
	if err != nil {
		return PolicySelective
	}
	return NormalizePolicy(spec.IndexPolicy)
}

// ShouldIndex reports whether AfterPersist should enqueue path.
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

// Track records a selective path so later persists reindex it.
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

func autoIndexPoints(specs []vfs.MountSpec) []string {
	var points []string
	for _, spec := range specs {
		if len(spec.Members) == 0 {
			if AutoIndex(spec.IndexPolicy) {
				points = append(points, spec.Point)
			}
			continue
		}
		for _, m := range spec.Members {
			if !AutoIndex(m.IndexPolicy) {
				continue
			}
			name := strings.TrimSpace(m.Params[vfs.ParamName])
			if name == "" {
				name = strings.TrimSpace(m.Profile)
			}
			points = append(points, spec.Point+"/"+name)
		}
	}
	return points
}
