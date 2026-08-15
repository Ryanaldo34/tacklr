package vfsindex

import (
	"context"
	"sync"
	"time"
)

// IndexReason explains why a path was scheduled for re-index.
// Notify accepts a reason for host/API clarity; IndexPath does not branch on it.
type IndexReason int

const (
	// ReasonSync is a successful VFS WriteFile or Sync (AfterPersist bridge).
	ReasonSync IndexReason = iota
	// ReasonExplicit is a host or tool request (index_file / IndexPath).
	ReasonExplicit
)

// IndexScheduler receives path invalidations. SyncScheduler runs inline;
// AsyncScheduler enqueues work with the same interface.
type IndexScheduler interface {
	Notify(ctx context.Context, virtualPath string, reason IndexReason) error
}

// SyncScheduler re-indexes immediately on Notify (v1).
type SyncScheduler struct {
	Indexer *MountIndexer
}

// NewSyncScheduler returns an IndexScheduler that calls IndexPath inline.
func NewSyncScheduler(idx *MountIndexer) *SyncScheduler {
	if idx == nil {
		panic("vfsindex: NewSyncScheduler requires MountIndexer")
	}
	return &SyncScheduler{Indexer: idx}
}

// Notify implements IndexScheduler.
func (s *SyncScheduler) Notify(ctx context.Context, virtualPath string, reason IndexReason) error {
	_ = reason
	return s.Indexer.IndexPath(ctx, virtualPath)
}

// Defaults for AsyncScheduler.
const (
	DefaultAsyncQueueCap = 64
	DefaultAsyncTimeout  = 2 * time.Minute
)

// AsyncScheduler re-indexes paths on background worker(s). Notify is non-blocking:
// it enqueues the path (duplicates coalesce) and returns immediately.
// Under pressure (queue full and path not already pending), the notify is dropped.
// Close cancels in-flight work and stops the worker.
type AsyncScheduler struct {
	Indexer  *MountIndexer
	QueueCap int           // max distinct pending paths; default DefaultAsyncQueueCap
	Timeout  time.Duration // per-path IndexPath timeout; default DefaultAsyncTimeout

	mu      sync.Mutex
	pending map[string]struct{}
	closed  bool
	wake    chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewAsyncScheduler starts a single background worker. Call Close on teardown.
func NewAsyncScheduler(idx *MountIndexer) *AsyncScheduler {
	if idx == nil {
		panic("vfsindex: NewAsyncScheduler requires MountIndexer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &AsyncScheduler{
		Indexer:  idx,
		QueueCap: DefaultAsyncQueueCap,
		Timeout:  DefaultAsyncTimeout,
		pending:  make(map[string]struct{}),
		wake:     make(chan struct{}, 1),
		cancel:   cancel,
	}
	s.wg.Add(1)
	go s.loop(ctx)
	return s
}

// Notify implements IndexScheduler. Never blocks on IndexPath.
// The caller's ctx is not used for enqueue: AfterPersist and similar short-lived
// contexts must not cancel pending work. IndexPath runs under the scheduler's
// own cancel + Timeout context.
func (s *AsyncScheduler) Notify(ctx context.Context, virtualPath string, reason IndexReason) error {
	_ = ctx
	_ = reason
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if _, ok := s.pending[virtualPath]; ok {
		// Already coalesced; worker will process without another wake.
		return nil
	}
	capN := s.QueueCap
	if capN <= 0 {
		capN = DefaultAsyncQueueCap
	}
	if len(s.pending) >= capN {
		// Drop under pressure (path not already coalesced).
		return nil
	}
	s.pending[virtualPath] = struct{}{}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// Close stops the worker and cancels any in-flight IndexPath. Safe to call once
// or more; subsequent calls are no-ops.
func (s *AsyncScheduler) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	return nil
}

func (s *AsyncScheduler) loop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			for {
				path, ok := s.takeOne()
				if !ok {
					break
				}
				if ctx.Err() != nil {
					return
				}
				s.runIndex(ctx, path)
			}
		}
	}
}

func (s *AsyncScheduler) takeOne() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return "", false
	}
	// Map iteration order is random; any one path is fine (coalesced).
	var p string
	for p = range s.pending {
		break
	}
	delete(s.pending, p)
	return p, true
}

func (s *AsyncScheduler) runIndex(parent context.Context, path string) {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = DefaultAsyncTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	_ = s.Indexer.IndexPath(ctx, path)
}
