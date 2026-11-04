package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
)

// harnessSpec names one session's harness: its tacklr.Harness (tools, MCP
// connections and their tools, skills, the system prompt) and its VFS. Key is a fingerprint
// of everything Build builds it from, so a step whose inputs changed (a new
// MCP config, refreshed auth) gets a new harness.
type harnessSpec struct {
	ID    session.ID
	Key   string
	Build func(context.Context) (*tacklr.Harness, *fuse.Server, error)
}

// harnessCache keeps each session's harness on this worker, so the steps of a
// session the runtime routes here share one instead of each building it.
// A harness nobody uses for idle closes, and past limit harnesses the one unused
// longest closes to make room. Committed state stays the store's:
// a step loads it into the harness only when the store's revision moved past
// the one the harness holds.
type harnessCache struct {
	idle  time.Duration
	limit int
	mu    sync.Mutex
	byID  map[session.ID]*entry
	// clock orders releases, so the harness unused longest is the one with
	// the lowest last.
	clock uint64
}

type entry struct {
	key   string
	ready chan struct{}
	err   error
	h     *tacklr.Harness
	mount *fuse.Server
	users int
	last  uint64
	gone  bool
	timer *time.Timer
	// state guards committed state: steps read it, Commit and a reload
	// write it.
	state sync.RWMutex
	rev   session.Revision
	// loaded says h holds the snapshot at rev; stale that a failed Commit
	// left h with state the store does not hold.
	loaded, stale bool
}

// newHarnessCache returns an empty cache whose harnesses close after idle unused.
// It keeps at most limit harnesses (zero: no limit): a harness in use is never
// closed for room, so the cache passes limit only while all of them are.
func newHarnessCache(idle time.Duration, limit int) *harnessCache {
	return &harnessCache{idle: idle, limit: limit, byID: map[session.ID]*entry{}}
}

// Read runs a step that reads committed state (Infer, a tool call) on w's
// harness. Steps of one session run at once; each tool call keeps its own
// writes until Commit.
func (ws *harnessCache) Read(ctx context.Context, store session.Store, w harnessSpec, step func(*tacklr.Harness) error) error {
	for {
		cur, err := ws.acquire(ctx, w)
		if err != nil {
			return err
		}
		err = cur.rlock(ctx, store, w.ID)
		if errors.Is(err, errHeld) {
			ws.replace(w.ID, cur)
			continue
		}
		if err != nil {
			ws.release(w.ID, cur)
			return err
		}
		err = step(cur.h)
		cur.state.RUnlock()
		ws.release(w.ID, cur)
		return err
	}
}

// errHeld is a harness whose state a step the turn moved past still reads (a
// cancelled tool that has not returned). Writing it would wait for that
// step, so the writer builds a new harness instead.
var errHeld = errors.New("harness held by an earlier step")

// replace drops cur, which a step the turn moved past still holds, so the
// next acquire builds a new harness.
func (ws *harnessCache) replace(id session.ID, cur *entry) {
	ws.mu.Lock()
	ws.drop(id, cur)
	ws.mu.Unlock()
	ws.release(id, cur)
}

// Commit is the Commit step on w's harness: it applies in and saves, unless
// the snapshot is already past base (an earlier attempt of this Commit
// saved), and returns the snapshot's revision. It holds the harness alone. A
// failed Commit drops the harness, since its state may be ahead of the store.
func (ws *harnessCache) Commit(ctx context.Context, store session.Store, w harnessSpec, base session.Revision, in tacklr.CommitIn, out chan session.Event) (session.Revision, error) {
	for {
		cur, err := ws.acquire(ctx, w)
		if err != nil {
			return "", err
		}
		// No step of the session runs alongside its Commit; a reader still
		// here is one the turn moved past.
		if !cur.state.TryLock() {
			ws.replace(w.ID, cur)
			continue
		}
		rev, err := cur.commitAt(ctx, store, w.ID, base, in, out)
		cur.state.Unlock()
		ws.release(w.ID, cur)
		return rev, err
	}
}

// commitAt brings the harness to the store's revision, then commits in on
// it unless the snapshot is already past base. Call it with state held for
// writing.
func (c *entry) commitAt(ctx context.Context, store session.Store, id session.ID, base session.Revision, in tacklr.CommitIn, out chan session.Event) (session.Revision, error) {
	if err := c.sync(ctx, store, id); err != nil {
		return "", err
	}
	if c.rev != base {
		return c.rev, nil
	}
	if in.Round != nil {
		r := *in.Round
		r.Outcomes = slices.Clone(r.Outcomes)
		if err := adapter.LoadRecords(ctx, store, id, r.Outcomes); err != nil {
			return "", err
		}
		in.Round = &r
	}
	// A failed Commit may leave the harness ahead of the store.
	c.stale = true
	if err := c.h.Commit(ctx, in, out); err != nil {
		return "", err
	}
	cp, err := c.h.Checkpoint()
	if err == nil {
		c.rev, err = store.Save(ctx, id, cp, c.rev)
	}
	if err != nil {
		return "", err
	}
	c.stale = false
	return c.rev, nil
}

// Close closes id's harness now, when its session ends. A step still using
// it finishes first.
func (ws *harnessCache) Close(id session.ID) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if cur := ws.byID[id]; cur != nil {
		ws.drop(id, cur)
	}
}

// CloseAll closes every harness, when the worker stops, and returns once
// the ones no step uses are closed and their workspaces unmounted. A step
// still running closes its harness when it ends.
func (ws *harnessCache) CloseAll() {
	ws.mu.Lock()
	var closed []<-chan struct{}
	for id, cur := range ws.byID {
		if done := ws.drop(id, cur); done != nil {
			closed = append(closed, done)
		}
	}
	ws.mu.Unlock()
	for _, done := range closed {
		<-done
	}
}

// acquire returns w's harness built and in use. A harness with another key, or
// a stale one, is closed and built again.
func (ws *harnessCache) acquire(ctx context.Context, w harnessSpec) (*entry, error) {
	ws.mu.Lock()
	cur := ws.byID[w.ID]
	if cur != nil && (cur.key != w.Key || cur.stale) {
		ws.drop(w.ID, cur)
		cur = nil
	}
	if cur == nil {
		ws.makeRoom()
		cur = &entry{key: w.Key, ready: make(chan struct{})}
		ws.byID[w.ID] = cur
		go cur.build(context.WithoutCancel(ctx), w.Build)
	}
	cur.users++
	if cur.timer != nil {
		cur.timer.Stop()
		cur.timer = nil
	}
	ws.mu.Unlock()
	<-cur.ready
	if cur.err != nil {
		ws.mu.Lock()
		ws.drop(w.ID, cur)
		ws.mu.Unlock()
		ws.release(w.ID, cur)
		return nil, cur.err
	}
	return cur, nil
}

// makeRoom closes the harness unused longest when the cache is at its limit.
// Call it with ws.mu held.
func (ws *harnessCache) makeRoom() {
	if ws.limit <= 0 || len(ws.byID) < ws.limit {
		return
	}
	var oldestID session.ID
	var oldest *entry
	for id, c := range ws.byID {
		if c.users == 0 && (oldest == nil || c.last < oldest.last) {
			oldestID, oldest = id, c
		}
	}
	if oldest != nil {
		ws.drop(oldestID, oldest)
	}
}

// build builds the harness outside the cache lock, so other sessions do not
// wait on it. It outlives the step that asked for it, so it does not stop
// when that step is cancelled.
func (c *entry) build(ctx context.Context, build func(context.Context) (*tacklr.Harness, *fuse.Server, error)) {
	c.h, c.mount, c.err = build(ctx)
	close(c.ready)
}

// rlock holds the harness's state for reading, at the store's revision. The
// calls of one reply read at once; a reader reloads only after a commit
// elsewhere, and returns errHeld when another reader still holds the state.
func (c *entry) rlock(ctx context.Context, store session.Store, id session.ID) error {
	held := false
	for {
		c.state.RLock()
		current, err := c.current(ctx, store, id)
		if err != nil || current {
			if err != nil {
				c.state.RUnlock()
			}
			return err
		}
		c.state.RUnlock()
		if !c.state.TryLock() {
			// Another step holds the state. A call of this reply that just
			// reloaded it leaves it current: read again once. Still stale,
			// it is a step the turn moved past.
			if held {
				return errHeld
			}
			held = true
			continue
		}
		err = c.sync(ctx, store, id)
		c.state.Unlock()
		if err != nil {
			return err
		}
	}
}

// current reports whether the harness holds the store's revision. Call it
// with state held.
func (c *entry) current(ctx context.Context, store session.Store, id session.ID) (bool, error) {
	rev, err := store.Revision(ctx, id)
	return err == nil && c.loaded && rev == c.rev, err
}

// sync loads the snapshot into the harness when the store's revision is not
// the one it holds. Call it with state held for writing.
func (c *entry) sync(ctx context.Context, store session.Store, id session.ID) error {
	rev, err := store.Revision(ctx, id)
	if err != nil {
		return err
	}
	if c.loaded && rev == c.rev {
		return nil
	}
	if rev == "" {
		if c.loaded {
			return fmt.Errorf("snapshot of %s was deleted: %w", id, session.ErrNotFound)
		}
		c.loaded = true
		return nil
	}
	cp, rev, err := store.Load(ctx, id)
	if err != nil {
		return err
	}
	if err := c.h.RestoreCheckpoint(cp); err != nil {
		return err
	}
	c.rev, c.loaded = rev, true
	return nil
}

// release ends one step's use. The last user of a dropped harness closes it;
// the last user of a live one starts its idle timer.
func (ws *harnessCache) release(id session.ID, cur *entry) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	cur.users--
	if cur.users > 0 {
		return
	}
	ws.clock++
	cur.last = ws.clock
	if cur.gone {
		cur.close()
		return
	}
	cur.timer = time.AfterFunc(ws.idle, func() {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		if ws.byID[id] == cur && cur.users == 0 {
			ws.drop(id, cur)
		}
	})
}

// drop takes cur out of the cache and closes it once no step uses it. The
// channel closes when that close is done; it is nil while a step uses cur.
// Call it with ws.mu held.
func (ws *harnessCache) drop(id session.ID, cur *entry) <-chan struct{} {
	if ws.byID[id] == cur {
		delete(ws.byID, id)
	}
	cur.gone = true
	if cur.timer != nil {
		cur.timer.Stop()
		cur.timer = nil
	}
	if cur.users > 0 {
		return nil
	}
	return cur.close()
}

// close closes c's harness once it is built, without holding up the caller.
// The channel closes when it is done.
func (c *entry) close() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-c.ready
		abandonTurn(c.h, c.mount)
	}()
	return done
}
