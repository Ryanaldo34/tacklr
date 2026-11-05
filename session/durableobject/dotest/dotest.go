// Package dotest runs durable object sessions in memory, for tests: a
// host's tests of its agent on the durable object runtime, and the
// runtime's own. Platform stops objects, fires their alarms and fails their
// reads and writes the way a real platform can, so a test sees what a
// session does when that happens.
package dotest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
)

// errStopped is what a stopped object's storage answers: the platform took
// the object away.
var errStopped = errors.New("dotest: object stopped")

// errOutage is a read FailReads failed, or a write FailWrites failed.
var errOutage = errors.New("dotest: storage unavailable")

// Platform is a durableobject.Platform in memory. Each object's storage
// outlives the object: after Evict, the next request makes a new object over
// the same storage, as a platform does.
type Platform struct {
	cfg    durableobject.Config
	mu     sync.Mutex
	disks  map[session.ID]*disk
	live   map[session.ID]*host
	alarms map[session.ID]time.Time
	// alarmed closes, and is replaced, whenever an object sets its alarm.
	alarmed chan struct{}
	// wire sends every call to an object over the object protocol
	// (NewWire).
	wire bool
}

// disk is one object's storage, and the reads and writes it fails.
type disk struct {
	*durableobject.MemStorage
	failRead  atomic.Pointer[func(key string) bool]
	failWrite atomic.Pointer[func([]durableobject.Op) bool]
}

// New returns an empty Platform whose objects run with cfg.
func New(cfg durableobject.Config) *Platform {
	return &Platform{cfg: cfg, disks: map[session.ID]*disk{}, live: map[session.ID]*host{}, alarms: map[session.ID]time.Time{}, alarmed: make(chan struct{})}
}

// Object implements durableobject.Platform. It is the id's live object,
// made on first use over the id's storage; on a NewWire platform, a Ref
// that reaches it over the object protocol.
func (p *Platform) Object(id session.ID) durableobject.Ref {
	if p.wire {
		return p.remote(id)
	}
	return p.host(id).obj
}

func (p *Platform) host(id session.ID) *host {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h := p.live[id]; h != nil {
		return h
	}
	d := p.disks[id]
	if d == nil {
		d = &disk{MemStorage: durableobject.NewMemStorage()}
		p.disks[id] = d
	}
	h := &host{p: p, id: id, disk: d}
	h.ctx, h.evict = context.WithCancel(context.Background())
	h.obj = durableobject.NewObject(h, p.cfg)
	p.live[id] = h
	return h
}

// Evict stops id's object, as a platform that takes it away does: what it
// still runs can no longer read or write its storage. The next request
// makes a new object over the same storage.
func (p *Platform) Evict(id session.ID) {
	p.mu.Lock()
	h := p.live[id]
	delete(p.live, id)
	p.mu.Unlock()
	if h != nil {
		h.dead.Store(true)
		h.evict()
		h.obj.Stop()
	}
}

// Alarm is the alarm id's object set; zero when none is set.
func (p *Platform) Alarm(id session.ID) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alarms[id]
}

// WaitAlarm returns id's alarm once its object set it to a time after
// after, or, with ctx's error, once ctx ends.
func (p *Platform) WaitAlarm(ctx context.Context, id session.ID, after time.Time) (time.Time, error) {
	for {
		p.mu.Lock()
		at, alarmed := p.alarms[id], p.alarmed
		p.mu.Unlock()
		if at.After(after) || ctx.Err() != nil {
			return at, ctx.Err()
		}
		select {
		case <-alarmed:
		case <-ctx.Done():
		}
	}
}

// FireAlarm delivers id's alarm, as the platform does when it is due.
func (p *Platform) FireAlarm(ctx context.Context, id session.ID) error {
	p.mu.Lock()
	delete(p.alarms, id)
	p.mu.Unlock()
	return p.host(id).obj.Alarm(ctx)
}

// Idle returns once id's object runs nothing (durableobject.Object.Idle).
func (p *Platform) Idle(ctx context.Context, id session.ID) error {
	return p.host(id).obj.Idle(ctx)
}

// Keys is every key id's storage holds, in order.
func (p *Platform) Keys(id session.ID) []string {
	p.mu.Lock()
	d := p.disks[id]
	p.mu.Unlock()
	if d == nil {
		return nil
	}
	return d.Keys()
}

// FailWrites makes id's storage fail each write whose ops fail reports,
// as an outage does; nil makes every write work again.
func (p *Platform) FailWrites(id session.ID, fail func(ops []durableobject.Op) bool) {
	p.host(id).disk.failWrite.Store(&fail)
}

// FailReads makes id's storage fail each read whose key fail reports, as
// an outage does: a Get's key, or a Scan's prefix. Nil makes every read
// work again.
func (p *Platform) FailReads(id session.ID, fail func(key string) bool) {
	p.host(id).disk.failRead.Store(&fail)
}

// fails reports whether a read of key fails (FailReads).
func (d *disk) fails(key string) bool {
	fail := d.failRead.Load()
	return fail != nil && *fail != nil && (*fail)(key)
}

// host is one object's durableobject.Host: its storage, until it is
// evicted.
type host struct {
	p    *Platform
	id   session.ID
	disk *disk
	dead atomic.Bool
	// ctx ends when the object is evicted.
	ctx   context.Context
	evict context.CancelFunc
	obj   *durableobject.Object
}

func (h *host) Get(key string) ([]byte, bool, error) {
	if h.dead.Load() {
		return nil, false, errStopped
	}
	if h.disk.fails(key) {
		return nil, false, errOutage
	}
	return h.disk.Get(key)
}

func (h *host) Scan(prefix, after string, limit int) ([]durableobject.Entry, error) {
	if h.dead.Load() {
		return nil, errStopped
	}
	if h.disk.fails(prefix) {
		return nil, errOutage
	}
	return h.disk.Scan(prefix, after, limit)
}

func (h *host) Write(ops ...durableobject.Op) error {
	if h.dead.Load() {
		return errStopped
	}
	if fail := h.disk.failWrite.Load(); fail != nil && *fail != nil && (*fail)(ops) {
		return errOutage
	}
	return h.disk.Write(ops...)
}

func (h *host) Object(id session.ID) durableobject.Ref { return h.p.Object(id) }

func (h *host) SetAlarm(t time.Time) error {
	if h.dead.Load() {
		return errStopped
	}
	h.p.mu.Lock()
	defer h.p.mu.Unlock()
	if t.IsZero() {
		delete(h.p.alarms, h.id)
	} else {
		h.p.alarms[h.id] = t
	}
	close(h.p.alarmed)
	h.p.alarmed = make(chan struct{})
	return nil
}
