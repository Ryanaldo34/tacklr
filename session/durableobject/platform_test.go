package durableobject

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/storetest"
)

// errEvicted is what a stopped object's storage answers: the platform took
// the object away.
var errEvicted = errors.New("object evicted")

// memDisk is one object's storage. It outlives the objects that use it.
type memDisk struct {
	mu sync.Mutex
	m  map[string][]byte
	// failWrites makes every write fail, as a storage outage does.
	failWrites atomic.Bool
}

func (d *memDisk) Get(key string) ([]byte, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.m[key]
	return slices.Clone(v), ok, nil
}

func (d *memDisk) Scan(prefix, after string, limit int) ([]Entry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var keys []string
	for k := range d.m {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Entry, len(keys))
	for i, k := range keys {
		out[i] = Entry{Key: k, Value: slices.Clone(d.m[k])}
	}
	return out, nil
}

func (d *memDisk) Write(ops ...Op) error {
	if d.failWrites.Load() {
		return errors.New("storage unavailable")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, op := range ops {
		if op.Delete {
			delete(d.m, op.Key)
			continue
		}
		d.m[op.Key] = slices.Clone(op.Value)
	}
	return nil
}

// MemPlatform is a Platform in memory, for tests. Each object's storage
// survives Evict, which stops the object as a platform does: its storage
// fails from then on, so whatever it still runs cannot write.
type MemPlatform struct {
	cfg    Config
	mu     sync.Mutex
	disks  map[session.ID]*memDisk
	live   map[session.ID]*memHost
	alarms map[session.ID]time.Time
}

// NewMemPlatform returns an empty MemPlatform whose objects run with cfg.
func NewMemPlatform(cfg Config) *MemPlatform {
	return &MemPlatform{cfg: cfg, disks: map[session.ID]*memDisk{}, live: map[session.ID]*memHost{}, alarms: map[session.ID]time.Time{}}
}

// Object implements Platform. It is the id's live object, made on first use
// over the id's storage.
func (p *MemPlatform) Object(id session.ID) Ref { return p.host(id).obj }

func (p *MemPlatform) host(id session.ID) *memHost {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h := p.live[id]; h != nil {
		return h
	}
	disk := p.disks[id]
	if disk == nil {
		disk = &memDisk{m: map[string][]byte{}}
		p.disks[id] = disk
	}
	h := &memHost{p: p, id: id, disk: disk}
	h.obj = NewObject(h, p.cfg)
	p.live[id] = h
	return h
}

// Evict stops id's object. The next request makes a new one over the same
// storage.
func (p *MemPlatform) Evict(id session.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h := p.live[id]; h != nil {
		h.dead.Store(true)
		delete(p.live, id)
	}
}

// Alarm is the alarm id's object set; zero when none is set.
func (p *MemPlatform) Alarm(id session.ID) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alarms[id]
}

// FireAlarm delivers id's alarm, as the platform does when it is due.
func (p *MemPlatform) FireAlarm(ctx context.Context, id session.ID) error {
	p.mu.Lock()
	delete(p.alarms, id)
	p.mu.Unlock()
	return p.host(id).obj.Alarm(ctx)
}

// Stored is how many entries of kind id's storage holds for session sid.
func (p *MemPlatform) Stored(id, sid session.ID, kind string) int {
	p.mu.Lock()
	disk := p.disks[id]
	p.mu.Unlock()
	if disk == nil {
		return 0
	}
	entries, _ := disk.Scan(prefix(kind, sid), "", 0)
	if v, ok, _ := disk.Get(key(kind, sid)); ok && v != nil {
		return len(entries) + 1
	}
	return len(entries)
}

// Stopped says id's object runs no session: it ended, or stopped on a
// storage failure.
func (p *MemPlatform) Stopped(id session.ID) bool {
	obj := p.host(id).obj
	obj.mu.Lock()
	defer obj.mu.Unlock()
	return !obj.running
}

// FailWrites makes id's storage fail every write, or work again.
func (p *MemPlatform) FailWrites(id session.ID, fail bool) {
	p.host(id).disk.failWrites.Store(fail)
}

// memHost is one object's Host: its storage until it is evicted.
type memHost struct {
	p    *MemPlatform
	id   session.ID
	disk *memDisk
	dead atomic.Bool
	obj  *Object
}

func (h *memHost) Get(key string) ([]byte, bool, error) {
	if h.dead.Load() {
		return nil, false, errEvicted
	}
	return h.disk.Get(key)
}

func (h *memHost) Scan(prefix, after string, limit int) ([]Entry, error) {
	if h.dead.Load() {
		return nil, errEvicted
	}
	return h.disk.Scan(prefix, after, limit)
}

func (h *memHost) Write(ops ...Op) error {
	if h.dead.Load() {
		return errEvicted
	}
	return h.disk.Write(ops...)
}

func (h *memHost) Object(id session.ID) Ref { return h.p.Object(id) }

func (h *memHost) SetAlarm(t time.Time) error {
	if h.dead.Load() {
		return errEvicted
	}
	h.p.mu.Lock()
	defer h.p.mu.Unlock()
	if t.IsZero() {
		delete(h.p.alarms, h.id)
	} else {
		h.p.alarms[h.id] = t
	}
	return nil
}

// FastRetries makes a step that runs again wait a millisecond, for the
// test's length.
func FastRetries(t *testing.T) {
	before := retryBackoff
	retryBackoff = time.Millisecond
	t.Cleanup(func() { retryBackoff = before })
}

func TestStores_meetTheSessionStoreContracts(t *testing.T) {
	disk := func() *memDisk { return &memDisk{m: map[string][]byte{}} }
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	storetest.Store(t, func(*testing.T) session.Store { return &snapshots{s: disk()} })
	storetest.Secrets(t, func(*testing.T) session.SecretStorage { return &secretStore{s: disk(), sealer: sealer} })
	storetest.EventLog(t, func(*testing.T) session.EventLog { return newEventLog(disk()) })
}
