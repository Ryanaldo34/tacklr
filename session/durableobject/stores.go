package durableobject

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/session"
)

// snapshots is session.Store on an object's storage: the checkpoint's rows,
// one key each, its revision, and each tool call's body by call key. It
// keeps each session's revision in memory as storage has it, since the
// object is its storage's only writer.
type snapshots struct {
	s    Storage
	mu   sync.Mutex
	gens map[session.ID]uint64
}

func newSnapshots(s Storage) *snapshots {
	return &snapshots{s: s, gens: map[session.ID]uint64{}}
}

// gen is id's checkpoint generation, its revision; 0 when it has none. Call
// it with st.mu held.
func (st *snapshots) gen(id session.ID) (uint64, error) {
	if n, ok := st.gens[id]; ok {
		return n, nil
	}
	raw, ok, err := st.s.Get(key(kindSnapshot, id))
	if err != nil {
		return 0, err
	}
	var n uint64
	if ok {
		if n, err = strconv.ParseUint(string(raw), 10, 64); err != nil {
			return 0, err
		}
	}
	st.gens[id] = n
	return n, nil
}

func revisionOf(gen uint64) session.Revision {
	if gen == 0 {
		return ""
	}
	return session.Revision(strconv.FormatUint(gen, 10))
}

// rowKey is the key of one checkpoint row of id.
func rowKey(id session.ID, kind, k string) string {
	return prefix(kindCheckpoint, id) + url.PathEscape(kind) + "/" + url.PathEscape(k)
}

// Save implements session.Store: the changed rows and the new revision in
// one write.
func (st *snapshots) Save(_ context.Context, id session.ID, changes []session.CheckpointChange, expected session.Revision) (session.Revision, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	ops, gen, err := st.saveOps(id, changes, expected)
	if err != nil {
		return "", err
	}
	if err := st.s.Write(ops...); err != nil {
		return "", err
	}
	st.gens[id] = gen
	return revisionOf(gen), nil
}

// saveOps are the writes of a Save, and the generation they make. Call it
// with st.mu held; once the writes are done, the generation is id's.
func (st *snapshots) saveOps(id session.ID, changes []session.CheckpointChange, expected session.Revision) ([]Op, uint64, error) {
	if id == "" {
		return nil, 0, fmt.Errorf("snapshot: session id is required")
	}
	gen, err := st.gen(id)
	if err != nil {
		return nil, 0, err
	}
	if expected != revisionOf(gen) {
		return nil, 0, session.ErrStaleCheckpoint
	}
	ops := make([]Op, 0, len(changes)+1)
	for _, c := range changes {
		if c.Delete {
			ops = append(ops, del(rowKey(id, c.Kind, c.Key)))
			continue
		}
		ops = append(ops, put(rowKey(id, c.Kind, c.Key), c.Value))
	}
	gen++
	return append(ops, put(key(kindSnapshot, id), []byte(strconv.FormatUint(gen, 10)))), gen, nil
}

// Load implements session.Store.
func (st *snapshots) Load(_ context.Context, id session.ID) ([]session.CheckpointEntry, session.Revision, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	gen, err := st.gen(id)
	if err != nil {
		return nil, "", err
	}
	if gen == 0 {
		return nil, "", fmt.Errorf("load snapshot %q: %w", id, session.ErrNotFound)
	}
	var rows []session.CheckpointEntry
	err = scanAll(st.s, prefix(kindCheckpoint, id), func(e Entry) error {
		// rowKey escaped both parts, so the one slash splits them.
		kind, k, _ := strings.Cut(e.Key[len(prefix(kindCheckpoint, id)):], "/")
		row := session.CheckpointEntry{Value: e.Value}
		row.Kind, _ = url.PathUnescape(kind)
		row.Key, _ = url.PathUnescape(k)
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return rows, revisionOf(gen), nil
}

// Revision implements session.Store. A live object answers from memory.
func (st *snapshots) Revision(_ context.Context, id session.ID) (session.Revision, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	gen, err := st.gen(id)
	return revisionOf(gen), err
}

func bodyKey(id session.ID, callKey string) string {
	return prefix(kindBody, id) + url.PathEscape(callKey)
}

// PutCall implements session.Store.
func (st *snapshots) PutCall(_ context.Context, id session.ID, callKey string, body []byte) error {
	return st.s.Write(put(bodyKey(id, callKey), body))
}

// Calls implements session.Store.
func (st *snapshots) Calls(_ context.Context, id session.ID, callKeys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(callKeys))
	for _, k := range callKeys {
		body, ok, err := st.s.Get(bodyKey(id, k))
		if err != nil {
			return nil, err
		}
		if ok {
			out[k] = body
		}
	}
	return out, nil
}

// ClearCalls implements session.Store.
func (st *snapshots) ClearCalls(_ context.Context, id session.ID) error {
	ops, err := deleteAll(st.s, prefix(kindBody, id))
	if err != nil {
		return err
	}
	return st.s.Write(ops...)
}

// Delete implements session.Store.
func (st *snapshots) Delete(_ context.Context, id session.ID) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	var ops []Op
	for _, kind := range []string{kindBody, kindCheckpoint} {
		more, err := deleteAll(st.s, prefix(kind, id))
		if err != nil {
			return err
		}
		ops = append(ops, more...)
	}
	if err := st.s.Write(append(ops, del(key(kindSnapshot, id)))...); err != nil {
		return err
	}
	st.gens[id] = 0
	return nil
}

var _ session.Store = (*snapshots)(nil)

// forget drops id's revision from memory, when its checkpoint was deleted
// in a write of the object's.
func (st *snapshots) forget(id session.ID) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.gens[id] = 0
}

// staged is one step's view of the session store: its Save and its
// PutCall become writes of the step's one write (ops), so the step's
// journal entry, its events and what it stored land together or not at
// all. Call applied once that write is done.
type staged struct {
	*snapshots
	ops   []Op
	id    session.ID
	gen   uint64
	saved bool
}

// Save implements session.Store.
func (s *staged) Save(_ context.Context, id session.ID, changes []session.CheckpointChange, expected session.Revision) (session.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ops, gen, err := s.saveOps(id, changes, expected)
	if err != nil {
		return "", err
	}
	s.ops = append(s.ops, ops...)
	s.id, s.gen, s.saved = id, gen, true
	return revisionOf(gen), nil
}

// PutCall implements session.Store.
func (s *staged) PutCall(_ context.Context, id session.ID, callKey string, body []byte) error {
	s.ops = append(s.ops, put(bodyKey(id, callKey), body))
	return nil
}

// applied takes the revision the step's write saved.
func (s *staged) applied() {
	if !s.saved {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gens[s.id] = s.gen
}

// secretStore is session.SecretStorage on an object's storage. Each bag is
// sealed before it is written, so storage never holds a plaintext token.
type secretStore struct {
	s      Storage
	sealer session.Sealer
}

// Put implements session.SecretStorage.
func (st *secretStore) Put(ctx context.Context, id session.ID, secrets session.Secrets) error {
	if id == "" {
		return fmt.Errorf("secrets: session id is required")
	}
	if len(secrets.Auth.Bindings) == 0 {
		return nil
	}
	plain, _ := json.Marshal(secrets) // strings, times and maps of strings only
	sealed, err := st.sealer.Seal(ctx, plain, []byte(id))
	if err != nil {
		return fmt.Errorf("secrets %q: seal: %w", id, err)
	}
	return st.s.Write(put(key(kindSecrets, id), sealed))
}

// Get implements session.SecretStorage.
func (st *secretStore) Get(ctx context.Context, id session.ID) (session.Secrets, error) {
	sealed, ok, err := st.s.Get(key(kindSecrets, id))
	if err != nil || !ok {
		return session.Secrets{}, err
	}
	plain, err := st.sealer.Open(ctx, sealed, []byte(id))
	if err != nil {
		return session.Secrets{}, fmt.Errorf("secrets %q: open: %w", id, err)
	}
	var out session.Secrets
	if err := json.Unmarshal(plain, &out); err != nil {
		return session.Secrets{}, fmt.Errorf("secrets %q: %w", id, err)
	}
	return out, nil
}

// Delete implements session.SecretStorage.
func (st *secretStore) Delete(_ context.Context, id session.ID) error {
	return st.s.Write(del(key(kindSecrets, id)))
}

var _ session.SecretStorage = (*secretStore)(nil)

// eventLog is session.EventLog on an object's storage. Appends are numbered
// without gaps; subscribers read storage at their own pace and wait on
// changed for the next append.
type eventLog struct {
	s    Storage
	mu   sync.Mutex
	logs map[session.ID]*sessionLog
}

// sessionLog is one session's head, and the channel that wakes its tails.
// changed is closed and replaced on every append, and on close.
type sessionLog struct {
	head    uint64
	closed  bool
	changed chan struct{}
}

type storedEvent struct {
	Topic string        `json:"topic"`
	Event session.Event `json:"event"`
}

// tailBatch is how many events one read of a tail returns.
const tailBatch = 256

func newEventLog(s Storage) *eventLog {
	return &eventLog{s: s, logs: map[session.ID]*sessionLog{}}
}

// log is id's log, its head read from storage on first use. Call it with
// l.mu held.
func (l *eventLog) log(id session.ID) (*sessionLog, error) {
	if sl, ok := l.logs[id]; ok {
		return sl, nil
	}
	raw, ok, err := l.s.Get(key(kindEventHead, id))
	if err != nil {
		return nil, err
	}
	sl := &sessionLog{changed: make(chan struct{})}
	if ok {
		sl.head, _ = strconv.ParseUint(string(raw), 10, 64)
	}
	l.logs[id] = sl
	return sl, nil
}

// Append implements session.EventLog.
func (l *eventLog) Append(_ context.Context, id session.ID, topic string, ev session.Event) error {
	return l.commit(id, []storedEvent{{Topic: topic, Event: ev}})
}

// commit writes evs, numbered after id's head, and ops in one write, then
// wakes id's tails. Every write of a step goes through it, so a step's
// events and its journal entry land together.
func (l *eventLog) commit(id session.ID, evs []storedEvent, ops ...Op) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	sl, err := l.log(id)
	if err != nil {
		return err
	}
	head := sl.head
	for _, se := range evs {
		raw, err := json.Marshal(se)
		if err != nil {
			return err
		}
		head++
		ops = append(ops, put(seqKey(kindEvent, id, head), raw))
	}
	if head != sl.head {
		ops = append(ops, put(key(kindEventHead, id), []byte(strconv.FormatUint(head, 10))))
	}
	if err := l.s.Write(ops...); err != nil {
		return err
	}
	if head != sl.head {
		sl.head = head
		close(sl.changed)
		sl.changed = make(chan struct{})
	}
	return nil
}

// Subscribe implements session.EventLog: it replays the events after after,
// then tails until ctx ends or the session's log closes. Retries are not
// sent.
func (l *eventLog) Subscribe(ctx context.Context, id session.ID, after session.Seq) (<-chan session.Event, error) {
	t, err := l.tail(ctx, id, after)
	if err != nil {
		return nil, err
	}
	return t.ch, nil
}

// tailing is one reader of a session's log: its events, and why it stopped
// early (err, set before ch closes).
type tailing struct {
	ch  chan session.Event
	err error
}

// tail reads id's events after after, as they come, until ctx ends, the
// session closes, or a read of the log fails.
func (l *eventLog) tail(ctx context.Context, id session.ID, after session.Seq) (*tailing, error) {
	l.mu.Lock()
	sl, err := l.log(id)
	l.mu.Unlock()
	if err != nil {
		return nil, err
	}
	t := &tailing{ch: make(chan session.Event, 64)}
	ch := t.ch
	go func() {
		defer close(ch)
		next := uint64(after)
		for {
			l.mu.Lock()
			closed, changed := sl.closed, sl.changed
			l.mu.Unlock()
			if closed {
				return
			}
			entries, err := l.s.Scan(prefix(kindEvent, id), seqKey(kindEvent, id, next), tailBatch)
			if err != nil {
				t.err = fmt.Errorf("durableobject: read events: %w", err)
				return
			}
			for _, e := range entries {
				next = seqOf(e.Key)
				var se storedEvent
				if json.Unmarshal(e.Value, &se) != nil || se.Topic == session.TopicRetry {
					continue
				}
				select {
				case ch <- se.Event:
				case <-ctx.Done():
					return
				}
			}
			if len(entries) == tailBatch {
				continue
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return
			}
		}
	}()
	return t, nil
}

// Head implements session.EventLog.
func (l *eventLog) Head(_ context.Context, id session.ID) (session.Seq, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sl, err := l.log(id)
	if err != nil {
		return 0, err
	}
	return session.Seq(sl.head), nil
}

// Trim implements session.EventLog.
func (l *eventLog) Trim(_ context.Context, id session.ID, through session.Seq) error {
	ops, err := l.trimOps(id, through)
	if err != nil {
		return err
	}
	return l.s.Write(ops...)
}

// trimOps are the deletes of id's events up to through.
func (l *eventLog) trimOps(id session.ID, through session.Seq) ([]Op, error) {
	var ops []Op
	after := ""
	for {
		entries, err := l.s.Scan(prefix(kindEvent, id), after, tailBatch)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if seqOf(e.Key) > uint64(through) {
				return ops, nil
			}
			ops = append(ops, del(e.Key))
		}
		if len(entries) < tailBatch {
			return ops, nil
		}
		after = entries[len(entries)-1].Key
	}
}

// forget ends id's tails and drops its head from memory, when its log was
// deleted in a write of the object's.
func (l *eventLog) forget(_ context.Context, id session.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if sl, ok := l.logs[id]; ok {
		sl.closed = true
		close(sl.changed)
		delete(l.logs, id)
	}
}

// CloseSession implements session.EventLog: it deletes the session's log
// and ends its tails, so a later session can reuse the id.
func (l *eventLog) CloseSession(_ context.Context, id session.ID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ops, err := deleteAll(l.s, prefix(kindEvent, id))
	if err != nil {
		return err
	}
	if err := l.s.Write(append(ops, del(key(kindEventHead, id)))...); err != nil {
		return err
	}
	if sl, ok := l.logs[id]; ok {
		sl.closed = true
		close(sl.changed)
		delete(l.logs, id)
	}
	return nil
}

var _ session.EventLog = (*eventLog)(nil)
