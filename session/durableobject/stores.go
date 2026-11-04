package durableobject

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/ryanaldo34/tacklr/session"
)

// snapshots is session.Store on an object's storage: the snapshot with its
// revision, and each tool call's outcome by call key.
type snapshots struct {
	s  Storage
	mu sync.Mutex
}

type snapshotRecord struct {
	Gen uint64 `json:"gen"`
	CP  []byte `json:"cp"`
}

func (st *snapshots) record(id session.ID) (snapshotRecord, bool, error) {
	raw, ok, err := st.s.Get(key(kindSnapshot, id))
	if err != nil || !ok {
		return snapshotRecord{}, false, err
	}
	var rec snapshotRecord
	err = json.Unmarshal(raw, &rec)
	return rec, true, err
}

func revisionOf(gen uint64) session.Revision {
	return session.Revision(strconv.FormatUint(gen, 10))
}

// Save implements session.Store.
func (st *snapshots) Save(_ context.Context, id session.ID, cp []byte, expected session.Revision) (session.Revision, error) {
	if id == "" {
		return "", fmt.Errorf("snapshot: session id is required")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	rec, ok, err := st.record(id)
	if err != nil {
		return "", err
	}
	var current session.Revision
	if ok {
		current = revisionOf(rec.Gen)
	}
	if expected != current {
		return "", session.ErrStaleCheckpoint
	}
	rec.Gen++
	rec.CP = cp
	raw, _ := json.Marshal(rec) // a counter and bytes always encode
	if err := st.s.Write(put(key(kindSnapshot, id), raw)); err != nil {
		return "", err
	}
	return revisionOf(rec.Gen), nil
}

// Load implements session.Store.
func (st *snapshots) Load(_ context.Context, id session.ID) ([]byte, session.Revision, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec, ok, err := st.record(id)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", fmt.Errorf("load snapshot %q: %w", id, session.ErrNotFound)
	}
	return rec.CP, revisionOf(rec.Gen), nil
}

// Revision implements session.Store.
func (st *snapshots) Revision(_ context.Context, id session.ID) (session.Revision, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec, ok, err := st.record(id)
	if err != nil || !ok {
		return "", err
	}
	return revisionOf(rec.Gen), nil
}

func outcomeKey(id session.ID, callKey string) string {
	return prefix(kindOutcome, id) + url.PathEscape(callKey)
}

// PutOutcome implements session.Store.
func (st *snapshots) PutOutcome(_ context.Context, id session.ID, callKey string, o session.Outcome) error {
	raw, _ := json.Marshal(o) // a string and bytes always encode
	return st.s.Write(put(outcomeKey(id, callKey), raw))
}

// Outcomes implements session.Store.
func (st *snapshots) Outcomes(_ context.Context, id session.ID, callKeys []string) (map[string]session.Outcome, error) {
	out := make(map[string]session.Outcome, len(callKeys))
	for _, k := range callKeys {
		raw, ok, err := st.s.Get(outcomeKey(id, k))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		var o session.Outcome
		if err := json.Unmarshal(raw, &o); err != nil {
			return nil, err
		}
		out[k] = o
	}
	return out, nil
}

// Delete implements session.Store.
func (st *snapshots) Delete(_ context.Context, id session.ID) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ops, err := deleteAll(st.s, prefix(kindOutcome, id))
	if err != nil {
		return err
	}
	return st.s.Write(append(ops, del(key(kindSnapshot, id)))...)
}

var _ session.Store = (*snapshots)(nil)

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
	if ev.Error != nil && ev.Fail == "" {
		ev.Fail = ev.Error.Error()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	sl, err := l.log(id)
	if err != nil {
		return err
	}
	n := sl.head + 1
	raw, err := json.Marshal(storedEvent{Topic: topic, Event: ev})
	if err != nil {
		return err
	}
	if err := l.s.Write(put(seqKey(kindEvent, id, n), raw), put(key(kindEventHead, id), []byte(strconv.FormatUint(n, 10)))); err != nil {
		return err
	}
	sl.head = n
	close(sl.changed)
	sl.changed = make(chan struct{})
	return nil
}

// Subscribe implements session.EventLog: it replays the events after after,
// then tails until ctx ends or the session's log closes. Retries are not
// sent.
func (l *eventLog) Subscribe(ctx context.Context, id session.ID, after session.Seq) (<-chan session.Event, error) {
	l.mu.Lock()
	sl, err := l.log(id)
	l.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ch := make(chan session.Event, 64)
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
	return ch, nil
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
