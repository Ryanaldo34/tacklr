package session

import (
	"context"
	"slices"
	"sync"
)

type logEntry struct {
	topic string
	ev    Event
}

type sessionLog struct {
	mu sync.Mutex
	// entries are the events after base: entries[0] is event base+1.
	entries []logEntry
	base    int
	closed  bool
	// changed is closed and replaced on every Append and on close: it wakes
	// the tails, which read the log from where they are.
	changed chan struct{}
}

// MemoryEventLog is a channel EventLog with topics events/retry/close.
type MemoryEventLog struct {
	mu       sync.RWMutex
	sessions map[ID]*sessionLog
}

// NewMemoryEventLog returns an empty EventLog.
func NewMemoryEventLog() *MemoryEventLog {
	return &MemoryEventLog{sessions: make(map[ID]*sessionLog)}
}

// session is id's log, made on first use.
func (l *MemoryEventLog) session(id ID) *sessionLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sessions[id]
	if !ok {
		s = &sessionLog{changed: make(chan struct{})}
		l.sessions[id] = s
	}
	return s
}

// Append implements EventLog.
func (l *MemoryEventLog) Append(_ context.Context, sessionID ID, topic string, ev Event) error {
	s := l.session(sessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, logEntry{topic: topic, ev: ev})
	close(s.changed)
	s.changed = make(chan struct{})
	return nil
}

// Subscribe implements EventLog. It replays seq > after, then tails: each
// subscriber reads the log at its own pace, so a slow one misses nothing
// and never blocks Append. Retry entries are not sent.
func (l *MemoryEventLog) Subscribe(ctx context.Context, sessionID ID, after Seq) (<-chan Event, error) {
	s := l.session(sessionID)
	ch := make(chan Event, 64)
	next := int(after) //nolint:gosec // G115: EventLog seq is well below MaxInt
	go func() {
		defer close(ch)
		for {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			from := min(max(next-s.base, 0), len(s.entries))
			entries, changed := s.entries[from:], s.changed
			next = s.base + len(s.entries)
			s.mu.Unlock()
			for _, e := range entries {
				if e.topic == TopicRetry {
					continue
				}
				select {
				case ch <- e.ev:
				case <-ctx.Done():
					return
				}
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

// Head implements EventLog. An unknown session is at 0, and gets no log.
func (l *MemoryEventLog) Head(_ context.Context, sessionID ID) (Seq, error) {
	l.mu.RLock()
	s := l.sessions[sessionID]
	l.mu.RUnlock()
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return Seq(s.base + len(s.entries)), nil //nolint:gosec // G115: counts are not negative
}

// Trim implements EventLog.
func (l *MemoryEventLog) Trim(_ context.Context, sessionID ID, through Seq) error {
	l.mu.RLock()
	s := l.sessions[sessionID]
	l.mu.RUnlock()
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(max(int(through)-s.base, 0), len(s.entries)) //nolint:gosec // G115: EventLog seq is well below MaxInt
	// A new array: tails read the old one outside the lock.
	s.entries = slices.Clone(s.entries[n:])
	s.base += n
	return nil
}

// CloseSession implements EventLog. It drops the log, ending its tails, so
// a later session can reuse the id and closed history does not stay in
// process memory.
func (l *MemoryEventLog) CloseSession(_ context.Context, sessionID ID) error {
	l.mu.Lock()
	s, ok := l.sessions[sessionID]
	delete(l.sessions, sessionID)
	l.mu.Unlock()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.entries = nil
	close(s.changed)
	s.changed = make(chan struct{})
	return nil
}

var _ EventLog = (*MemoryEventLog)(nil)
