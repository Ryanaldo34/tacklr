package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ryanaldo34/tacklr/session"
)

// tailBatch is how many events one read of a tail returns.
const tailBatch = 256

// listenRetry is how long the listener waits before it reconnects.
const listenRetry = time.Second

// EventLog is session.EventLog on Postgres. Each session's events are
// numbered by a counter row that an Append bumps and writes from in one
// statement, so sequences have no gaps. An Append also NOTIFYs the session's
// id (never the event: a NOTIFY payload is at most 8000 bytes), and one
// listening connection per EventLog wakes the subscribers, which then read
// what they have not seen. Close the EventLog to stop its listener.
type EventLog struct {
	db *DB

	mu    sync.Mutex
	tails map[session.ID]map[*tail]struct{}
	// stop ends the listener; nil until the first Subscribe starts it.
	stop    context.CancelFunc
	stopped chan struct{}
}

// tail is one subscriber's wake-ups: wake when its session has new events,
// closed when the session is closed.
type tail struct {
	wake   chan struct{}
	closed chan struct{}
}

// EventLog returns the session EventLog.
func (db *DB) EventLog() *EventLog {
	return &EventLog{db: db, tails: map[session.ID]map[*tail]struct{}{}, stopped: make(chan struct{})}
}

// Append implements session.EventLog.
func (l *EventLog) Append(ctx context.Context, sessionID session.ID, topic string, ev session.Event) error {
	body, _ := json.Marshal(ev) // strings, bytes and tool calls only; Error is not encoded
	_, err := l.db.pool.Exec(ctx, l.db.sql(`
		WITH head AS (
			INSERT INTO {s}.session_event_heads AS h (session_id, seq) VALUES ($1, 1)
			ON CONFLICT (session_id) DO UPDATE SET seq = h.seq + 1
			RETURNING seq
		), added AS (
			INSERT INTO {s}.session_events (session_id, seq, topic, event)
			SELECT $1, seq, $2, $3 FROM head
			RETURNING seq
		)
		SELECT pg_notify($4, 'e:' || $1) FROM added`), sessionID, topic, body, l.db.channel)
	if err != nil {
		return fmt.Errorf("append event %q: %w", sessionID, err)
	}
	return nil
}

// Subscribe implements session.EventLog. It replays the events after after,
// then tails: each subscriber reads at its own pace, so a slow one misses
// nothing and never blocks Append. Retry entries are not sent. The channel
// closes when ctx ends, the session is closed, or the EventLog is.
func (l *EventLog) Subscribe(ctx context.Context, sessionID session.ID, after session.Seq) (<-chan session.Event, error) {
	t := l.add(sessionID)
	ch := make(chan session.Event, 64)
	go func() {
		defer close(ch)
		defer l.remove(sessionID, t)
		next := after
		for {
			batch, err := l.read(ctx, sessionID, next)
			for _, e := range batch {
				next = e.seq
				if e.topic == session.TopicRetry {
					continue
				}
				select {
				case ch <- e.ev:
				case <-ctx.Done():
					return
				}
			}
			if err == nil && len(batch) == tailBatch {
				continue
			}
			// A failed read is tried again soon, not only on the next event.
			var retry <-chan time.Time
			if err != nil {
				retry = time.After(listenRetry)
			}
			select {
			case <-retry:
			case <-t.wake:
			case <-t.closed:
				return
			case <-l.stopped:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

type logged struct {
	seq   session.Seq
	topic string
	ev    session.Event
}

// read is up to tailBatch of sessionID's events after after, in order. A
// failed read returns what it has; the tail tries again on its next wake.
func (l *EventLog) read(ctx context.Context, sessionID session.ID, after session.Seq) ([]logged, error) {
	// A failed query is Rows in an error state, which ForEachRow reports.
	rows, _ := l.db.pool.Query(ctx, l.db.sql(`
		SELECT seq, topic, event FROM {s}.session_events
		WHERE session_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`), sessionID, int64(after), tailBatch) //nolint:gosec // G115: seq is well below MaxInt64
	var out []logged
	var seq int64
	var e logged
	_, err := pgx.ForEachRow(rows, []any{&seq, &e.topic, &e.ev}, func() error {
		e.seq = session.Seq(seq) //nolint:gosec // G115: sequences start at 1 and only grow
		out = append(out, e)
		// The next row's event decodes into a zero Event, not onto this one.
		e.ev = session.Event{}
		return nil
	})
	return out, err
}

// Head implements session.EventLog. An unknown session is at 0.
func (l *EventLog) Head(ctx context.Context, sessionID session.ID) (session.Seq, error) {
	var seq int64
	err := l.db.pool.QueryRow(ctx, l.db.sql(`SELECT coalesce((SELECT seq FROM {s}.session_event_heads WHERE session_id = $1), 0)`), sessionID).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("event head %q: %w", sessionID, err)
	}
	return session.Seq(seq), nil //nolint:gosec // G115: sequences start at 1 and only grow
}

// Trim implements session.EventLog. The head row keeps the numbering.
func (l *EventLog) Trim(ctx context.Context, sessionID session.ID, through session.Seq) error {
	_, err := l.db.pool.Exec(ctx, l.db.sql(`DELETE FROM {s}.session_events WHERE session_id = $1 AND seq <= $2`),
		sessionID, int64(through)) //nolint:gosec // G115: seq is well below MaxInt64
	if err != nil {
		return fmt.Errorf("trim events %q: %w", sessionID, err)
	}
	return nil
}

// CloseSession implements session.EventLog. It deletes the session's log,
// so a later session can reuse the id, and ends its tails in every process.
func (l *EventLog) CloseSession(ctx context.Context, sessionID session.ID) error {
	_, err := l.db.pool.Exec(ctx, l.db.sql(`
		WITH events AS (DELETE FROM {s}.session_events WHERE session_id = $1),
		     head AS (DELETE FROM {s}.session_event_heads WHERE session_id = $1)
		SELECT pg_notify($2, 'c:' || $1)`), sessionID, l.db.channel)
	if err != nil {
		return fmt.Errorf("close events %q: %w", sessionID, err)
	}
	return nil
}

// Close stops the listener and ends every tail of this EventLog.
func (l *EventLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stop != nil {
		l.stop()
		l.stop = nil
	}
	select {
	case <-l.stopped:
	default:
		close(l.stopped)
	}
}

// add registers a tail of sessionID, starting the listener on first use.
func (l *EventLog) add(sessionID session.ID) *tail {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := &tail{wake: make(chan struct{}, 1), closed: make(chan struct{})}
	if l.tails[sessionID] == nil {
		l.tails[sessionID] = map[*tail]struct{}{}
	}
	l.tails[sessionID][t] = struct{}{}
	if l.stop == nil {
		select {
		case <-l.stopped:
		default:
			ctx, stop := context.WithCancel(context.Background())
			l.stop = stop
			go l.listen(ctx)
		}
	}
	return t
}

func (l *EventLog) remove(sessionID session.ID, t *tail) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.tails[sessionID], t)
	if len(l.tails[sessionID]) == 0 {
		delete(l.tails, sessionID)
	}
}

// listen holds one connection out of the pool that LISTENs on the log's
// channel and routes each notification to its session's tails. After it
// (re)connects it wakes every tail, so an event appended while it was not
// listening is read.
func (l *EventLog) listen(ctx context.Context) {
	for {
		conn, err := l.db.pool.Acquire(ctx)
		if err == nil {
			l.receive(ctx, conn.Hijack())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(listenRetry):
		}
	}
}

// receive LISTENs on conn until it fails or ctx ends, then closes it.
func (l *EventLog) receive(ctx context.Context, conn *pgx.Conn) {
	defer conn.Close(context.Background())
	_, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.db.channel}.Sanitize())
	if err == nil {
		l.wakeAll()
	}
	for err == nil {
		var n *pgconn.Notification
		if n, err = conn.WaitForNotification(ctx); err == nil {
			kind, id, _ := strings.Cut(n.Payload, ":")
			l.route(session.ID(id), kind == "c")
		}
	}
}

// route wakes sessionID's tails, or ends them when the session closed.
func (l *EventLog) route(sessionID session.ID, closed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for t := range l.tails[sessionID] {
		if closed {
			close(t.closed)
			delete(l.tails[sessionID], t)
			continue
		}
		select {
		case t.wake <- struct{}{}:
		default:
		}
	}
}

func (l *EventLog) wakeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, tails := range l.tails {
		for t := range tails {
			select {
			case t.wake <- struct{}{}:
			default:
			}
		}
	}
}

var _ session.EventLog = (*EventLog)(nil)
