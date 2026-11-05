package session

import (
	"context"
	"sync"
)

// liveBuffer is how many deltas a live reader can be behind before it
// misses some.
const liveBuffer = 256

// Live sends a session's deltas to the readers that listen now. It stores
// nothing: a reader that was not listening, or fell behind, misses deltas,
// and the complete event it reads from the session's log has the full text.
// A runtime keeps one Live where its model calls run.
type Live struct {
	mu      sync.Mutex
	readers map[ID]map[chan Event]struct{}
}

// NewLive returns a Live with no readers.
func NewLive() *Live {
	return &Live{readers: map[ID]map[chan Event]struct{}{}}
}

// Publish sends ev to each reader of id that has room for it. It never
// blocks.
func (l *Live) Publish(id ID, ev Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.readers[id] {
		select {
		case ch <- ev:
		default:
		}
	}
}

// Subscribe is id's deltas from now until ctx ends, when the channel
// closes.
func (l *Live) Subscribe(ctx context.Context, id ID) <-chan Event {
	ch := make(chan Event, liveBuffer)
	l.mu.Lock()
	if l.readers[id] == nil {
		l.readers[id] = map[chan Event]struct{}{}
	}
	l.readers[id][ch] = struct{}{}
	l.mu.Unlock()
	context.AfterFunc(ctx, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.readers[id], ch)
		if len(l.readers[id]) == 0 {
			delete(l.readers, id)
		}
		close(ch)
	})
	return ch
}

// Merge is one stream of a session's events: the events of its log, and
// its live deltas between them. Of each message's deltas, it sends only
// the one that starts where the text sent so far ends, and none after the
// message's complete event: a reader gets each byte once, in order, and
// the complete event has the rest. The stream ends when log closes or ctx
// ends. live may be nil.
func Merge(ctx context.Context, log, live <-chan Event) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		texts := map[string]*text{}
		for {
			var ev Event
			var ok bool
			// Deltas that are already here go first: in one process they
			// were sent before the complete event the log has next.
			select {
			case ev, ok = <-live:
			default:
				select {
				case ev, ok = <-log:
					if !ok {
						return
					}
				case ev, ok = <-live:
				case <-ctx.Done():
					return
				}
			}
			if !ok {
				live = nil
				continue
			}
			if !sendable(texts, ev) {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// text is what Merge sent of one message: its length so far, and whether
// its complete event went out.
type text struct {
	sent int
	done bool
}

// sendable reports whether Merge sends ev, and notes what it sends. A turn's
// end forgets the turn's messages.
func sendable(texts map[string]*text, ev Event) bool {
	switch ev.Type {
	case EventComplete, EventError, EventInterrupt:
		clear(texts)
		return true
	case EventMessage, EventReasoning:
	default:
		return true
	}
	k := string(ev.Type) + ":" + ev.MessageID
	t := texts[k]
	if t == nil {
		t = &text{}
		texts[k] = t
	}
	if !ev.Delta {
		t.done = true
		return true
	}
	if t.done || ev.Offset != t.sent {
		return false
	}
	t.sent += len(ev.Content)
	return true
}
