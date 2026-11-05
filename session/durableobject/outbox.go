package durableobject

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// letter is one thing an object owes the world outside its storage: a
// child's start, a wake for another object, or the deletion of a session's
// messages from the brain. A step writes its letters with its journal
// entry, and the object delivers them after, in order, until each is done,
// so none is lost when the object stops. Every one is safe to deliver
// again: a start of a session that exists, a wake of one that is gone, a
// job end its parent already took, and a delete that already happened are
// not errors.
type letter struct {
	To session.ID `json:"to,omitempty"`
	// Create starts To with Init; Auth is the credentials it starts with,
	// sealed for To, since a letter is stored.
	Create *Init  `json:"create,omitempty"`
	Auth   []byte `json:"auth,omitempty"`
	// Wake queues a wake for To.
	Wake *session.Wake `json:"wake,omitempty"`
	// Forget deletes the session's messages from the agent's brain.
	Forget session.ID `json:"forget,omitempty"`
}

// outboxPrefix is where an object keeps its letters, by sequence number.
const outboxPrefix = kindOutbox + "/"

// outbox is an object's letters not yet delivered, in memory as in
// storage.
type outbox struct {
	next    uint64
	queue   []posted
	sending bool
	// retry is when the alarm delivers again after a failure (zero: no
	// failure waits), and wait how long the next failure waits.
	retry time.Time
	wait  time.Duration
}

// posted is a letter and its key.
type posted struct {
	key string
	l   letter
}

// load reads the letters in storage.
func (b *outbox) load(s Storage) error {
	b.queue = nil
	return scanAll(s, outboxPrefix, func(e Entry) error {
		var l letter
		if err := json.Unmarshal(e.Value, &l); err != nil {
			return fmt.Errorf("outbox entry %s: %w", e.Key, err)
		}
		b.queue = append(b.queue, posted{key: e.Key, l: l})
		b.next = seqOf(e.Key) + 1
		return nil
	})
}

// post is the writes of ls, numbered after the letters before them. Call
// it with the object's lock held, and sent once the write is done.
func (b *outbox) post(ls ...letter) []Op {
	ops := make([]Op, len(ls))
	for i, l := range ls {
		raw, _ := json.Marshal(l) // ids, plain data and sealed bytes
		ops[i] = put(fmt.Sprintf("%s%016x", outboxPrefix, b.next), raw)
		b.next++
	}
	return ops
}

// sent queues the letters whose writes ops are, once those are in storage.
func (b *outbox) sent(ops []Op) {
	for _, op := range ops {
		var l letter
		_ = json.Unmarshal(op.Value, &l) // post encoded it
		b.queue = append(b.queue, posted{key: op.Key, l: l})
	}
}

// sendLocked starts delivering the letters, unless that runs. Call it with
// o.mu held.
func (o *Object) sendLocked() {
	if o.out.sending || len(o.out.queue) == 0 || o.ctx.Err() != nil {
		return
	}
	o.out.sending = true
	go o.send()
}

// send delivers the letters in order, deleting each once it is done. A
// failure stops it until the retry: the alarm then, or sooner a timer.
func (o *Object) send() {
	for {
		o.mu.Lock()
		if len(o.out.queue) == 0 {
			o.out.sending, o.out.retry, o.out.wait = false, time.Time{}, 0
			o.armLocked()
			o.settle()
			o.mu.Unlock()
			return
		}
		p := o.out.queue[0]
		o.mu.Unlock()
		err := o.deliver(p.l)
		if err == nil {
			err = o.host.Write(del(p.key))
		}
		o.mu.Lock()
		if err != nil {
			slog.Warn("deliver to object", "area", telemetry.AreaRuntime, "to", p.l.To, "error", err)
			o.out.wait = min(max(o.out.wait*2, o.cfg.RetryBackoff), retryBackoffMax)
			o.out.retry, o.out.sending = time.Now().Add(o.out.wait), false
			o.armLocked()
			o.settle()
			wait := o.out.wait
			o.mu.Unlock()
			time.AfterFunc(wait, func() {
				o.mu.Lock()
				defer o.mu.Unlock()
				o.sendLocked()
			})
			return
		}
		o.out.queue = o.out.queue[1:]
		o.out.wait = 0
		o.mu.Unlock()
	}
}

// deliver does what l asks.
func (o *Object) deliver(l letter) error {
	ctx := o.ctx
	switch {
	case l.Create != nil:
		init := *l.Create
		if len(l.Auth) > 0 {
			plain, err := o.cfg.Sealer.Open(ctx, l.Auth, []byte(l.To))
			if err != nil {
				return err
			}
			var sec session.Secrets
			if err := json.Unmarshal(plain, &sec); err != nil {
				return err
			}
			init.Auth = sec.Auth
		}
		if err := o.host.Object(l.To).Create(ctx, init); !errors.Is(err, session.ErrExists) {
			return err
		}
		return nil
	case l.Wake != nil:
		if err := o.host.Object(l.To).Wake(ctx, *l.Wake); !errors.Is(err, session.ErrNotFound) {
			return err
		}
		return nil
	}
	return o.cfg.Agent.Brain.DeleteSessionMessages(ctx, o.cfg.Agent.SearchNamespace, string(l.Forget))
}

// armLocked sets the platform's one alarm to the earliest of what needs
// it: the watchdog of a running step, and the retry of a failed delivery.
// Call it with o.mu held.
func (o *Object) armLocked() {
	at := o.watchdog
	if r := o.out.retry; !r.IsZero() && (at.IsZero() || r.Before(at)) {
		at = r
	}
	if at.Equal(o.armed) {
		return
	}
	if err := o.host.SetAlarm(at); err == nil {
		o.armed = at
	}
}
