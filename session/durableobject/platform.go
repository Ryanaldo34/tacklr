// Package durableobject runs Tacklr sessions as durable objects: one object
// per session, each with its own ordered storage, alarm and address, as a
// Cloudflare Durable Object has. The platform calls the session's Object for
// every request and alarm; Client is the session.Client hosts call.
//
// An object can stop at any moment and lose everything but its storage. Its
// session survives by journal replay: the object records the result of
// every orchestrator.Runtime call, and a restarted object runs orchestrator.Run again and
// answers from the journal until it reaches the first step that never
// finished. Wakes go into a mailbox in storage first, so none is lost, and
// an alarm restarts an object whose step stopped with it.
//
// The package is plain Go. A platform implements Host for each object and
// Platform for reaching the others; package cloudflare does it on Workers.
package durableobject

import (
	"context"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// Storage is one object's durable key-value storage, ordered by key. The
// object is its only writer.
type Storage interface {
	// Get is the value at key; ok is false when there is none.
	Get(key string) (value []byte, ok bool, err error)
	// Scan is the entries whose keys start with prefix and sort after after
	// (an empty after is the start), in key order: at most limit of them,
	// or all when limit is zero.
	Scan(prefix, after string, limit int) ([]Entry, error)
	// Write applies ops in order, all of them or none.
	Write(ops ...Op) error
}

// Entry is one stored key and its value.
type Entry struct {
	Key   string
	Value []byte
}

// Op is one write: a put of Value at Key, or a delete when Delete is set.
type Op struct {
	Key    string
	Value  []byte
	Delete bool
}

// Ref is a handle on one session's object, wherever it runs: *Object is
// one, and a platform's stub for a remote object is another.
type Ref interface {
	// Create starts the session. It is session.ErrExists when the object
	// already holds one.
	Create(ctx context.Context, init Init) error
	// Wake queues w for the session. It is session.ErrNotFound when the
	// object holds no session, or one that ended.
	Wake(ctx context.Context, w session.Wake) error
	Status(ctx context.Context) (session.Status, error)
	// Jobs is the session's running jobs, in start order.
	Jobs(ctx context.Context) ([]session.ID, error)
	Head(ctx context.Context) (session.Seq, error)
	Subscribe(ctx context.Context, after session.Seq) (session.Subscription, error)
	// Wire is the wire state a protocol server keeps with the session
	// (the payload of server.ProtocolWireStore). It is session.ErrNotFound
	// when the object holds no session, or no wire state.
	Wire(ctx context.Context) ([]byte, error)
	// SetWire replaces the session's wire state; nil deletes it. A close
	// of the session deletes it too.
	SetWire(ctx context.Context, raw []byte) error
}

// Platform reaches session objects by id. An object starts on first use.
type Platform interface {
	Object(id session.ID) Ref
}

// Host is what the platform gives one object: its storage, its alarm, and
// the other objects.
type Host interface {
	Storage
	Platform
	// SetAlarm asks the platform to call Object.Alarm at t. The zero time
	// clears the alarm.
	SetAlarm(t time.Time) error
}

// Init is what a session starts from: who it is and, for a child, its
// task.
type Init struct {
	SessionID session.ID
	// Trace is the context of the parent turn that started a task session:
	// the child's turn is part of the parent's trace.
	Trace      telemetry.Carrier `json:",omitempty"`
	Specialist string
	// Worker names the JobHandler a job session runs.
	Worker     string
	Parent     session.ID
	Mounts     []session.MountRecipe
	MCPServers []session.MCPServer
	State      map[string]any
	// Task is a child's prompt: the session runs that one turn and ends.
	Task string
	// Auth is the credentials the session starts with; a child gets its
	// parent's. The object seals them and keeps them out of its journal.
	Auth session.AuthContext
}
