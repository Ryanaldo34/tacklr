package session

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/message"
)

// SessionID is the durable agent session identifier.
type ID string

// Seq is a monotonically increasing EventLog offset for one session.
type Seq uint64

// AuthContext is credentials and mount intent for one work item (Prompt, Resume,
// or a child session). Protocols map their wire auth into this type.
// Autonomous hosts set it on the payload that queues the work. Tokens are not
// stored in the snapshot or the durable history. The runtime writes them to
// SecretStorage before the work item runs.
type AuthContext struct {
	// Bindings are this slice's mounts and/or provider tokens. A binding with
	// an alias upserts the recipe. A binding with only provider+token refreshes
	// every cached recipe for that provider.
	Bindings []mount.Binding `json:"bindings,omitempty"`
	// Drop removes cached recipes by alias or provider. Applied before Bindings.
	Drop []string `json:"drop,omitempty"`
}

// MountRecipe is secret-free VFS context remembered across turns.
// It records where a mount came from (provider, alias, backend ids). File
// contents are never stored; providers lazy-load on open/read.
type MountRecipe struct {
	Provider  string            `json:"provider"`
	Alias     string            `json:"alias"`
	Params    map[string]string `json:"params,omitempty"`
	SourceIDs []string          `json:"sourceIds,omitempty"`
	Writable  bool              `json:"writable,omitempty"`
}

// CreateSession is the typed input for Runtime.CreateSession.
type CreateSession struct {
	SessionID  ID
	MCPServers []MCPServer
	// Mounts seeds the session recipe cache (no secrets). Tokens arrive on Prompt.
	Mounts []MountRecipe
	// Parent, when set, makes this a child session of that parent. The child
	// reuses the same wait loop. Empty MCPServers/Mounts inherit from parent.
	Parent ID
	// Specialist selects an assistant on the agent's Specialists list.
	// Required with Parent for spawn_specialist children.
	Specialist string
	// Worker selects a named job handler. Exclusive with Specialist.
	// The child session runs that handler instead of a model.
	Worker string
	// State is host values for the session's user state
	// (JSON-serializable). The first step writes them into the checkpoint;
	// tools read them with CallContext.StateGet. No tokens or clients.
	// Child sessions do not inherit this map.
	State map[string]any
}

// Prompt is the typed input for Runtime.Prompt.
type Prompt struct {
	Text        string
	UserMessage *message.Message
	// MCPServers, when non-nil, replaces session-scoped MCP configs for this turn.
	MCPServers []MCPServer
	Auth       AuthContext
	// State is host values the turn's first step writes into the
	// session's user state. JSON-serializable values only. No tokens or
	// clients.
	State map[string]any
}

// Resume is the typed input for Runtime.Resume (HITL answer plus optional auth).
type Resume struct {
	Responses map[string][]byte
	Auth      AuthContext
	// State is host values the resumed step writes into the session's
	// user state.
	State map[string]any
}

// SessionState is parent-facing session/job state. Child HITL does not change
// this from running until the interrupt is resolved and the child completes,
// fails, or is cancelled.
type State string

const (
	Running  State = "running"
	Complete State = "complete"
	Failed   State = "failed"
	Unknown  State = "unknown"
)

const (
	// SessionKindSpecialist is Status.Kind for spawn_specialist children.
	KindSpecialist = "specialist"
	// SessionKindWorker is Status.Kind for a named job-handler child.
	KindWorker = "worker"
)

// SessionStatus is a value type returned by Runtime.Status. Not an interface.
type Status struct {
	ID         ID
	Parent     ID
	State      State
	Specialist string
	Kind       string
	Result     string
	// Waiting is true while the session is parked for HITL. Parent-facing
	// State stays running until that interrupt is resolved.
	Waiting bool
}

// EventLog topics.
const (
	TopicEvents = "events"
	TopicRetry  = "retry"
)

var (
	// ErrNotFound is a session that is unknown, closed, or already torn down.
	ErrNotFound = errors.New("session not found")
	// ErrExists is CreateSession with an id that is already live.
	ErrExists = errors.New("session already exists")
	// ErrInvalid is a request a Client rejects before it reaches a session:
	// state JSON cannot hold, or a specialist and a worker together.
	ErrInvalid = errors.New("invalid session request")
	// ErrStaleCheckpoint is Store.Save when expected Revision does not
	// match the row (another writer already saved). Reload and retry.
	ErrStaleCheckpoint = errors.New("stale checkpoint")
)

// EventLog is the session progress stream. Topics are TopicEvents and
// TopicRetry (a repeated attempt). It holds durable events only, never a
// Delta, and only the session's current turn: the runtime trims the turns
// before it when a new turn starts.
type EventLog interface {
	Append(ctx context.Context, sessionID ID, topic string, ev Event) error
	// Subscribe sends the events after after, then each new one. When
	// after is before the oldest event kept, it starts at that event.
	Subscribe(ctx context.Context, sessionID ID, after Seq) (<-chan Event, error)
	Head(ctx context.Context, sessionID ID) (Seq, error)
	// Trim deletes the events up to and including through. The numbers of
	// the events that stay, and of later ones, do not change.
	Trim(ctx context.Context, sessionID ID, through Seq) error
	CloseSession(ctx context.Context, sessionID ID) error
}

// Revision is the Store compare-and-swap token for one session row.
// The zero value means no row exists yet; the next Save creates it.
type Revision string

// Store keeps each session's checkpoint: what the harness needs to
// think again after a park or a worker recycle, as rows (CheckpointEntry),
// so a commit writes only the rows it changed. Tokens, file bytes, MCP env
// and headers, the session's identity, and child handles are not in it.
//
// Save's expected Revision must match the last Load (zero if no row).
// Mismatch means another writer already saved — reload and retry.
type Store interface {
	// Save applies changes to the checkpoint, all of them or none, when it
	// is at expected, and returns its new revision.
	Save(ctx context.Context, sessionID ID, changes []CheckpointChange, expected Revision) (Revision, error)
	// Load is the checkpoint's rows, in no set order, and its revision.
	Load(ctx context.Context, sessionID ID) ([]CheckpointEntry, Revision, error)
	// Revision is the snapshot's current revision, "" when none exists. A
	// worker reads it to learn whether the committed state it holds is
	// current, without loading the snapshot.
	Revision(ctx context.Context, sessionID ID) (Revision, error)
	// PutCall keeps the body of one tool call (tacklr.CallBody) under its
	// session and call key, so the durable runtime's history holds only
	// that the call ran. A Put of a key replaces it: a step that runs again
	// leaves one. Bodies stay until ClearCalls or Delete, so a Subscribe in
	// the turn can read every result.
	PutCall(ctx context.Context, sessionID ID, callKey string, body []byte) error
	// Calls returns the bodies kept under callKeys; a key with none is
	// absent from the map.
	Calls(ctx context.Context, sessionID ID, callKeys []string) (map[string][]byte, error)
	// ClearCalls removes the session's call bodies. The runtime calls it
	// when a new turn starts: the turns before it committed their rounds,
	// so no step reads their calls again.
	ClearCalls(ctx context.Context, sessionID ID) error
	// Delete removes the session's snapshot and its call bodies.
	Delete(ctx context.Context, sessionID ID) error
}

// WithoutSecrets returns a copy with Credential Token and ExpiresAt cleared.
// Binding metadata (provider, alias, params, writable) is kept. The input is
// not modified.
func (a AuthContext) WithoutSecrets() AuthContext {
	out := cloneAuth(a)
	for i := range out.Bindings {
		out.Bindings[i].Auth = mount.Credential{}
	}
	return out
}

func cloneAuth(a AuthContext) AuthContext {
	out := AuthContext{Drop: slices.Clone(a.Drop)}
	if len(a.Bindings) == 0 {
		return out
	}
	out.Bindings = make([]mount.Binding, len(a.Bindings))
	for i, b := range a.Bindings {
		b.Params = maps.Clone(b.Params)
		out.Bindings[i] = b
	}
	return out
}

// ErrModelRefused and ErrMaxTokens are why a turn ended that a client acts
// on (a refusal, the token limit); its error event names them.
var (
	ErrModelRefused = errors.New("model refused")
	ErrMaxTokens    = errors.New("max tokens reached")
)
