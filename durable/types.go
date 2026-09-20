package durable

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/vfs"
)

// SessionID is the durable agent session identifier.
type SessionID string

// Seq is a monotonically increasing EventLog offset for one session.
type Seq uint64

// AuthContext is credentials and mount intent for one work item (Prompt, Resume,
// or a one-shot child workflow). Protocols map their wire auth into this type.
// Autonomous hosts set it on the payload that queues the work. Tokens are not
// stored in SnapshotStore or Temporal event history. The Temporal adapter
// writes them to SecretStorage before signaling.
type AuthContext struct {
	// Bindings are this slice's mounts and/or provider tokens. A binding with
	// an alias upserts the recipe. A binding with only provider+token refreshes
	// every cached recipe for that provider.
	Bindings []vfs.Binding `json:"bindings,omitempty"`
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
	AgentID    string
	SessionID  SessionID
	MCPServers []mcp.MCPConfig
	// Mounts seeds the session recipe cache (no secrets). Tokens arrive on Prompt.
	Mounts []MountRecipe
	// Parent, when set, makes this a child session of that parent. The child
	// reuses the same wait loop. Empty MCPServers/Mounts inherit from parent.
	Parent SessionID
	// Specialist selects a Specialist from the parent's catalog spec. Required
	// with Parent for spawn_specialist children. The host does not register the
	// worker as a top-level catalog agent.
	Specialist string
	// Worker selects a named JobHandler on Runtime Config.Jobs. Exclusive
	// with Specialist. The child session runs that handler instead of a model.
	Worker string
	// State seeds checkpoint userState (JSON-serializable values).
	// Tools read it via HarnessRuntime.StateGet. Canonical copy is
	// Snapshot.Checkpoint, not Temporal workflow variables. No tokens or clients.
	// Child sessions do not inherit this map.
	State map[string]any
}

// Prompt is the typed input for Runtime.Prompt.
type Prompt struct {
	Text        string
	UserMessage *tacklr.Message
	// AgentID, when set, selects the catalog agent for this turn slice.
	AgentID string
	// MCPServers, when non-nil, replaces session-scoped MCP configs for this turn.
	MCPServers []mcp.MCPConfig
	Auth       AuthContext
	// State merges into checkpoint userState for this turn after restore.
	// JSON-serializable values only. No tokens or clients.
	State map[string]any
}

// Resume is the typed input for Runtime.Resume (HITL answer plus optional auth).
type Resume struct {
	Responses map[string][]byte
	Auth      AuthContext
	// State merges into checkpoint userState when the parked turn continues.
	State map[string]any
}

// Snapshot is the session record in SnapshotStore. Both runtimes write the
// same shape. Wait-loop fields (leftover Temporal tool calls, MCP overlay,
// child futures) stay on the loop, not here.
type Snapshot struct {
	AgentID    string
	Specialist string
	Parent     SessionID
	// Children are child session ids in start order (no handles, no tokens).
	Children   []SessionID
	Checkpoint tacklr.SessionCheckpoint
	Mounts     []MountRecipe
	// Worker, when set, means this session is a named JobHandler child.
	Worker string
}

// SessionState is parent-facing session/job state. Child HITL does not change
// this from running until the interrupt is resolved and the child completes,
// fails, or is cancelled.
type SessionState string

const (
	SessionRunning  SessionState = "running"
	SessionComplete SessionState = "complete"
	SessionFailed   SessionState = "failed"
	SessionUnknown  SessionState = "unknown"
)

const (
	// SessionKindSpecialist is Status.Kind for spawn_specialist children.
	SessionKindSpecialist = "specialist"
	// SessionKindWorker is Status.Kind for Config.Jobs children.
	SessionKindWorker = "worker"
)

// SessionStatus is a value type returned by Runtime.Status. Not an interface.
type SessionStatus struct {
	ID         SessionID
	Parent     SessionID
	State      SessionState
	Specialist string
	Kind       string
	Result     string
	Err        error
	// Waiting is true while the session is parked for HITL. Parent-facing
	// State stays running until that interrupt is resolved.
	Waiting bool
}

// EventLog topics. Temporal Workflow Streams uses the same names.
const (
	TopicEvents = "events"
	TopicRetry  = "retry"
)

var (
	// ErrSessionNotFound is unknown, closed, or already torn down.
	ErrSessionNotFound = errors.New("session not found")
	// ErrSessionExists is CreateSession with an id that is already live.
	ErrSessionExists = errors.New("session already exists")
	// ErrAgentNotFound is Catalog miss.
	ErrAgentNotFound = errors.New("agent not found")
	// ErrStaleCheckpoint is SnapshotStore.Save when expected Revision does not
	// match the row (another writer already saved). Reload and retry.
	ErrStaleCheckpoint = errors.New("stale checkpoint")
)

// JobHandler runs a named background job. Register on inprocess/temporal Config.Jobs.
type JobHandler func(ctx context.Context, task string) (string, error)

// ChildSessionID is the stable id for a spawn_specialist child session.
func ChildSessionID(parent SessionID, specialist, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/w/%s/%s", parent, strings.TrimSpace(specialist), strings.TrimSpace(callID)))
}

// JobID is the stable id for a named background job (not a child session).
func JobID(parent SessionID, name, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/j/%s/%s", parent, strings.TrimSpace(name), strings.TrimSpace(callID)))
}

// EventLog is the portable progress stream. Temporal implements it with
// Workflow Streams. In-process uses a memory channel. Topics are TopicEvents
// and TopicRetry (activity attempt > 1).
type EventLog interface {
	Append(ctx context.Context, sessionID SessionID, topic string, ev tacklr.StreamEvent) error
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (<-chan tacklr.StreamEvent, error)
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	CloseSession(ctx context.Context, sessionID SessionID) error
}

// Revision is the SnapshotStore compare-and-swap token for one session row.
// The zero value means no row exists yet; the next Save creates it.
type Revision string

// SnapshotStore is the session record: what the harness needs to think again
// after HITL or a worker recycle. It is not the wait loop and not credentials.
//
// Frozen contents: SessionCheckpoint (window, plan, parked interrupt, userState),
// MountRecipe topology, and session identity (agent, parent, specialist, child
// ids). Tokens, file bytes, leftover unstarted Temporal tool calls, MCP env
// and headers, and child workflow futures never go here.
//
// Save's expected Revision must match the last Load (zero if no row).
// Mismatch means another writer already saved — reload and retry.
type SnapshotStore interface {
	Save(ctx context.Context, sessionID SessionID, snap Snapshot, expected Revision) (Revision, error)
	Load(ctx context.Context, sessionID SessionID) (Snapshot, Revision, error)
	Delete(ctx context.Context, sessionID SessionID) error
}

// WithoutSecrets returns a copy with Credential Token and ExpiresAt cleared.
// Binding metadata (provider, alias, params, writable) is kept. The input is
// not modified.
func (a AuthContext) WithoutSecrets() AuthContext {
	out := cloneAuth(a)
	for i := range out.Bindings {
		out.Bindings[i].Auth = vfs.Credential{}
	}
	return out
}

func cloneAuth(a AuthContext) AuthContext {
	out := AuthContext{Drop: slices.Clone(a.Drop)}
	if len(a.Bindings) == 0 {
		return out
	}
	out.Bindings = make([]vfs.Binding, len(a.Bindings))
	for i, b := range a.Bindings {
		b.Params = maps.Clone(b.Params)
		b.Live = nil
		out.Bindings[i] = b
	}
	return out
}
