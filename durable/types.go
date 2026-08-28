package durable

import (
	"errors"

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
// stored in SnapshotStore.
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
	// State seeds host-owned session userState (JSON-serializable values).
	// Tools read it via HarnessRuntime.StateGet. It is checkpointed — do not
	// store tokens or clients. Child sessions do not inherit this map.
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
	// State merges into session userState for this turn (after checkpoint restore).
	// JSON-serializable values only. Checkpointed — no tokens or clients.
	State map[string]any
}

// Resume is the typed input for Runtime.Resume (HITL answer plus optional auth).
type Resume struct {
	Responses map[string][]byte
	Auth      AuthContext
	// State merges into session userState when the parked turn continues.
	State map[string]any
}

// Snapshot is one session's harness checkpoint plus VFS recipes (no tokens).
type Snapshot struct {
	AgentID    string
	Specialist string
	Parent     SessionID
	// Children are child session ids in start order (no handles, no tokens).
	Children   []SessionID
	Checkpoint tacklr.SessionCheckpoint
	Mounts     []MountRecipe
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

// SessionKindSpecialist is Status.Kind for spawn_specialist children.
const SessionKindSpecialist = "specialist"

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
