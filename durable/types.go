package durable

import (
	"errors"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
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
}

// Prompt is the typed input for Runtime.Prompt.
type Prompt struct {
	Text        string
	UserMessage *streaming.Message
	// AgentID, when set, selects the catalog agent for this turn slice.
	AgentID string
	// MCPServers, when non-nil, replaces session-scoped MCP configs for this turn.
	MCPServers []mcp.MCPConfig
	Auth       AuthContext
}

// Resume is the typed input for Runtime.Resume (HITL answer plus optional auth).
type Resume struct {
	Responses map[string][]byte
	Auth      AuthContext
}

// Snapshot is one session's harness checkpoint plus VFS recipes (no tokens).
type Snapshot struct {
	AgentID    string
	Checkpoint stores.SessionCheckpoint
	Mounts     []MountRecipe
}

// EventLog topics. Temporal Workflow Streams uses the same names.
const (
	TopicEvents = "events"
	TopicRetry  = "retry"
	TopicClose  = "close"
)

var (
	// ErrSessionNotFound is returned when a session id is unknown or was closed.
	ErrSessionNotFound = errors.New("session not found")
	// ErrSessionExists is returned when CreateSession is called with an id that is live.
	ErrSessionExists = errors.New("session already exists")
	// ErrAgentNotFound is returned when Catalog has no such agent.
	ErrAgentNotFound = errors.New("agent not found")
	// ErrSessionClosed is returned when a signal targets a session that is shutting down.
	ErrSessionClosed = errors.New("session closed")
	// ErrEtagMismatch is returned when SnapshotStore.Save sees a stale etag.
	ErrEtagMismatch = errors.New("snapshot: etag mismatch")
)
