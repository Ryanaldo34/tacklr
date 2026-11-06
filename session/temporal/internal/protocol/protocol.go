// Package protocol is how a Temporal client and worker talk about one
// session: the workflow's type name and input, its signal and query names,
// and the item its event stream carries.
package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const (
	// Workflow is the session workflow's type name: a client starts it by
	// name, a worker registers it.
	Workflow = "SessionWorkflow"
	// TaskQueue is the queue sessions start on when the host names none.
	TaskQueue = "tacklr"
)

const (
	// SignalWake carries every session.Wake (prompt, resume, cancel, close) on
	// one channel, so the workflow handles them in the order they were sent.
	SignalWake = "tacklr_wake"

	// QueryStatus answers the session's session.Status.
	QueryStatus = "tacklr_status"
	// QueryJobs answers the IDs of the session's jobs.
	QueryJobs = "tacklr_jobs"
	// QueryLive answers the live URL of the worker that holds the
	// session's harness, where its deltas stream; "" until it is routed.
	QueryLive = "tacklr_live"
)

// Route is the worker that holds a session's harness: its own task queue,
// and the URL of its live endpoint ("" when it serves none).
type Route struct {
	Queue string
	Live  string
}

// LivePath is where a worker's live endpoint serves a session's deltas:
// LivePath followed by the session id.
const LivePath = "/tacklr/live/"

// LiveToken is the bearer token for id's deltas: hex HMAC-SHA256 of the id
// under the key clients and workers share.
func LiveToken(key []byte, id session.ID) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id))
	return hex.EncodeToString(mac.Sum(nil))
}

// Input is the session workflow's start state, not a checkpoint.
// Credentials live in SecretStorage. State merges into the checkpoint on
// the first activity save.
type Input struct {
	SessionID  session.ID
	MCPServers []session.MCPServer
	Mounts     []session.MountRecipe
	// WorkerQueueTimeout is the ScheduleToStart timeout of a step sent to the
	// worker that holds the session's harness.
	WorkerQueueTimeout time.Duration
	// ActivityTimeout is StartToCloseTimeout for Inference/Tool activities.
	// Zero means 10 minutes.
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero means 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero means 3.
	ActivityAttempts int32
	// Task, when set, makes a task session (a child a tool started): its
	// first prompt is Task, and the workflow completes when that turn ends.
	Task string
	// Trace is the context of the parent turn that started a task
	// session: the child's turn is part of the parent's trace.
	Trace      telemetry.Carrier
	Parent     session.ID
	Specialist string
	Worker     string
	// State is CreateSession.State, already JSON-roundtripped. Overlay onto
	// the checkpoint; not a durable workflow copy of userState.
	State map[string]any
	// MaxToolCalls caps the Tool steps of one reply that run at once;
	// MaxChildren the children a session runs at once.
	MaxToolCalls int
	MaxChildren  int
	// ContinueAsNewEvents, when > 0, continues the session as new once its
	// history has that many events; zero follows Temporal's suggestion.
	ContinueAsNewEvents int
	// Carry is the state an earlier run of this session handed on when it
	// continued as new; nil on the first run.
	Carry json.RawMessage `json:",omitempty"`
}

// StreamItem is one event on a session's workflow stream. A tool result's
// content stays in the session.Store, out of history: Call is the key of
// the call whose body holds it, and Subscribe puts it back. The event is a
// named field: embedded, its UnmarshalJSON would decode the whole item.
type StreamItem struct {
	Event session.Event `json:"event"`
	Call  string        `json:"call,omitempty"`
}
