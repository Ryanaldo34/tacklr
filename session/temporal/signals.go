package temporal

import (
	"time"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
)

const (
	// signalWake carries every session.Wake (prompt, resume, cancel, close) on
	// one channel, so the workflow handles them in the order they were sent.
	signalWake = "tacklr_wake"

	queryStatus   = "tacklr_status"
	queryChildren = "tacklr_children"

	// signalChildWaiting only wakes a parent whose child has parked.
	// It is not a session wake.
	signalChildWaiting = "child-waiting"
)

// workflowInput is wait-loop start state (scheduler), not a Snapshot.
// Credentials live in SecretStorage. userState seed merges into the
// checkpoint on the first activity save.
type workflowInput struct {
	SessionID  session.SessionID
	MCPServers []mcp.MCPConfig
	Mounts     []session.MountRecipe
	// TurnLocalityTimeout, when > 0, pins the turn's activities to one worker
	// (Temporal CreateSession). Zero skips worker sessions: activities can run
	// on any worker. There is no default timeout.
	TurnLocalityTimeout time.Duration
	// ActivityTimeout is StartToCloseTimeout for Inference/Tool activities.
	// Zero means 10 minutes.
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero means 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero means 3.
	ActivityAttempts int32
	// Prompt, when set, runs one turn then completes the workflow (spawn_specialist child).
	Prompt     string
	Parent     session.SessionID
	Specialist string
	Worker     string
	// State is CreateSession.State, already JSON-roundtripped. Overlay onto
	// the checkpoint; not a durable workflow copy of userState.
	State map[string]any
}
