package temporal

import (
	"time"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
)

const (
	signalPrompt = "Prompt"
	signalResume = "Resume"
	signalCancel = "Cancel"
	signalClose  = "Close"

	queryStatus   = "tacklr_status"
	queryChildren = "tacklr_children"

	signalChildWaiting = "ChildWaiting"
)

// WorkflowInput is the typed start payload (no interface{}). Auth is
// secret-free on the wire; credentials live in SecretStorage.
type WorkflowInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Mounts     []durable.MountRecipe
	Auth       durable.AuthContext
	// TurnLocalityTimeout, when > 0, pins the turn's activities to one worker
	// (Temporal CreateSession). Zero skips worker sessions: activities can run
	// on any worker. There is no default timeout.
	TurnLocalityTimeout time.Duration
	// ActivityTimeout is StartToCloseTimeout for Inference/Tool activities.
	// Zero means 10 minutes (resolveActivityTimeout).
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero means 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts. Zero means 3.
	ActivityAttempts int32
	// Prompt, when set, runs one turn then completes the workflow (spawn_specialist child).
	Prompt     string
	Parent     durable.SessionID
	Specialist string
	// State is CreateSession.State, already JSON-roundtripped by Runtime.CreateSession.
	State map[string]any
}

// promptSignal is the Prompt signal. Auth is secret-free on the wire.
type promptSignal struct {
	Text        string
	UserMessage *tacklr.Message
	AgentID     string
	MCPServers  []mcp.MCPConfig
	Auth        durable.AuthContext
	State       map[string]any
}

// resumeSignal is the Resume signal. Auth is secret-free on the wire.
type resumeSignal struct {
	Responses map[string][]byte
	Auth      durable.AuthContext
	State     map[string]any
}

type waitSignal struct {
	kind   string
	prompt promptSignal
	resume resumeSignal
}
