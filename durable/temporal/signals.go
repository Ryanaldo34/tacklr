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

// WorkflowInput is the typed start payload (no interface{}).
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
	// Prompt, when set, runs one turn then completes the workflow (spawn_specialist child).
	Prompt     string
	Parent     durable.SessionID
	Specialist string
	// State is CreateSession.State, already JSON-roundtripped by Runtime.CreateSession.
	State map[string]any
}

type promptSignal struct {
	Text        string
	UserMessage *tacklr.Message
	AgentID     string
	MCPServers  []mcp.MCPConfig
	Auth        durable.AuthContext
	State       map[string]any
}

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
