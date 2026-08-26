package temporal

import (
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
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
}

type promptSignal struct {
	Text        string
	UserMessage *streaming.Message
	AgentID     string
	MCPServers  []mcp.MCPConfig
	Auth        durable.AuthContext
}

type resumeSignal struct {
	Responses map[string][]byte
	Auth      durable.AuthContext
}

type waitSignal struct {
	kind   string
	prompt promptSignal
	resume resumeSignal
}
