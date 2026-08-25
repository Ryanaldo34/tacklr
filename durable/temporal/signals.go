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
)

// WorkflowInput is the typed start payload (no interface{}).
type WorkflowInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Mounts     []durable.MountRecipe
	Auth       durable.AuthContext
	// WorkerSessionTimeout, when > 0, pins the turn's activities to one worker
	// (Temporal CreateSession). Zero skips worker sessions: activities can run
	// on any worker. There is no default timeout.
	WorkerSessionTimeout time.Duration
	// Prompt, when set, runs one turn then completes the workflow (spawn_worker child).
	Prompt string
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
