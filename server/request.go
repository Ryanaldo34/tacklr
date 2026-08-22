package server

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
)

// TurnRequest describes a prompt or resume turn after Protocol.BindTurn.
type TurnRequest struct {
	SessionID              string
	AgentID                string
	ThreadID               string
	Prompt                 string
	UserMessage            *tacklr.Message
	Responses              map[string]json.RawMessage
	Load                   bool
	AllowMissingCheckpoint bool
	CWD                    string
	MCPServers             []mcp.MCPConfig
	Auth                   durable.AuthContext
}

// turnRequest is the common payload for both SSE and WebSocket prompt/resume
// endpoints. Fields are omitempty so the same shape can be reused across all
// four handlers while each endpoint validates only the fields it cares about.
type turnRequest struct {
	AgentID   string                     `json:"agent_id"`
	ThreadID  string                     `json:"thread_id,omitempty"`
	Prompt    string                     `json:"prompt,omitempty"`
	Responses map[string]json.RawMessage `json:"responses,omitempty"`
	Auth      *durable.AuthContext       `json:"auth,omitempty"`
}

// resolveThread chooses the thread ID and whether to load an existing session
// based on the request.
func resolveThread(pr *parsedRequest) (threadID string, load bool) {
	if pr.ThreadID != "" {
		return pr.ThreadID, true
	}
	return uuid.New().String(), false
}
