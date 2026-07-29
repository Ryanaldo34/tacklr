package server

import (
	"encoding/json"

	"github.com/ryanaldo34/tacklr/mcp"
)

// ConfigOption describes a selectable session configuration option returned by
// session/new and session/load.
type ConfigOption struct {
	ID           string              `json:"id"`
	Name         string              `json:"name"`
	Description  string              `json:"description,omitempty"`
	Category     string              `json:"category"`
	Type         string              `json:"type"`
	CurrentValue string              `json:"currentValue"`
	Options      []ConfigOptionValue `json:"options"`
}

// ConfigOptionValue is one choice within a select-type ConfigOption.
type ConfigOptionValue struct {
	Value       string `json:"value"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// parsedRequest is retained for ACP parse helpers and tests that assert on
// validated request fields. New protocols map wire → TurnRequest inside handlers.
type parsedRequest struct {
	AgentID   string
	ThreadID  string
	Prompt    string
	Responses map[string]json.RawMessage

	// ACP JSON-RPC envelope
	ID           json.RawMessage
	Method       string
	Notification bool

	// ACP session lifecycle
	CWD        string
	MCPServers []mcp.MCPConfig

	// ACP session/set_config_option
	ConfigID    string
	ConfigValue string

	// ClientCapsRaw is the raw initialize params (for clientCapabilities).
	ClientCapsRaw json.RawMessage

	// Extensibility — raw _meta blob for custom fields
	Meta json.RawMessage
}
