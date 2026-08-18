package server

import (
	"encoding/json"

	"github.com/ryanaldo34/tacklr"
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
// validated request fields. Session lifecycle uses Protocol methods with Params.
type parsedRequest struct {
	AgentID  string
	ThreadID string
	Prompt   string
	// UserMessage is set for multimodal ACP prompts (Content + ContentParts).
	// When non-nil, runHarness prefers RunMessage over Prompt string.
	UserMessage *tacklr.Message
	Responses   map[string]json.RawMessage

	// ACP JSON-RPC envelope
	ID           json.RawMessage
	Method       string
	Notification bool
	// Params is the raw JSON-RPC params object (passed to Protocol session methods).
	Params json.RawMessage

	// ACP session lifecycle (validated view of Params)
	CWD        string
	MCPServers []mcp.MCPConfig

	// ACP session/set_config_option
	ConfigID    string
	ConfigValue string

	// ProtocolVersion is the client's requested major version (initialize).
	ProtocolVersion int

	// ClientCapsRaw is the raw initialize params (for clientCapabilities).
	ClientCapsRaw json.RawMessage

	// AuthMethodID is the ACP v1 method selected by authenticate.
	AuthMethodID string

	// Extensibility — raw _meta blob for custom fields
	Meta json.RawMessage
}
