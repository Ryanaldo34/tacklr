package server

import (
	"encoding/json"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
)

type Protocol string

const (
	ProtocolACP Protocol = "acp"
	ProtocolSSE Protocol = "sse"
)

// StreamEventHandler converts a StreamEvent into zero or more wire frames to
// be sent to the client. It is a pure format conversion — content management
// (buffering, chunking) is handled by the streaming strategy set on the
// harness.
type StreamEventHandler func(threadID string, event *streaming.StreamEvent) ([][]byte, error)

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

// parsedRequest carries the key information extracted from a request body by a
// protocol validator. It is the standard struct that handlers use to invoke the
// agent harness, regardless of the transport protocol.
type parsedRequest struct {
	AgentID   string
	ThreadID  string
	Prompt    string
	Responses map[string]json.RawMessage

	// ACP JSON-RPC envelope
	ID           json.RawMessage
	Method       string
	Notification bool // true when the message has no id (JSON-RPC notification)

	// ACP session lifecycle
	CWD        string
	MCPServers []mcp.MCPConfig

	// ACP session/set_config_option
	ConfigID    string
	ConfigValue string

	// Extensibility — raw _meta blob for custom fields
	Meta json.RawMessage
}

type RequestValidator func([]byte) (*parsedRequest, error)

var handlers = map[Protocol]StreamEventHandler{
	ProtocolACP: eventToAcpJsonRpc,
	ProtocolSSE: eventToRawSSE,
}

var validators = map[Protocol]RequestValidator{
	ProtocolSSE: validateSSERequest,
	ProtocolACP: validateACPRequest,
}

func GetHandler(protocol Protocol) StreamEventHandler {
	return handlers[protocol]
}

func GetValidator(protocol Protocol) RequestValidator {
	return validators[protocol]
}
