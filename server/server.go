package server

import (
	"encoding/json"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
)

// HTTPMode selects how ServeHTTP binds routes for a Protocol.
type HTTPMode int

const (
	// HTTPModeRPC binds a single POST / JSON-RPC endpoint (ACP).
	HTTPModeRPC HTTPMode = iota
	// HTTPModeStream binds SSE (POST) and WebSocket (GET) endpoints.
	HTTPModeStream
)

// StreamEventHandler converts a StreamEvent into zero or more wire frames.
// Content management (buffering, chunking) is handled by the streaming strategy
// on the harness.
type StreamEventHandler func(threadID string, event *streaming.StreamEvent) ([][]byte, error)

// RequestValidator parses a raw request body into a transport-agnostic request.
type RequestValidator func([]byte) (*parsedRequest, error)

// Protocol converts between wire format and domain types.
type Protocol interface {
	Parse([]byte) (*parsedRequest, error)
	EncodeEvent(threadID string, event *streaming.StreamEvent) ([][]byte, error)
	HTTPMode() HTTPMode
}

// funcProtocol adapts RequestValidator + StreamEventHandler into Protocol.
type funcProtocol struct {
	validate RequestValidator
	encode   StreamEventHandler
	mode     HTTPMode
}

func (p *funcProtocol) Parse(body []byte) (*parsedRequest, error) {
	return p.validate(body)
}

func (p *funcProtocol) EncodeEvent(threadID string, event *streaming.StreamEvent) ([][]byte, error) {
	return p.encode(threadID, event)
}

func (p *funcProtocol) HTTPMode() HTTPMode { return p.mode }

// NewProtocol builds a Protocol from the existing validator/encoder function types.
func NewProtocol(validate RequestValidator, encode StreamEventHandler, mode HTTPMode) Protocol {
	return &funcProtocol{validate: validate, encode: encode, mode: mode}
}

// Built-in protocols.
var (
	ACP = NewProtocol(validateACPRequest, eventToAcpJsonRpc, HTTPModeRPC)
	SSE = NewProtocol(validateSSERequest, eventToRawSSE, HTTPModeStream)
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
