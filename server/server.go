package server

import (
	"encoding/json"

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

// parsedRequest carries the key information extracted from a request body by a
// protocol validator. It is the standard struct that handlers use to invoke the
// agent harness, regardless of the transport protocol.
type parsedRequest struct {
	AgentID   string
	ThreadID  string
	Prompt    string
	Responses map[string]json.RawMessage
}

type RequestValidator func([]byte) (*parsedRequest, error)

var handlers = map[Protocol]StreamEventHandler{
	ProtocolACP: eventToAcpJsonRpc,
	ProtocolSSE: eventToRawSSE,
}

var validators = map[Protocol]RequestValidator{
	ProtocolSSE: validateSSERequest,
}
