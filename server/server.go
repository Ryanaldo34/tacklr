package server

import (
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

type RequestValidator func([]byte) clientError

var handlers = map[Protocol]StreamEventHandler{
	ProtocolACP: eventToAcpJsonRpc,
	ProtocolSSE: eventToRawSSE,
}
var validators = map[Protocol]RequestValidator{}
