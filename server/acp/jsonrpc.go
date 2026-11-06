package acp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ryanaldo34/tacklr/server"
)

// JSON-RPC 2.0 error codes. Cancelled follows the ACP / LSP convention.
const (
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInternal       = -32603
	CodeApplication    = -32000
	CodeCancelled      = -32800
)

// ErrorCode maps err to a JSON-RPC 2.0 code.
func ErrorCode(err error) int {
	switch {
	case errors.Is(err, server.ErrMethodNotFound):
		return CodeMethodNotFound
	case errors.Is(err, server.ErrInvalidRequest):
		return CodeInvalidRequest
	case errors.Is(err, context.Canceled):
		return CodeCancelled
	case server.IsClientError(err):
		return CodeApplication
	default:
		return CodeInternal
	}
}

// ErrorBody is one JSON-RPC error envelope.
func ErrorBody(id json.RawMessage, err error) map[string]any {
	pub := server.PublicError(err)
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    ErrorCode(err),
			"message": pub.Error(),
		},
	}
}
