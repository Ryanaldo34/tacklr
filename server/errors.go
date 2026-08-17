package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
)

// Sentinel errors for classification via errors.Is / errors.As.
var (
	ErrInvalidRequest            = errors.New("invalid request")
	ErrMethodNotFound            = errors.New("method not found")
	ErrAgentNotFound             = errors.New("agent not found")
	ErrSessionNotFound           = errors.New("session not found")
	ErrSessionStoreNotConfigured = errors.New("session store is not configured")
	ErrStreamingNotSupported     = errors.New("streaming not supported")
	ErrInternal                  = errors.New("internal server error")
)

// JSON-RPC 2.0 error codes.
const (
	jsonRPCCodeInvalidRequest = -32600
	jsonRPCCodeMethodNotFound = -32601
	jsonRPCCodeInternal       = -32603
	jsonRPCCodeApplication    = -32000 // server/application errors (-32000..-32099)
	jsonRPCCodeCancelled      = -32800 // request cancelled (ACP / LSP convention)
)

// ErrRequestCancelled is returned when a request is aborted via session/cancel
// or context cancellation.
var ErrRequestCancelled = errors.New("request cancelled")

// clientError is a caller-facing error that unwraps to a sentinel.
type clientError struct {
	sentinel error
	msg      string
}

func (e *clientError) Error() string { return e.msg }
func (e *clientError) Unwrap() error { return e.sentinel }

func clientErrorf(sentinel error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, msg: fmt.Sprintf(format, args...)}
}

// IsClientError reports whether err is safe to surface to the client
// (validation, not-found, bad payload). Internal failures return false.
func IsClientError(err error) bool {
	if err == nil {
		return false
	}
	var ce *clientError
	if errors.As(err, &ce) {
		return true
	}
	return errors.Is(err, stores.ErrSessionNotFound) ||
		errors.Is(err, interrupt.ErrInterruptNotFound) ||
		errors.Is(err, interrupt.ErrInvalidPayload) ||
		errors.Is(err, ErrInvalidRequest) ||
		errors.Is(err, ErrMethodNotFound) ||
		errors.Is(err, ErrAgentNotFound) ||
		errors.Is(err, ErrSessionNotFound) ||
		errors.Is(err, ErrSessionStoreNotConfigured) ||
		errors.Is(err, errConnectionNotInitialized)
}

// PublicError returns a wire-safe error: client errors pass through unchanged;
// all other errors become ErrInternal so internal details are not leaked.
func PublicError(err error) error {
	if err == nil {
		return ErrInternal
	}
	if IsClientError(err) {
		return err
	}
	return ErrInternal
}

// JSONRPCErrorCode maps err to a JSON-RPC 2.0 error code.
func JSONRPCErrorCode(err error) int {
	switch {
	case err == nil:
		return jsonRPCCodeInternal
	case errors.Is(err, ErrRequestCancelled), errors.Is(err, context.Canceled):
		return jsonRPCCodeCancelled
	case errors.Is(err, ErrMethodNotFound):
		return jsonRPCCodeMethodNotFound
	case errors.Is(err, ErrInvalidRequest):
		return jsonRPCCodeInvalidRequest
	case IsClientError(err):
		return jsonRPCCodeApplication
	default:
		return jsonRPCCodeInternal
	}
}
