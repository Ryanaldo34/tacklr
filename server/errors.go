package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/interrupt"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

// Wire-facing sentinels. Session/agent/auth groups are the owning package's
// sentinels (same pointer) so errors.Is is one check.
var (
	ErrInvalidRequest         = errors.New("invalid request")
	ErrMethodNotFound         = errors.New("method not found")
	ErrInternal               = errors.New("internal server error")
	ErrAgentNotFound          = durable.ErrAgentNotFound
	ErrSessionNotFound        = durable.ErrSessionNotFound
	ErrAuthenticationRequired = tacklrsecurity.ErrAuthenticationRequired
	ErrAuthenticationFailed   = tacklrsecurity.ErrAuthenticationFailed
	ErrAuthorizationDenied    = tacklrsecurity.ErrAuthorizationDenied
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
// When cause is set, errors.Is/As can also match the underlying failure.
type clientError struct {
	sentinel error
	msg      string
	cause    error
}

func (e *clientError) Error() string { return e.msg }

func (e *clientError) Unwrap() []error {
	if e.cause != nil {
		return []error{e.sentinel, e.cause}
	}
	return []error{e.sentinel}
}

func clientErrorf(sentinel error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, msg: fmt.Sprintf(format, args...)}
}

func clientErrorCause(sentinel, cause error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, cause: cause, msg: fmt.Sprintf(format, args...)}
}

// writeWireError writes a client-visible error and logs a write failure.
func writeWireError(w MessageWriter, id json.RawMessage, err error) {
	if writeErr := w.WriteError(id, err); writeErr != nil {
		slog.Warn("failed to write client error", "error", writeErr, "cause", err)
	}
}

// IsClientError reports whether err is safe to surface to the client
// (validation, not-found, bad payload). Internal failures return false.
func logTurnError(err error, agentID, threadID string) {
	if IsClientError(err) {
		slog.Debug("client error", "error", err, "agent_id", agentID, "thread_id", threadID)
		return
	}
	slog.Error("agent turn failed", "error", err, "agent_id", agentID, "thread_id", threadID)
}

func IsClientError(err error) bool {
	if err == nil {
		return false
	}
	var ce *clientError
	if errors.As(err, &ce) {
		return true
	}
	return errors.Is(err, ErrSessionNotFound) ||
		errors.Is(err, ErrAgentNotFound) ||
		errors.Is(err, interrupt.ErrInterruptNotFound) ||
		errors.Is(err, interrupt.ErrInvalidPayload) ||
		errors.Is(err, ErrInvalidRequest) ||
		errors.Is(err, ErrMethodNotFound) ||
		errors.Is(err, ErrAuthenticationRequired) ||
		errors.Is(err, ErrAuthenticationFailed) ||
		errors.Is(err, ErrAuthorizationDenied)
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
