package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/durable"
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
	return clientErrorCause(sentinel, nil, format, args...)
}

func clientErrorCause(sentinel, cause error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, cause: cause, msg: fmt.Sprintf(format, args...)}
}

// reply writes a JSON-RPC result or error. err wins.
func reply(w MessageWriter, id json.RawMessage, result any, err error) error {
	if err != nil {
		_ = w.WriteError(id, err)
		return err
	}
	return w.WriteResult(id, result)
}

func IsClientError(err error) bool {
	var ce *clientError
	return errors.As(err, &ce)
}

// PublicError returns a wire-safe error: client errors pass through unchanged;
// all other errors become ErrInternal so internal details are not leaked.
func PublicError(err error) error {
	if IsClientError(err) {
		return err
	}
	return ErrInternal
}

// JSONRPCErrorCode maps err to a JSON-RPC 2.0 error code.
func JSONRPCErrorCode(err error) int {
	var ce *clientError
	if errors.As(err, &ce) {
		err = ce.sentinel
		switch {
		case errors.Is(err, ErrMethodNotFound):
			return jsonRPCCodeMethodNotFound
		case errors.Is(err, ErrInvalidRequest):
			return jsonRPCCodeInvalidRequest
		default:
			return jsonRPCCodeApplication
		}
	}
	if errors.Is(err, context.Canceled) {
		return jsonRPCCodeCancelled
	}
	return jsonRPCCodeInternal
}
