package server

import (
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/session"
)

// Wire-facing sentinels. Session errors are the owning package's sentinels
// (same pointer) so errors.Is is one check. Authentication sentinels live in auth.go.
var (
	ErrInvalidRequest  = errors.New("invalid request")
	ErrMethodNotFound  = errors.New("method not found")
	ErrInternal        = errors.New("internal server error")
	ErrSessionNotFound = session.ErrSessionNotFound
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

// Errorf is a caller-facing error that unwraps to sentinel.
func Errorf(sentinel error, format string, args ...any) error {
	return ErrorCause(sentinel, nil, format, args...)
}

// ErrorCause is Errorf with an underlying cause for errors.Is.
func ErrorCause(sentinel, cause error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, cause: cause, msg: fmt.Sprintf(format, args...)}
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
