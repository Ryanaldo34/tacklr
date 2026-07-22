package server

import (
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/stores"
)

var (
	ErrInvalidRequest            = errors.New("invalid request")
	ErrAgentNotFound             = errors.New("agent not found")
	ErrSessionNotFound           = errors.New("session not found")
	ErrSessionStoreNotConfigured = errors.New("session store is not configured")
	ErrStreamingNotSupported     = errors.New("streaming not supported")
	ErrInternal                  = errors.New("internal server error")
)

type clientError struct {
	sentinel error
	msg      string
}

func (e *clientError) Error() string { return e.msg }
func (e *clientError) Unwrap() error { return e.sentinel }

func clientErrorf(sentinel error, format string, args ...any) error {
	return &clientError{sentinel: sentinel, msg: fmt.Sprintf(format, args...)}
}

func IsClientError(err error) bool {
	var ce *clientError
	if errors.As(err, &ce) {
		return true
	}
	return errors.Is(err, stores.ErrSessionNotFound) ||
		errors.Is(err, control.ErrInterruptNotFound) ||
		errors.Is(err, control.ErrInvalidPayload)
}
