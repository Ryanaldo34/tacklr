package interrupt

import "errors"

var (
	ErrInterruptNotFound = errors.New("interrupt not found")
	ErrInvalidPayload    = errors.New("invalid payload")
)
