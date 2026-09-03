package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// correctionError is a model-facing tool failure. Error() is what the model sees
// (what failed and how to correct it). Unwrap is the harness cause.
type correctionError struct {
	msg   string
	cause error
}

func (e *correctionError) Error() string {
	if e == nil || e.msg == "" {
		return ErrCorrection.Error()
	}
	return e.msg
}

func (e *correctionError) Unwrap() error { return e.cause }

func (e *correctionError) Is(target error) bool { return target == ErrCorrection }

// Correction wraps cause with model-facing correction text. msg is Error();
// errors.Is matches ErrCorrection and cause. A nil/empty msg uses cause.Error().
func Correction(cause error, msg string) error {
	msg = strings.TrimSpace(msg)
	if msg == "" && cause != nil {
		msg = strings.TrimPrefix(cause.Error(), "vfs: ")
	}
	if msg == "" {
		return ErrCorrection
	}
	var a *correctionError
	if errors.As(cause, &a) {
		return cause
	}
	return &correctionError{msg: msg, cause: cause}
}

// Correctionf is Correction with fmt.Sprintf.
func Correctionf(cause error, format string, args ...any) error {
	return Correction(cause, fmt.Sprintf(format, args...))
}

// presentToolError is the turn processor table: interrupt (yield), cancel,
// already Correction, ErrFailed (service), else Correction(err, err.Error()).
func presentToolError(name string, err error) error {
	var intr interrupt.Interrupt
	if errors.As(err, &intr) || errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrection) || errors.Is(err, ErrFailed) {
		return err
	}
	msg := strings.TrimPrefix(err.Error(), "vfs: ")
	if name != "" && !strings.HasPrefix(msg, name) {
		msg = name + ": " + msg
	}
	return Correction(err, msg)
}
