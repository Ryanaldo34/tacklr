package tacklr

import (
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// HarnessRuntime is the public tool hook surface (state, interrupts, emit).
// Session internals are not accessible through it.
type HarnessRuntime = session.Runtime

// Todo is a plan list item (also used in plan_update stream payloads).
type Todo = streaming.Todo

// Re-export interrupt extension types for tool authors.
type (
	Interrupt               = interrupt.Interrupt
	PayloadValidator        = interrupt.PayloadValidator
	UserChoice              = interrupt.UserChoice
	UserSelectionInterrupt  = interrupt.UserSelectionInterrupt
	ToolPermissionInterrupt = interrupt.ToolPermissionInterrupt
	PermissionOption        = interrupt.PermissionOption
)

var (
	ErrInterruptNotFound     = interrupt.ErrInterruptNotFound
	ErrInvalidPayload        = interrupt.ErrInvalidPayload
	DefaultPermissionOptions = interrupt.DefaultPermissionOptions
)

const (
	PermissionAllowOnce    = interrupt.PermissionAllowOnce
	PermissionAllowAlways  = interrupt.PermissionAllowAlways
	PermissionRejectOnce   = interrupt.PermissionRejectOnce
	PermissionRejectAlways = interrupt.PermissionRejectAlways
)

// RegisterInterrupt registers a custom interrupt factory for checkpoint rehydrate.
func RegisterInterrupt(factory func() Interrupt) {
	interrupt.Register(factory)
}
