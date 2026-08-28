package builtins

import (
	"context"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/interrupt"
)

type stubRuntime struct{}

func (stubRuntime) EmitUpdate(string)           {}
func (stubRuntime) StateGet(string) (any, bool) { return nil, false }
func (stubRuntime) StateSet(string, any) error  { return nil }
func (stubRuntime) StateDelete(string)          {}
func (stubRuntime) Park(string, []byte) (tacklr.Interrupt, error) {
	return nil, interrupt.ErrInterruptNotFound
}
func (stubRuntime) CurrentToolCallID() string { return "" }
func (stubRuntime) SpawnChild(context.Context, string, string) (string, error) {
	return "", tacklr.ErrFailed
}
func (stubRuntime) Children() []tacklr.Child { return nil }
func (stubRuntime) CancelChild(context.Context, string) error {
	return tacklr.ErrFailed
}
func (stubRuntime) AwaitChild(context.Context, string) (tacklr.Child, error) {
	return tacklr.Child{}, tacklr.ErrNotFound
}
