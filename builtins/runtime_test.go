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
func (stubRuntime) Schedule(context.Context, tacklr.JobRequest) (tacklr.Job, error) {
	return tacklr.Job{}, tacklr.ErrFailed
}
func (stubRuntime) Jobs() []tacklr.Job { return nil }
func (stubRuntime) CancelJob(context.Context, string) error {
	return tacklr.ErrFailed
}
func (stubRuntime) RunSpecialist(context.Context, string, string) (string, error) {
	return "", tacklr.ErrNotFound
}
