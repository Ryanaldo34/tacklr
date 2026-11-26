package tacklr

import (
	"errors"
	"testing"
)

// TestValidateJob_refusesAJobThatCannotStart: a job with no name, a
// specialist started as a job, a job started as a specialist, a name no
// one registered, or no task is refused; a registered job with a task
// starts.
func TestValidateJob_refusesAJobThatCannotStart(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		name, task                           string
		specialist, isSpecialist, registered bool
		want                                 error
	}{
		"no name":                      {task: "x", registered: true, want: ErrInvalid},
		"a specialist as a job":        {name: "researcher", task: "x", isSpecialist: true, want: ErrInvalid},
		"a job as a specialist":        {name: "ci", task: "x", specialist: true, registered: true, want: ErrNotFound},
		"a job no one registered":      {name: "nightly", task: "x", want: ErrNotFound},
		"no task":                      {name: "ci", registered: true, want: ErrInvalid},
		"a registered job with a task": {name: "ci", task: "test", registered: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := validateJob(tc.name, tc.task, tc.specialist, tc.isSpecialist, tc.registered)

			if !errors.Is(err, tc.want) || (tc.want == nil) != (err == nil) {
				t.Fatalf("validateJob = %v, want %v", err, tc.want)
			}
		})
	}
}
