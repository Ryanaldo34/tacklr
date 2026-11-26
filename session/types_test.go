package session_test

import (
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// TestCreateSession_Validate_refusesASpecialistThatIsAlsoAWorker: a
// session is a specialist, a worker, or neither; never both.
func TestCreateSession_Validate_refusesASpecialistThatIsAlsoAWorker(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		req  session.CreateSession
		want error
	}{
		"neither":    {session.CreateSession{}, nil},
		"specialist": {session.CreateSession{Specialist: "reviewer"}, nil},
		"worker":     {session.CreateSession{Worker: "ci"}, nil},
		"both":       {session.CreateSession{Specialist: "reviewer", Worker: "ci"}, session.ErrInvalid},
	} {
		// Act
		err := tc.req.Validate()

		// Assert
		if !errors.Is(err, tc.want) || (tc.want == nil) != (err == nil) {
			t.Errorf("%s: Validate = %v, want %v", name, err, tc.want)
		}
	}
}
