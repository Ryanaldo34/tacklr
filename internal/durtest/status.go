package durtest

import (
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// AssertStatusMatchesEvent checks Runtime.Status against a turn-finished stream
// event. complete → SessionComplete, error → SessionFailed, yield → running+Waiting.
func AssertStatusMatchesEvent(t testing.TB, rt session.Client, id session.ID, ev session.Event) {
	t.Helper()
	switch ev.Type {
	case session.EventComplete, session.EventError, session.EventInterrupt:
	default:
		return
	}
	st, err := rt.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	switch ev.Type {
	case session.EventComplete:
		if st.State != session.Complete {
			t.Fatalf("complete event but Status %+v", st)
		}
	case session.EventError:
		if st.State != session.Failed {
			t.Fatalf("error event but Status %+v", st)
		}
	case session.EventInterrupt:
		if st.State != session.Running || !st.Waiting {
			t.Fatalf("yield event but Status %+v", st)
		}
	}
}
