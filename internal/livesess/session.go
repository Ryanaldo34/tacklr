package livesess

import (
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Session is an in-process durable session for tests (Prompt / Subscribe / Resume).
type Session struct {
	Runtime *inprocess.Runtime
	ID      durable.SessionID
	sub     durable.Subscription
}

// StartSession registers opts as the default catalog agent and creates a session.
func StartSession(t testing.TB, opts tacklr.AgentOptions) *Session {
	t.Helper()
	if opts.Config.MaxWindowSize == 0 {
		opts.Config.MaxWindowSize = 8192
	}
	sessionID := durable.SessionID(opts.SessionID)
	opts.SessionID = ""
	opts.MountSession = nil
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{Options: opts})
	rt := inprocess.New(cat, inprocess.WithProjection(vfs.DirectProjection{}))
	id, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default", SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sub.Close()
		_ = rt.Close(t.Context(), id)
	})
	return &Session{Runtime: rt, ID: id, sub: sub}
}

func (s *Session) Prompt(t testing.TB, text string) []tacklr.StreamEvent {
	t.Helper()
	if err := s.Runtime.Prompt(t.Context(), s.ID, durable.Prompt{Text: text}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) PromptMessage(t testing.TB, msg *tacklr.Message) []tacklr.StreamEvent {
	t.Helper()
	if err := s.Runtime.Prompt(t.Context(), s.ID, durable.Prompt{UserMessage: msg}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) Resume(t testing.TB, responses map[string][]byte) []tacklr.StreamEvent {
	t.Helper()
	if err := s.Runtime.Resume(t.Context(), s.ID, durable.Resume{Responses: responses}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) Wait(t testing.TB) []tacklr.StreamEvent {
	t.Helper()
	deadline := time.After(8 * time.Second)
	var got []tacklr.StreamEvent
	for {
		select {
		case ev, ok := <-s.sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError || ev.Type == tacklr.StreamEventInterrupt {
				return got
			}
		case <-deadline:
			t.Fatalf("timeout waiting for turn events, got %d", len(got))
		}
	}
}
