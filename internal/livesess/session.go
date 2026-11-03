package livesess

import (
	"context"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/session"
)

// Session is a Temporal Docker session for tests (Prompt / Subscribe / Resume).
type Session struct {
	Client session.Client
	ID     session.ID
	sub    session.Subscription
}

// StartSession registers opts as the default catalog agent and creates a session.
func StartSession(t testing.TB, opts tacklr.AgentOptions) *Session {
	t.Helper()
	if opts.MaxWindowSize == 0 {
		opts.MaxWindowSize = 8192
	}
	rt := Runtime(t, opts)
	id, err := rt.CreateSession(t.Context(), session.CreateSession{})
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
	return &Session{Client: rt, ID: id, sub: sub}
}

func (s *Session) Prompt(t testing.TB, text string) []session.Event {
	t.Helper()
	if err := s.Client.Prompt(t.Context(), s.ID, session.Prompt{Text: text}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) PromptMessage(t testing.TB, msg *message.Message) []session.Event {
	t.Helper()
	if err := s.Client.Prompt(t.Context(), s.ID, session.Prompt{UserMessage: msg}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) Resume(t testing.TB, responses map[string][]byte) []session.Event {
	t.Helper()
	if err := s.Client.Resume(t.Context(), s.ID, session.Resume{Responses: responses}); err != nil {
		t.Fatal(err)
	}
	return s.Wait(t)
}

func (s *Session) Wait(t testing.TB) []session.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var got []session.Event
	for {
		select {
		case ev, ok := <-s.sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
			if ev.Type == session.EventComplete || ev.Type == session.EventError || ev.Type == session.EventInterrupt {
				testkit.AssertStatusMatchesEvent(t, s.Client, s.ID, ev)
				return got
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for turn events, got %d", len(got))
		}
	}
}
