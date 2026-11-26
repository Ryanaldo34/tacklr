package durableobject

import (
	"context"
	"fmt"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/storetest"
)

// TestStores_meetTheSessionStoreContracts: the snapshot, secret and event
// stores an object keeps over its storage meet the contract every session
// store must meet.
func TestStores_meetTheSessionStoreContracts(t *testing.T) {
	t.Parallel()
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	storetest.Store(t, func(*testing.T) session.Store { return newSnapshots(NewMemStorage()) })
	storetest.Secrets(t, func(*testing.T) session.SecretStorage { return &secretStore{s: NewMemStorage(), sealer: sealer} })
	storetest.EventLog(t, func(*testing.T) session.EventLog { return newEventLog(NewMemStorage()) })
}

// TestEventLog_aLogLongerThanAReadReplaysTrimsAndClosesWhole: a log of more
// events than one storage read returns replays every event, trims through
// its second read, and a close deletes all of it.
func TestEventLog_aLogLongerThanAReadReplaysTrimsAndClosesWhole(t *testing.T) {
	t.Parallel()
	ctx, s := t.Context(), NewMemStorage()
	log := newEventLog(s)
	n := tailBatch + 44
	for _, id := range []session.ID{"trimmed", "closed"} {
		for i := range n {
			if err := log.Append(ctx, id, session.TopicEvents, session.Event{Content: fmt.Sprint(i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	replay := func(id session.ID, want int) []session.Event {
		t.Helper()
		sctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch, err := log.Subscribe(sctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		var got []session.Event
		for range want {
			got = append(got, <-ch)
		}
		return got
	}

	whole := replay("trimmed", n)
	trimErr := log.Trim(ctx, "trimmed", session.Seq(n-4))
	kept := replay("trimmed", 4)
	closeErr := log.CloseSession(ctx, "closed")
	left, scanErr := s.Scan(prefix(kindEvent, "closed"), "", 0)

	if whole[0].Content != "1" || whole[n-1].Content != fmt.Sprint(n) {
		t.Errorf("replay = %s..%s, want 1..%d", whole[0].Content, whole[n-1].Content, n)
	}
	if trimErr != nil || kept[0].Content != fmt.Sprint(n-3) {
		t.Errorf("after a trim through %d (%v) the replay starts at %s, want %d", n-4, trimErr, kept[0].Content, n-3)
	}
	if closeErr != nil || scanErr != nil || len(left) != 0 {
		t.Errorf("close = %v, then %d events left (%v), want none", closeErr, len(left), scanErr)
	}
}
