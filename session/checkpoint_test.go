package session_test

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
)

// TestDiff_putsNewAndChangedRowsAndDeletesTheGoneOnes: from the digest of
// the rows a store holds, a diff puts each row that is new or changed, in
// order, then deletes each row that is gone, and skips the rest.
func TestDiff_putsNewAndChangedRowsAndDeletesTheGoneOnes(t *testing.T) {
	t.Parallel()
	row := func(kind, key, value string) session.CheckpointEntry {
		return session.CheckpointEntry{Kind: kind, Key: key, Value: []byte(value)}
	}
	saved := session.DigestOf([]session.CheckpointEntry{row("message", "0", "a"), row("message", "1", "b"), row("message", "2", "c"), row("state", "k", "1")})
	next := []session.CheckpointEntry{row("message", "0", "a"), row("message", "1", "summary"), row("state", "k", "1"), row("state", "new", "2")}

	changes, digest := session.Diff(saved, next)
	again, _ := session.Diff(digest, next)

	var got []string
	for _, c := range changes {
		op := "put "
		if c.Delete {
			op = "delete "
		}
		got = append(got, op+c.Kind+"/"+c.Key)
	}
	if want := []string{"put message/1", "put state/new", "delete message/2"}; !slices.Equal(got, want) {
		t.Fatalf("changes %v; want %v", got, want)
	}
	if len(again) != 0 {
		t.Fatalf("a diff from its own digest %v; want no change", again)
	}
}
