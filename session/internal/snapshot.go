package adapter

import (
	"context"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// Snapshot is what a runtime knows of one harness's committed state: the
// store revision it holds, the digest of the rows it last saved or loaded,
// and whether a failed Commit left it ahead of the store. Both runtimes
// keep one per harness, so committed state stays the store's: a step loads
// it into the harness only when the store's revision moved past the one
// the harness holds, and a Commit writes only the rows it changed. The
// caller serializes its use.
type Snapshot struct {
	rev    session.Revision
	digest session.CheckpointDigest
	loaded bool
	stale  bool
}

// Stale reports whether a failed Commit left the harness ahead of the
// store: the runtime builds it again.
func (s *Snapshot) Stale() bool { return s.stale }

// Current reports whether the harness holds the store's revision.
func (s *Snapshot) Current(ctx context.Context, store session.Store, id session.ID) (bool, error) {
	rev, err := store.Revision(ctx, id)
	return err == nil && s.loaded && rev == s.rev, err
}

// Sync loads the checkpoint into h when the store's revision is not the one
// it holds.
func (s *Snapshot) Sync(ctx context.Context, store session.Store, id session.ID, h *tacklr.Harness) error {
	rev, err := store.Revision(ctx, id)
	if err != nil {
		return err
	}
	if s.loaded && rev == s.rev {
		return nil
	}
	if rev == "" {
		if s.loaded {
			return fmt.Errorf("snapshot of %s was deleted: %w", id, session.ErrNotFound)
		}
		s.loaded = true
		return nil
	}
	rows, rev, err := store.Load(ctx, id)
	if err != nil {
		return err
	}
	if err := h.RestoreCheckpoint(rows); err != nil {
		return err
	}
	s.rev, s.digest, s.loaded = rev, session.DigestOf(rows), true
	return nil
}

// Commit is the Commit step on h: it brings h to the store's revision,
// then applies in and saves the rows it changed, unless the checkpoint is
// already past base (an earlier attempt of this Commit saved). It returns
// the checkpoint's revision.
func (s *Snapshot) Commit(ctx context.Context, store session.Store, id session.ID, h *tacklr.Harness, base session.Revision, in tacklr.CommitIn, out chan session.Event) (session.Revision, error) {
	if err := s.Sync(ctx, store, id, h); err != nil {
		return "", err
	}
	if s.rev != base {
		return s.rev, nil
	}
	if in.Round != nil {
		r := *in.Round
		r.Outcomes = slices.Clone(r.Outcomes)
		if err := LoadRecords(ctx, store, id, r.Outcomes); err != nil {
			return "", err
		}
		in.Round = &r
	}
	// A failed Commit may leave the harness ahead of the store.
	s.stale = true
	if err := h.Commit(ctx, in, out); err != nil {
		return "", err
	}
	rows, err := h.Checkpoint()
	if err != nil {
		return "", err
	}
	changes, digest := session.Diff(s.digest, rows)
	rev, err := store.Save(ctx, id, changes, s.rev)
	if err != nil {
		return "", err
	}
	s.rev, s.digest, s.stale = rev, digest, false
	return rev, nil
}
