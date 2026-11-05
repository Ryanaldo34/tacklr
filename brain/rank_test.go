package brain_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// TestRanking_fusesListsByRankAfterAgingEach: an id high in two lists leads
// and takes the metadata a later list adds; with decay, a fresh hit passes an
// equal stale one, and a hit with no time keeps its score. Edges fuse the
// same way, and decay ages them by their own time.
func TestRanking_fusesListsByRankAfterAgingEach(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	a, b, c, parent := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	stale, fresh, timeless := uuid.New(), uuid.New(), uuid.New()
	from, staleTo, freshTo := uuid.New(), uuid.New(), uuid.New()
	oldEdge := brain.EdgeSearchHit{FromID: from, ToID: staleTo, RelationType: "about", Score: 1, Meta: brain.EdgeMeta{UpdatedAt: now.AddDate(0, -6, 0)}}
	newEdge := brain.EdgeSearchHit{FromID: from, ToID: freshTo, RelationType: "about", Score: 0.9, Meta: brain.EdgeMeta{UpdatedAt: now}}

	fused := brain.Ranking{}.Fuse(
		[]brain.ScoredID{{ID: a, Score: 9, UpdatedAt: now.Add(-time.Hour)}, {ID: b, Score: 8}, {ID: c, Score: 1}},
		[]brain.ScoredID{{ID: c, Score: 9}, {ID: a, Score: 5, UpdatedAt: now, ParentID: &parent, Title: "t", Content: "c", Position: new(3), Properties: map[string]any{"k": 1}}},
	)
	aged := brain.Ranking{Decay: 0.02, Now: clock}.Fuse([]brain.ScoredID{
		{ID: stale, Score: 1, UpdatedAt: now.AddDate(0, 0, -30)},
		{ID: fresh, Score: 1, UpdatedAt: now.AddDate(0, 0, -1)},
		{ID: timeless, Score: 0.9},
	})
	plainEdges := brain.Ranking{}.FuseEdges([]brain.EdgeSearchHit{oldEdge, newEdge})
	agedEdges := brain.Ranking{Decay: 0.05, Now: clock}.FuseEdges([]brain.EdgeSearchHit{oldEdge, newEdge}, []brain.EdgeSearchHit{newEdge})

	ids := func(hits []brain.ScoredID) []uuid.UUID {
		out := make([]uuid.UUID, len(hits))
		for i, h := range hits {
			out[i] = h.ID
		}
		return out
	}
	if !slices.Equal(ids(fused), []uuid.UUID{a, c, b}) {
		t.Errorf("fused = %v, want a (in both lists high), c, b", ids(fused))
	}
	if top := fused[0]; !top.UpdatedAt.Equal(now) || top.ParentID == nil || *top.ParentID != parent || top.Title != "t" ||
		top.Content != "c" || top.Position == nil || *top.Position != 3 || top.Properties["k"] != 1 {
		t.Errorf("fused a = %+v, want the newer time and the metadata of the second list", top)
	}
	if !slices.Equal(ids(aged), []uuid.UUID{fresh, timeless, stale}) {
		t.Errorf("aged = %v, want the fresh hit, the hit with no time, then the stale hit", ids(aged))
	}
	if plainEdges[0].ToID != staleTo || len(agedEdges) != 2 || agedEdges[0].ToID != freshTo {
		t.Errorf("edges without decay lead with %s, with decay %+v, want the stale edge, then the fresh one", plainEdges[0].ToID, agedEdges)
	}
}

// TestEmbedding_embedBatchesStopsAtTheFirstStageError: a failed stage ends
// the embedding with its error, before the reader reads the rest.
func TestEmbedding_embedBatchesStopsAtTheFirstStageError(t *testing.T) {
	parts := func(yield func(brain.Indexed, error) bool) {
		for i := range 10 {
			if !yield(brain.Indexed{Object: brain.Object{ID: uuid.New()}, Text: strings.Repeat("x", i+1)}, nil) {
				return
			}
		}
	}
	staged := 0

	n, err := brain.Embedding{BatchSize: 2, Concurrency: 1}.EmbedBatches(t.Context(), parts, func(uuid.UUID) bool { return false }, func(context.Context, []brain.TreePart) error {
		staged++
		return errors.New("disk full")
	})

	if err == nil || err.Error() != "disk full" || staged != 1 || n > 4 {
		t.Fatalf("embed batches = %d parts read, %d batches staged, %v, want it to stop at the failed stage", n, staged, err)
	}
}
