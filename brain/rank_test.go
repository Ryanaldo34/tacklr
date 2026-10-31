package brain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRRF_fusesRanks(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	list1 := []ScoredID{{ID: a, Score: 9}, {ID: b, Score: 8}, {ID: c, Score: 1}}
	list2 := []ScoredID{{ID: c, Score: 9}, {ID: a, Score: 5}}

	fused := fuseRanks([][]ScoredID{list1, list2}, 60)
	sortScored(fused)

	if len(fused) != 3 {
		t.Fatalf("len=%d", len(fused))
	}
	if fused[0].ID != a {
		t.Fatalf("want a first, got %v order=%v", fused[0].ID, idsOf(fused))
	}

	// Later lists fill missing metadata and take a newer UpdatedAt.
	parent := uuid.New()
	pos := 3
	tOld := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tNew := tOld.Add(time.Hour)
	id := uuid.New()
	filled := fuseRanks([][]ScoredID{
		{{ID: id, Score: 1, UpdatedAt: tOld}},
		{{ID: id, Score: 1, UpdatedAt: tNew, ParentID: &parent, Title: "t", Content: "c", Position: &pos, Properties: map[string]any{"k": 1}}},
	}, 60)
	if len(filled) != 1 {
		t.Fatalf("fill len=%d", len(filled))
	}
	got := filled[0]
	if !got.UpdatedAt.Equal(tNew) || got.ParentID == nil || *got.ParentID != parent ||
		got.Title != "t" || got.Content != "c" || got.Position == nil || *got.Position != 3 ||
		got.Properties["k"] != 1 {
		t.Fatalf("fill: %+v", got)
	}

	// Empty / nil lists. A zero k selects the default constant.
	if got := fuseRanks(nil, 0); len(got) != 0 {
		t.Fatalf("nil: %+v", got)
	}
	if got := fuseRanks([][]ScoredID{nil, {}}, -1); len(got) != 0 {
		t.Fatalf("empty: %+v", got)
	}
	// Tie-break: equal RRF scores use UpdatedAt then ID.
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	idLo, idHi := uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")
	tied := []ScoredID{
		{ID: idHi, Score: 1, UpdatedAt: t1},
		{ID: idLo, Score: 1, UpdatedAt: t1},
		{ID: uuid.New(), Score: 1, UpdatedAt: t2},
	}
	sortScored(tied)
	if !tied[0].UpdatedAt.Equal(t2) {
		t.Fatalf("fresher first: %+v", tied)
	}
}

func TestTemporal_prefersFresherWhenSimilar(t *testing.T) {
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	oldID, newID := uuid.New(), uuid.New()
	parts := []ScoredID{
		{ID: oldID, Score: 1.0, UpdatedAt: now.Add(-30 * 24 * time.Hour)},
		{ID: newID, Score: 1.0, UpdatedAt: now.Add(-1 * 24 * time.Hour)},
	}
	applyTemporal(parts, 0.02, now)
	sortScored(parts)
	if parts[0].ID != newID {
		t.Fatalf("fresher should win: score old=%v new=%v", parts[1].Score, parts[0].Score)
	}

	parts2 := []ScoredID{
		{ID: oldID, Score: 1.0, UpdatedAt: now.Add(-30 * 24 * time.Hour)},
		{ID: newID, Score: 1.0, UpdatedAt: now.Add(-1 * 24 * time.Hour)},
	}
	applyTemporal(parts2, 0, now)
	if parts2[0].Score != 1.0 || parts2[1].Score != 1.0 {
		t.Fatalf("zero lambda must not change scores: %+v", parts2)
	}

	parts3 := []ScoredID{{ID: uuid.New(), Score: 2, UpdatedAt: now.Add(24 * time.Hour)}}
	applyTemporal(parts3, 0.02, now)
	if parts3[0].Score != 2 {
		t.Fatalf("future updated_at: %v", parts3[0].Score)
	}

	// Helix-style hits with no UpdatedAt must keep native rank scores.
	parts4 := []ScoredID{{ID: uuid.New(), Score: 0.9}}
	applyTemporal(parts4, 0.02, now)
	if parts4[0].Score != 0.9 {
		t.Fatalf("zero UpdatedAt must not decay: %v", parts4[0].Score)
	}
}

func TestPromote_multipleEvidenceAndParentHit(t *testing.T) {
	parent := uuid.New()
	p1, p2, p3 := uuid.New(), uuid.New(), uuid.New()
	parts := []ScoredID{
		{ID: p1, Score: 3, ParentID: &parent, Title: "a", Content: strings.Repeat("x", 300), UpdatedAt: time.Now()},
		{ID: p2, Score: 5, ParentID: &parent, Title: "b", Content: "short", UpdatedAt: time.Now()},
		{ID: p3, Score: 4, ParentID: &parent, Title: "c", Content: "mid", UpdatedAt: time.Now()},
		{ID: parent, Score: 1, ParentID: nil, Title: "root", UpdatedAt: time.Now()},
	}
	out := promoteParents(parts, 2, "", defaultSnippetCap)
	if len(out) != 1 || out[0].ParentID != parent {
		t.Fatalf("%+v", out)
	}
	if out[0].Score != 10 {
		t.Fatalf("the record's own hit plus its top evidence = %v, want 1+5+4", out[0].Score)
	}
	if len(out[0].Evidence) != 2 || out[0].Evidence[0].PartID != p2 {
		t.Fatalf("top evidence: %+v", out[0].Evidence)
	}
	long := ScoredID{ID: uuid.New(), Score: 9, ParentID: &parent, Content: strings.Repeat("z", 300), UpdatedAt: time.Now()}
	out2 := promoteParents([]ScoredID{long}, 1, "", defaultSnippetCap)
	if len(out2) != 1 || !strings.HasSuffix(out2[0].Evidence[0].Snippet, "…") {
		t.Fatalf("snippet truncate: %+v", out2)
	}
	if snippetAround("  ", "", 10) != "" {
		t.Fatal("snippet of blank text")
	}
	body := strings.Repeat("a", 80) + "oauth refresh" + strings.Repeat("b", 300)
	if sn := snippetAround(body, "oauth refresh", 240); !strings.Contains(sn, "oauth refresh") || !strings.HasPrefix(sn, "…") || !strings.HasSuffix(sn, "…") {
		t.Fatalf("snippet window: %q", sn)
	}
}

func idsOf(parts []ScoredID) []uuid.UUID {
	out := make([]uuid.UUID, len(parts))
	for i, p := range parts {
		out[i] = p.ID
	}
	return out
}

func TestRanking_fuseEdgesAgesEachListByUpdatedAt(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	from, stale, fresh := uuid.New(), uuid.New(), uuid.New()
	old := EdgeSearchHit{FromID: from, ToID: stale, RelationType: "about", Score: 1, Meta: EdgeMeta{UpdatedAt: now.AddDate(0, -6, 0)}}
	recent := EdgeSearchHit{FromID: from, ToID: fresh, RelationType: "about", Score: 0.9, Meta: EdgeMeta{UpdatedAt: now}}

	plain := Ranking{}.FuseEdges([]EdgeSearchHit{old, recent})
	aged := Ranking{Decay: 0.05, Now: func() time.Time { return now }}.FuseEdges([]EdgeSearchHit{old, recent}, []EdgeSearchHit{recent})

	if plain[0].ToID != stale || aged[0].ToID != fresh || len(aged) != 2 {
		t.Fatalf("without decay first = %s, with decay = %+v, want the stale edge first, then the fresh one", plain[0].ToID, aged)
	}
}

func TestEmbedding_embedBatchesStopsAtTheFirstStageError(t *testing.T) {
	parts := func(yield func(Indexed, error) bool) {
		for i := range 10 {
			if !yield(Indexed{Object: Object{ID: uuid.New()}, Text: strings.Repeat("x", i+1)}, nil) {
				return
			}
		}
	}
	staged := 0

	n, err := Embedding{BatchSize: 2, Concurrency: 1}.EmbedBatches(t.Context(), parts, func(uuid.UUID) bool { return false }, func(context.Context, []TreePart) error {
		staged++
		return errors.New("disk full")
	})

	if err == nil || err.Error() != "disk full" || staged != 1 || n > 4 {
		t.Fatalf("embed batches = %d parts read, %d batches staged, %v, want it to stop at the failed stage", n, staged, err)
	}
}
