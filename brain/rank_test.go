package brain

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRRF_fusesRanks(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	list1 := []ScoredID{{ID: a, Score: 9}, {ID: b, Score: 8}, {ID: c, Score: 1}}
	list2 := []ScoredID{{ID: c, Score: 9}, {ID: a, Score: 5}}

	fused := rrfFuse([][]ScoredID{list1, list2}, 60)
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
	filled := rrfFuse([][]ScoredID{
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

	// Empty / nil lists and k<=0 defaults.
	if got := rrfFuse(nil, 0); len(got) != 0 {
		t.Fatalf("nil: %+v", got)
	}
	if got := rrfFuse([][]ScoredID{nil, {}}, -1); len(got) != 0 {
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

func TestEngineConfig_explicitZeroLambdaPreserved(t *testing.T) {
	zero := 0.0
	cfg := EngineConfig{Lambda: &zero}.withDefaults()
	if cfg.Lambda == nil || *cfg.Lambda != 0 {
		t.Fatalf("want preserved zero, got %v", cfg.Lambda)
	}
	cfg2 := EngineConfig{}.withDefaults()
	if cfg2.Lambda == nil || *cfg2.Lambda != 0.02 {
		t.Fatalf("want default lambda, got %v", cfg2.Lambda)
	}
	if cfg2.lambdaValue() != 0.02 {
		t.Fatal("lambdaValue default")
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
	out := promoteParents(parts, 2)
	if len(out) != 1 || out[0].ParentID != parent {
		t.Fatalf("%+v", out)
	}
	if out[0].Score != 5 {
		t.Fatalf("max score: %v", out[0].Score)
	}
	if len(out[0].Evidence) != 2 || out[0].Evidence[0].PartID != p2 {
		t.Fatalf("top evidence: %+v", out[0].Evidence)
	}
	long := ScoredID{ID: uuid.New(), Score: 9, ParentID: &parent, Content: strings.Repeat("z", 300), UpdatedAt: time.Now()}
	out2 := promoteParents([]ScoredID{long}, 1)
	if len(out2) != 1 || !strings.HasSuffix(out2[0].Evidence[0].Snippet, "…") {
		t.Fatalf("snippet truncate: %+v", out2)
	}
	if snippet("  ", 10) != "" || snippet("keep", 0) != "keep" {
		t.Fatal("snippet empty/cap")
	}
	def := promoteParents([]ScoredID{
		{ID: p1, Score: 1, ParentID: &parent, Title: "a"},
		{ID: p2, Score: 2, ParentID: &parent, Title: "b"},
		{ID: p3, Score: 3, ParentID: &parent, Title: "c"},
		{ID: uuid.New(), Score: 4, ParentID: &parent, Title: "d"},
	}, 0)
	if len(def) != 1 || len(def[0].Evidence) != 3 {
		t.Fatalf("default evidenceN: %+v", def)
	}
}

func idsOf(parts []ScoredID) []uuid.UUID {
	out := make([]uuid.UUID, len(parts))
	for i, p := range parts {
		out[i] = p.ID
	}
	return out
}
