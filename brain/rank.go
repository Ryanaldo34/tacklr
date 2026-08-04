package brain

import (
	"cmp"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
)

const defaultRRFk = 60

// rrfFuse merges ranked lists with Reciprocal Rank Fusion.
// Each input list must already be ordered best-first. Channel scores are ignored.
func rrfFuse(lists [][]ScoredID, k int) []ScoredID {
	if k <= 0 {
		k = defaultRRFk
	}
	type acc struct {
		rrf       float64
		UpdatedAt time.Time
		ParentID  *uuid.UUID
		Title     string
		Content   string
		Position  *int
	}
	byID := map[uuid.UUID]*acc{}
	order := make([]uuid.UUID, 0)

	for _, list := range lists {
		for rank, item := range list {
			a, ok := byID[item.ID]
			if !ok {
				a = &acc{
					UpdatedAt: item.UpdatedAt,
					ParentID:  item.ParentID,
					Title:     item.Title,
					Content:   item.Content,
					Position:  item.Position,
				}
				byID[item.ID] = a
				order = append(order, item.ID)
			}
			a.rrf += 1.0 / float64(k+rank+1)
			if item.UpdatedAt.After(a.UpdatedAt) {
				a.UpdatedAt = item.UpdatedAt
			}
			if a.ParentID == nil && item.ParentID != nil {
				a.ParentID = item.ParentID
			}
			if a.Title == "" && item.Title != "" {
				a.Title = item.Title
			}
			if a.Content == "" && item.Content != "" {
				a.Content = item.Content
			}
			if a.Position == nil && item.Position != nil {
				a.Position = item.Position
			}
		}
	}

	out := make([]ScoredID, 0, len(order))
	for _, id := range order {
		a := byID[id]
		out = append(out, ScoredID{
			ID:        id,
			Score:     a.rrf,
			UpdatedAt: a.UpdatedAt,
			ParentID:  a.ParentID,
			Title:     a.Title,
			Content:   a.Content,
			Position:  a.Position,
		})
	}
	return out
}

// applyTemporal multiplies scores by exp(-λ * age_days) using updated_at.
// lambda <= 0 leaves scores unchanged.
func applyTemporal(parts []ScoredID, lambda float64, now time.Time) {
	if lambda <= 0 {
		return
	}
	now = now.UTC()
	for i := range parts {
		age := now.Sub(parts[i].UpdatedAt.UTC()).Hours() / 24.0
		if age < 0 {
			age = 0
		}
		parts[i].Score *= math.Exp(-lambda * age)
	}
}

// cmpScored orders by score desc, then updated_at desc, then id asc.
func cmpScored(a, b ScoredID) int {
	if c := cmp.Compare(b.Score, a.Score); c != 0 {
		return c
	}
	if a.UpdatedAt.After(b.UpdatedAt) {
		return -1
	}
	if a.UpdatedAt.Before(b.UpdatedAt) {
		return 1
	}
	return cmp.Compare(a.ID.String(), b.ID.String())
}

func sortScored(parts []ScoredID) {
	slices.SortFunc(parts, cmpScored)
}
