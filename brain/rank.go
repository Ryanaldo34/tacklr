package brain

import (
	"bytes"
	"cmp"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
)

const defaultRRFk = 60

// FuseRanks merges best-first lists with reciprocal rank fusion.
// k == 0 selects the default fusion constant. Channel scores are ignored.
// The score of an id is the sum, over every list that contains it, of 1/(k+r),
// where r is the 1-based position in that list (Cormack, Clarke, Buettcher 2009).
// Loop rank is 0-based, so the term is written 1/(k+rank+1).
func FuseRanks(lists [][]ScoredID, k int) []ScoredID {
	k = cmp.Or(k, defaultRRFk)
	type acc struct {
		rrf        float64
		UpdatedAt  time.Time
		ParentID   *uuid.UUID
		Title      string
		Content    string
		Position   *int
		Properties map[string]any
	}
	byID := make(map[uuid.UUID]acc)
	order := make([]uuid.UUID, 0)

	for _, list := range lists {
		for rank, item := range list {
			a, ok := byID[item.ID]
			if !ok {
				a = acc{
					UpdatedAt:  item.UpdatedAt,
					ParentID:   item.ParentID,
					Title:      item.Title,
					Content:    item.Content,
					Position:   item.Position,
					Properties: item.Properties,
				}
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
			if a.Properties == nil && item.Properties != nil {
				a.Properties = item.Properties
			}
			byID[item.ID] = a
		}
	}

	out := make([]ScoredID, 0, len(order))
	for _, id := range order {
		a := byID[id]
		out = append(out, ScoredID{
			ID:         id,
			Score:      a.rrf,
			UpdatedAt:  a.UpdatedAt,
			ParentID:   a.ParentID,
			Title:      a.Title,
			Content:    a.Content,
			Position:   a.Position,
			Properties: a.Properties,
		})
	}
	return out
}

// applyTemporal multiplies scores by exp(-λ * age_days) using updated_at.
// lambda <= 0 leaves scores unchanged.
// Zero UpdatedAt is left untouched (e.g. Helix search hits only carry $distance rank).
func applyTemporal(parts []ScoredID, lambda float64, now time.Time) {
	if lambda <= 0 {
		return
	}
	for i := range parts {
		parts[i].Score *= ageFactor(lambda, now, parts[i].UpdatedAt)
	}
}

// cmpScored orders by score desc, then updated_at desc, then id asc.
func cmpScored(a, b ScoredID) int {
	if c := cmp.Compare(b.Score, a.Score); c != 0 {
		return c
	}
	if c := a.UpdatedAt.Compare(b.UpdatedAt); c != 0 {
		return -c // newer first
	}
	return cmpUUID(a.ID, b.ID)
}

func cmpUUID(a, b uuid.UUID) int {
	return bytes.Compare(a[:], b[:])
}

func sortScored(parts []ScoredID) {
	slices.SortFunc(parts, cmpScored)
}

// TopScores keeps hits with score > 0, orders them best-first, and returns at most k.
// k <= 0 keeps every positive hit. Cosine lists use this instead of FuseRanks
// because those scores share one scale.
func TopScores(in []ScoredID, k int) []ScoredID {
	out := in[:0]
	for _, hit := range in {
		if hit.Score > 0 {
			out = append(out, hit)
		}
	}
	sortScored(out)
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

// Ranking is how a backend fuses ranked lists: reciprocal rank fusion with
// constant RRFk (0 is 60), after each list is aged by exp(-Decay * age in
// days) using UpdatedAt and re-sorted. Decay 0 keeps the lists as ranked.
// Now is the clock for age (nil is time.Now).
type Ranking struct {
	RRFk  int
	Decay float64
	Now   func() time.Time
}

// Fuse ages each list, fuses them, and returns the hits best first. The
// lists are changed in place.
func (r Ranking) Fuse(lists ...[]ScoredID) []ScoredID {
	if r.Decay > 0 {
		now := time.Now()
		if r.Now != nil {
			now = r.Now()
		}
		for _, list := range lists {
			applyTemporal(list, r.Decay, now)
			sortScored(list)
		}
	}
	fused := FuseRanks(lists, r.RRFk)
	sortScored(fused)
	return fused
}

// FuseEdges fuses ranked edge lists the same way, best first. An edge in
// several lists keeps its first hit's metadata; decay uses Meta.UpdatedAt.
func (r Ranking) FuseEdges(lists ...[]EdgeSearchHit) []EdgeSearchHit {
	k := cmp.Or(r.RRFk, defaultRRFk)
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	type key struct {
		from, to uuid.UUID
		rel      string
	}
	byKey := make(map[key]int)
	var out []EdgeSearchHit
	for _, list := range lists {
		if r.Decay > 0 {
			for i := range list {
				list[i].Score *= ageFactor(r.Decay, now, list[i].Meta.UpdatedAt)
			}
			slices.SortStableFunc(list, func(a, b EdgeSearchHit) int { return cmp.Compare(b.Score, a.Score) })
		}
		for rank, h := range list {
			kk := key{h.FromID, h.ToID, h.RelationType}
			i, ok := byKey[kk]
			if !ok {
				i = len(out)
				byKey[kk] = i
				h.Score = 0
				out = append(out, h)
			}
			out[i].Score += 1.0 / float64(k+rank+1)
		}
	}
	slices.SortStableFunc(out, func(a, b EdgeSearchHit) int { return cmp.Compare(b.Score, a.Score) })
	return out
}

// ageFactor is exp(-lambda * age in days); a zero time does not age.
func ageFactor(lambda float64, now, at time.Time) float64 {
	if lambda <= 0 || at.IsZero() {
		return 1
	}
	return math.Exp(-lambda * max(now.Sub(at).Hours()/24, 0))
}
