package brain

import (
	"cmp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const defaultSnippetCap = 240

// promotedParent is a parent candidate after part aggregation.
type promotedParent struct {
	ParentID uuid.UUID
	Score    float64
	Evidence []Evidence
}

// promoteParents groups hits by parent. A hit with no parent_id is itself a parent
// (no evidence). Part hits attach as evidence under parent_id.
// Parent score is the max contributing score; evidence keeps top evidenceN parts.
func promoteParents(parts []ScoredID, evidenceN int) []promotedParent {
	if evidenceN <= 0 {
		evidenceN = 3
	}
	type bucket struct {
		score    float64
		evidence []Evidence
	}
	byParent := map[uuid.UUID]*bucket{}
	order := make([]uuid.UUID, 0)

	for _, p := range parts {
		var pid uuid.UUID
		isPart := p.ParentID != nil
		if isPart {
			pid = *p.ParentID
		} else {
			pid = p.ID
		}
		b, ok := byParent[pid]
		if !ok {
			b = &bucket{score: p.Score}
			byParent[pid] = b
			order = append(order, pid)
		} else if p.Score > b.score {
			b.score = p.Score
		}
		if !isPart {
			continue
		}
		b.evidence = append(b.evidence, Evidence{
			PartID:   p.ID,
			Title:    p.Title,
			Snippet:  snippet(p.Content, defaultSnippetCap),
			Score:    p.Score,
			Position: p.Position,
		})
	}

	out := make([]promotedParent, 0, len(order))
	for _, pid := range order {
		b := byParent[pid]
		slices.SortFunc(b.evidence, func(a, b Evidence) int {
			if c := cmp.Compare(b.Score, a.Score); c != 0 {
				return c
			}
			return cmp.Compare(a.PartID.String(), b.PartID.String())
		})
		if len(b.evidence) > evidenceN {
			b.evidence = b.evidence[:evidenceN]
		}
		out = append(out, promotedParent{
			ParentID: pid,
			Score:    b.score,
			Evidence: b.evidence,
		})
	}
	slices.SortFunc(out, func(a, b promotedParent) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return cmp.Compare(a.ParentID.String(), b.ParentID.String())
	})
	return out
}

func snippet(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if s == "" || maxRunes <= 0 {
		return s
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes]) + "…"
}
