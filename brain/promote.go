package brain

import (
	"cmp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	defaultSnippetCap = 240
	snippetLookbehind = 40
)

// promotedParent is a parent candidate after part aggregation.
type promotedParent struct {
	ParentID uuid.UUID
	Score    float64
	Evidence []Evidence
}

// promoteParents groups hits by parent. A hit with no parent_id is the
// record itself; part hits attach as evidence under parent_id. A record
// scores its own hit plus its kept evidence, so one whose own text matches
// ranks above one that matches only through a part. A record keeps its
// evidenceN best parts, each quoted in at most snippetCap runes around query.
func promoteParents(parts []ScoredID, evidenceN int, query string, snippetCap int) []promotedParent {
	type bucket struct {
		own      float64 // the record's own hit
		evidence []Evidence
	}
	byParent := make(map[uuid.UUID]bucket, len(parts))
	order := make([]uuid.UUID, 0, len(parts))

	for _, p := range parts {
		pid := p.ID
		isPart := p.ParentID != nil
		if isPart {
			pid = *p.ParentID
		}
		b, ok := byParent[pid]
		if !ok {
			order = append(order, pid)
		}
		if !isPart {
			b.own = max(b.own, p.Score)
		} else {
			b.evidence = append(b.evidence, Evidence{
				PartID:     p.ID,
				Title:      p.Title,
				Snippet:    snippetAround(p.Content, query, snippetCap),
				Score:      p.Score,
				Position:   p.Position,
				Properties: p.Properties,
			})
		}
		byParent[pid] = b
	}

	out := make([]promotedParent, 0, len(order))
	for _, pid := range order {
		b := byParent[pid]
		slices.SortFunc(b.evidence, func(a, b Evidence) int {
			if c := cmp.Compare(b.Score, a.Score); c != 0 {
				return c
			}
			return cmpUUID(a.PartID, b.PartID)
		})
		if len(b.evidence) > evidenceN {
			b.evidence = b.evidence[:evidenceN]
		}
		score := b.own
		for _, ev := range b.evidence {
			score += ev.Score
		}
		out = append(out, promotedParent{
			ParentID: pid,
			Score:    score,
			Evidence: b.evidence,
		})
	}
	slices.SortFunc(out, func(a, b promotedParent) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return cmpUUID(a.ParentID, b.ParentID)
	})
	return out
}

// snippetAround returns at most maxRunes of s, starting snippetLookbehind runes
// before the earliest query word. An empty query, or a query with no word in s,
// keeps the prefix. A window that is not the full text gains an ellipsis on the cut side.
func snippetAround(s, query string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	folded := strings.ToLower(s)
	idx := -1
	for _, tok := range strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) {
		if i := strings.Index(folded, tok); i >= 0 && (idx < 0 || i < idx) {
			idx = i
		}
	}
	if idx < 0 {
		cut := capRunes(s, maxRunes)
		if cut == s {
			return s
		}
		return cut + "…"
	}
	start := idx
	for n := snippetLookbehind; n > 0 && start > 0; n-- {
		start--
		for start > 0 && !utf8.RuneStart(s[start]) {
			start--
		}
	}
	cut := capRunes(s[start:], maxRunes)
	out := cut
	if start > 0 {
		out = "…" + out
	}
	if start+len(cut) < len(s) {
		out += "…"
	}
	return out
}
