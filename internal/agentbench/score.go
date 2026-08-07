package agentbench

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func judgeCase(c Case, eng *brain.Engine, scope brain.Scope, turns []TurnTrace) CaseResult {
	cr := CaseResult{
		ID:     c.ID,
		Suite:  c.Suite,
		Turns:  turns,
		Scores: map[string]float64{},
		Notes:  nil,
	}
	var final strings.Builder
	var tools []ToolCallRecord
	var interrupts int
	var turnErr string
	for _, t := range turns {
		final.WriteString(t.Assistant)
		final.WriteByte('\n')
		tools = append(tools, t.Tools...)
		interrupts += t.Interrupts
		if t.Error != "" {
			turnErr = t.Error
		}
	}
	finalText := final.String()
	toolNames := make([]string, 0, len(tools))
	var toolBlob strings.Builder
	for _, t := range tools {
		toolNames = append(toolNames, t.Name)
		toolBlob.WriteString(t.Name)
		toolBlob.WriteString(t.Arguments)
		toolBlob.WriteString(t.Result)
	}
	blob := toolBlob.String() + finalText

	ok := true
	add := func(key string, pass bool, note string) {
		if pass {
			cr.Scores[key] = 1
		} else {
			cr.Scores[key] = 0
			ok = false
			if note != "" {
				cr.Notes = append(cr.Notes, note)
			}
		}
	}

	if turnErr != "" {
		add("no_error", false, "turn error: "+turnErr)
	} else {
		add("no_error", true, "")
	}

	if len(c.Expect.FinalContainsAny) > 0 {
		add("final_any", containsAny(finalText, c.Expect.FinalContainsAny),
			"final missing any of "+strings.Join(c.Expect.FinalContainsAny, "|"))
	}
	if len(c.Expect.FinalContainsAll) > 0 {
		add("final_all", containsAll(finalText, c.Expect.FinalContainsAll),
			"final missing all-of requirement")
	}
	if len(c.Expect.MustTools) > 0 {
		canon := make([]string, 0, len(toolNames))
		for _, n := range toolNames {
			if x := normalizeToolName(n); x != "" {
				canon = append(canon, x)
			}
		}
		add("must_tools", toolsSatisfy(toolNames, c.Expect.MustTools),
			"required tools not used (seen: "+strings.Join(canon, ",")+")")
	}
	if len(c.Expect.MustNotTools) > 0 {
		bad := false
		for _, n := range toolNames {
			nn := normalizeToolName(n)
			for _, ban := range c.Expect.MustNotTools {
				if nn != "" && nn == normalizeToolName(ban) {
					bad = true
				}
			}
		}
		add("must_not_tools", !bad, "forbidden tool used")
	}
	if c.Expect.MustInterrupt {
		add("interrupt", interrupts > 0, "expected ask_user_choice / interrupt")
	}
	if c.Expect.BrainKind != "" || c.Expect.BrainTitleContains != "" || c.Expect.BrainQuery != "" {
		hit := brainHit(eng, scope, c.Expect)
		add("brain_state", hit, "brain state expectation failed")
	}
	if len(c.Expect.GoldEvidenceIDs) > 0 {
		add("evidence_hit", evidenceHit(blob, c.Expect.GoldEvidenceIDs), "gold evidence ids not seen in tools/answer")
	}

	cr.Success = ok
	if ok {
		cr.Scores["success"] = 1
	} else {
		cr.Scores["success"] = 0
	}
	return cr
}

func containsAny(text string, subs []string) bool {
	low := strings.ToLower(text)
	for _, s := range subs {
		if s != "" && strings.Contains(low, strings.ToLower(s)) {
			return true
		}
	}
	return false
}

func containsAll(text string, subs []string) bool {
	low := strings.ToLower(text)
	for _, s := range subs {
		if s == "" {
			continue
		}
		if !strings.Contains(low, strings.ToLower(s)) {
			return false
		}
	}
	return true
}

// normalizeToolName maps stream tool labels (often DisplayName) to canonical names.
// e.g. "Create Plan" → "create_plan", "Knowledge Search" → "search".
func normalizeToolName(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" || n == "?" {
		return ""
	}
	n = strings.ReplaceAll(n, " ", "_")
	// DisplayName → tool Name (tools_brain / builtins).
	switch n {
	case "knowledge_search":
		return "search"
	case "knowledge_schema":
		return "schema"
	case "find_objects", "find_object":
		return "find_objects"
	case "find_exact":
		return "find_exact"
	case "find_links":
		return "find_links"
	case "expand_object", "expand":
		return "expand"
	case "read_object", "read":
		return "read"
	case "link_objects", "link":
		return "link"
	case "save_memory", "save_fact", "save_discovery":
		return n
	case "create_plan", "list_plan", "edit_plan", "complete_todo":
		return n
	case "ask_user_choice":
		return "ask_user_choice"
	case "web_search", "web_fetch":
		return n
	case "continue":
		return "continue"
	default:
		return n
	}
}

func toolsSatisfy(used []string, groups [][]string) bool {
	set := map[string]struct{}{}
	for _, u := range used {
		if n := normalizeToolName(u); n != "" {
			set[n] = struct{}{}
		}
	}
	for _, g := range groups {
		ok := false
		for _, name := range g {
			if _, hit := set[normalizeToolName(name)]; hit {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func evidenceHit(blob string, ids []uuid.UUID) bool {
	low := strings.ToLower(blob)
	for _, id := range ids {
		if strings.Contains(low, strings.ToLower(id.String())) {
			return true
		}
	}
	return false
}

func brainHit(eng *brain.Engine, scope brain.Scope, exp Expect) bool {
	ctx := context.Background()
	q := strings.TrimSpace(exp.BrainQuery)
	if q == "" {
		q = strings.TrimSpace(exp.BrainTitleContains)
	}
	if q == "" {
		q = exp.BrainKind
	}
	if q == "" {
		return true
	}
	sc := brain.NewSearchContext()
	// Prefer find_objects when kind is a first-class entity kind.
	if exp.BrainKind != "" && exp.BrainKind != "Chunk" && exp.BrainKind != "Document" {
		page, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{
			Query: q, Kinds: []string{exp.BrainKind}, Limit: 20,
		}, sc)
		if err == nil {
			for _, o := range page.Objects {
				if exp.BrainTitleContains == "" || strings.Contains(strings.ToLower(o.Title), strings.ToLower(exp.BrainTitleContains)) {
					return true
				}
			}
		}
	}
	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: q, Limit: 20}, sc)
	if err != nil {
		return false
	}
	for _, o := range page.Objects {
		if exp.BrainKind != "" && !strings.EqualFold(o.Kind, exp.BrainKind) {
			// search returns parents; kind filter soft
			continue
		}
		if exp.BrainTitleContains == "" || strings.Contains(strings.ToLower(o.Title), strings.ToLower(exp.BrainTitleContains)) {
			return true
		}
		// Also accept if query terms appear in title
		if exp.BrainTitleContains == "" {
			return true
		}
	}
	// Fallback: find_objects without kind
	page2, err := eng.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: q, Limit: 20}, sc)
	if err != nil {
		return false
	}
	for _, o := range page2.Objects {
		if exp.BrainKind != "" && !strings.EqualFold(o.Kind, exp.BrainKind) {
			continue
		}
		if exp.BrainTitleContains == "" || strings.Contains(strings.ToLower(o.Title), strings.ToLower(exp.BrainTitleContains)) {
			return true
		}
	}
	return false
}

// DefaultGates are v1 success-rate floors (non-skipped cases only).
func DefaultGates() map[string]float64 {
	return map[string]float64{
		SuiteMemory:        0.70,
		SuiteMultihopQA:    0.60,
		SuiteToolDomain:    0.70,
		SuitePlanInterrupt: 0.75,
		SuiteWebAugmented:  0.50,
	}
}

// EvaluateGates returns whether gates pass and human-readable notes.
func EvaluateGates(rep Report, hasExa bool) (bool, []string) {
	gates := DefaultGates()
	ok := true
	var notes []string
	for suite, min := range gates {
		sr, exists := rep.Suites[suite]
		if !exists {
			continue
		}
		if suite == SuiteWebAugmented && !hasExa {
			notes = append(notes, "web_augmented: skipped (no EXA_API_KEY)")
			continue
		}
		if sr.Passed+sr.Failed == 0 {
			notes = append(notes, suite+": no executed cases")
			continue
		}
		if sr.SuccessRate+1e-9 < min {
			ok = false
			notes = append(notes, suite+": success_rate below gate")
		}
	}
	return ok, notes
}
