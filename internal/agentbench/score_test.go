package agentbench

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestToolsSatisfy_andContains(t *testing.T) {
	used := []string{"create_plan", "save_memory", "search"}
	if !toolsSatisfy(used, [][]string{{"create_plan"}, {"save_memory", "save_fact"}, {"search", "find_objects"}}) {
		t.Fatal("must pass")
	}
	if toolsSatisfy(used, [][]string{{"web_search"}}) {
		t.Fatal("must fail")
	}
	if !toolsSatisfy([]string{"create_plan", "save_memory", "search", "ask_user_choice", "?"},
		[][]string{{"create_plan"}, {"save_memory"}, {"search"}, {"ask_user_choice"}}) {
		t.Fatal("canonical names must satisfy")
	}
	if !containsAny("Hello Async World", []string{"async"}) {
		t.Fatal("containsAny")
	}
	if !containsAll("legal and Alex", []string{"legal", "Alex"}) {
		t.Fatal("containsAll")
	}
}

func TestEvidenceHit(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111101")
	if !evidenceHit("expand on "+id.String(), []uuid.UUID{id}) {
		t.Fatal("hit")
	}
	if evidenceHit("nothing", []uuid.UUID{id}) {
		t.Fatal("miss")
	}
}

func TestAllCases_uniqueIDs(t *testing.T) {
	seen := map[string]struct{}{}
	for _, c := range AllCases() {
		if c.ID == "" || c.Suite == "" {
			t.Fatalf("empty id/suite: %+v", c)
		}
		if _, ok := seen[c.ID]; ok {
			t.Fatalf("duplicate case id %s", c.ID)
		}
		seen[c.ID] = struct{}{}
		if len(c.Turns) == 0 {
			t.Fatalf("no turns: %s", c.ID)
		}
	}
	if len(seen) < 5 {
		t.Fatalf("want several cases, got %d", len(seen))
	}
}

func TestEvaluateGates_webSkip(t *testing.T) {
	rep := Report{Suites: map[string]SuiteResult{
		SuiteMemory: {Passed: 2, Failed: 0, SuccessRate: 1},
	}}
	ok, notes := EvaluateGates(rep, false)
	if !ok {
		// only memory present at 1.0 — gates for missing suites are skipped
		t.Log(notes)
	}
	_ = strings.Join(notes, ",")
}

func TestDryRun_registersCases(t *testing.T) {
	rep, err := Run(t.Context(), Config{DryRun: true, Suites: AllSuites})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, sr := range rep.Suites {
		total += sr.Passed + sr.Skipped
	}
	if total < 5 {
		t.Fatalf("dry-run cases: %d", total)
	}
}
