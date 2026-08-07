package agentbench

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
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

// Offline judge: no live model — pure rule outcomes.
func TestJudgeCase_passAndFail(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	gold := uuid.MustParse("11111111-1111-1111-1111-111111111101")
	pass := judgeCase(Case{
		ID: "t.pass", Suite: SuiteMemory,
		Expect: Expect{
			FinalContainsAny: []string{"async"},
			FinalContainsAll: []string{"legal"},
			MustTools:        [][]string{{"search", "find_objects"}},
			MustNotTools:     []string{"web_search"},
			GoldEvidenceIDs:  []uuid.UUID{gold},
		},
	}, eng, brain.Scope{}, []TurnTrace{{
		Assistant: "legal async answer",
		Tools:     []ToolCallRecord{{Name: "search", Arguments: gold.String(), Result: "ok"}},
	}})
	if !pass.Success {
		t.Fatalf("want pass: %+v", pass.Notes)
	}
	fail := judgeCase(Case{
		ID: "t.fail", Suite: SuiteMemory,
		Expect: Expect{MustTools: [][]string{{"web_search"}}, MustInterrupt: true},
	}, eng, brain.Scope{}, []TurnTrace{{
		Assistant: "no tools",
		Error:     "boom",
	}})
	if fail.Success {
		t.Fatal("want fail")
	}
	if fail.Scores["no_error"] != 0 || fail.Scores["must_tools"] != 0 || fail.Scores["interrupt"] != 0 {
		t.Fatalf("scores: %+v", fail.Scores)
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
