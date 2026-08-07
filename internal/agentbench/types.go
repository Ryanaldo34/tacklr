// Package agentbench runs multi-turn harness benchmarks aligned with
// industry agent/memory/tool evaluation shapes (LoCoMo-style memory,
// multi-hop QA, τ-bench-style domain end state, plan+interrupt, web-augmented).
//
// Case definitions and seed worlds live in Go (see cases_*.go), not external JSONL.
package agentbench

import (
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// Suite identifiers.
const (
	SuiteMemory        = "memory"
	SuiteMultihopQA    = "multihop_qa"
	SuiteToolDomain    = "tool_domain"
	SuiteWebAugmented  = "web_augmented"
	SuitePlanInterrupt = "plan_interrupt"
)

// AllSuites is the default suite list for -suite all.
var AllSuites = []string{
	SuitePlanInterrupt,
	SuiteMemory,
	SuiteMultihopQA,
	SuiteToolDomain,
	SuiteWebAugmented,
}

// Case is one multi-turn benchmark scenario with in-memory seed and rule expects.
type Case struct {
	ID    string
	Suite string
	// RequiresExa skips the case when EXA_API_KEY is empty (not a failure).
	RequiresExa bool
	// RestoreSession: after the first N-1 turns, rebuild agent from session for the last turn.
	RestoreSession bool
	Turns          []string
	// InterruptChoiceTitle is matched against ask_user_choice options (case-insensitive contains).
	// When empty, selectionIdx 0 is used.
	InterruptChoiceTitle string
	// InterruptSelectionIdx used when title is empty or unmatched.
	InterruptSelectionIdx int
	Seed                  SeedWorld
	Expect                Expect
}

// SeedWorld is applied under a fresh namespace before the first turn.
type SeedWorld struct {
	// Objects with fixed IDs (for multihop evidence gold). Empty ID → generated on seed.
	Objects []SeedObject
	Edges   []SeedEdge
}

// SeedObject is a brain row to Put before the case runs.
type SeedObject struct {
	ID       uuid.UUID
	Kind     string
	Title    string
	Summary  string
	Content  string
	ParentID *uuid.UUID
	Position *int
	Props    map[string]any
}

// SeedEdge is a Link between two seed object ids (must exist in Objects).
type SeedEdge struct {
	From, To uuid.UUID
	Relation string
	Note     string
}

// Expect is a rule-based judge. All set fields must pass for Success.
type Expect struct {
	// FinalContainsAny: final assistant text must contain at least one (case-insensitive).
	FinalContainsAny []string
	// FinalContainsAll: every string must appear.
	FinalContainsAll []string
	// MustTools: each inner list is an OR group; every group must match some tool name used.
	MustTools [][]string
	// MustNotTools: none of these tool names may appear.
	MustNotTools []string
	// MustInterrupt: at least one ask_user_choice (or any interrupt) must have fired.
	MustInterrupt bool
	// BrainKindContains: after the case, FindObjects or list kinds via search for this kind
	// with query substring must return ≥1 hit (empty Query → any of that kind via find with kind filter + broad query).
	BrainKind  string
	BrainQuery string
	// BrainTitleContains: some object of BrainKind (or any if empty) has title containing this.
	BrainTitleContains string
	// GoldEvidenceIDs: at least one must appear in tool args or results text (evidence hit).
	GoldEvidenceIDs []uuid.UUID
}

// ToolCallRecord is one observed tool invocation from the stream.
type ToolCallRecord struct {
	Name      string
	Arguments string
	Result    string // best-effort from tool_result content
}

// TurnTrace is one user turn of a case.
type TurnTrace struct {
	Prompt       string
	Assistant    string
	Tools        []ToolCallRecord
	Interrupts   int
	Error        string
	Duration     time.Duration
	RestoredSess bool
}

// CaseResult is the judged outcome of one case.
type CaseResult struct {
	ID      string   `json:"id"`
	Suite   string   `json:"suite"`
	Skipped bool     `json:"skipped,omitempty"`
	SkipWhy string   `json:"skip_why,omitempty"`
	Success bool     `json:"success"`
	Notes   []string `json:"notes,omitempty"`
	// Scores are named 0/1 or fractions for aggregation.
	Scores map[string]float64 `json:"scores,omitempty"`
	Turns  []TurnTrace        `json:"-"` // omit heavy traces from default JSON unless verbose
}

// SuiteResult aggregates cases in one suite.
type SuiteResult struct {
	Suite       string       `json:"suite"`
	N           int          `json:"n"`
	Skipped     int          `json:"skipped"`
	Passed      int          `json:"passed"`
	Failed      int          `json:"failed"`
	SuccessRate float64      `json:"success_rate"` // among non-skipped
	IllegalRate float64      `json:"illegal_rate"`
	Cases       []CaseResult `json:"cases"`
}

// Report is the full scorecard written to -out.
type Report struct {
	Model      string                 `json:"model"`
	EmbedModel string                 `json:"embed_model,omitempty"`
	StartedAt  time.Time              `json:"started_at"`
	Duration   time.Duration          `json:"duration_ms"` // wall; JSON via custom if needed
	Suites     map[string]SuiteResult `json:"suites"`
	GatesOK    bool                   `json:"gates_ok"`
	GateNotes  []string               `json:"gate_notes,omitempty"`
}

// KindSpecs used for all seeded worlds.
func KindSpecs() []brain.KindSpec {
	return []brain.KindSpec{
		{Kind: "Document", IsParent: true},
		{Kind: "Chunk", IsPart: true},
		{Kind: "Person", IsParent: true},
		{Kind: "Project", IsParent: true},
		{Kind: "Fact", IsParent: true},
		{Kind: "Memory", IsParent: true},
		{Kind: "Discovery", IsParent: true},
	}
}

// SharedSystemPrompt steers plan + brain + web tool use without domain hardcoding.
const SharedSystemPrompt = `You are a careful work assistant with knowledge tools and a plan.

Rules:
- Prefer tools over guessing. Use create_plan and complete_todo for multi-step work.
- Use knowledge tools (schema, search, find_exact, find_objects, expand, read, find_links, link, save_*) for durable facts and notes.
- Save durable preferences with save_memory or save_fact (clear title and summary).
- Use ask_user_choice when you need a discrete user decision before writing or proceeding.
- Use web_search only when internal knowledge is insufficient for a factual gap.
- When answering, be concise and mention relevant note titles when known.
`
