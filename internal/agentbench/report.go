package agentbench

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// WriteJSON writes the scorecard as JSON.
func WriteJSON(w io.Writer, rep Report) error {
	type wire struct {
		Model      string                 `json:"model"`
		EmbedModel string                 `json:"embed_model,omitempty"`
		StartedAt  time.Time              `json:"started_at"`
		Duration   string                 `json:"duration"`
		Suites     map[string]SuiteResult `json:"suites"`
		GatesOK    bool                   `json:"gates_ok"`
		GateNotes  []string               `json:"gate_notes,omitempty"`
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(wire{
		Model:      rep.Model,
		EmbedModel: rep.EmbedModel,
		StartedAt:  rep.StartedAt,
		Duration:   rep.Duration.String(),
		Suites:     rep.Suites,
		GatesOK:    rep.GatesOK,
		GateNotes:  rep.GateNotes,
	})
}

// FormatMarkdown returns a compact scorecard for stdout.
func FormatMarkdown(rep Report) string {
	var b strings.Builder
	b.WriteString("# agent-bench scorecard\n\n")
	fmt.Fprintf(&b, "- model: `%s`\n- embed_model: `%s`\n- duration: %s\n- gates_ok: **%v**\n\n",
		rep.Model, rep.EmbedModel, rep.Duration, rep.GatesOK)
	if len(rep.GateNotes) > 0 {
		b.WriteString("## Gate notes\n\n")
		for _, n := range rep.GateNotes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		b.WriteByte('\n')
	}
	b.WriteString("| Suite | Passed | Failed | Skipped | Success rate |\n")
	b.WriteString("|-------|--------|--------|---------|--------------|\n")
	for _, name := range AllSuites {
		sr, ok := rep.Suites[name]
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %.0f%% |\n",
			name, sr.Passed, sr.Failed, sr.Skipped, sr.SuccessRate*100)
	}
	b.WriteString("\n## Cases\n\n")
	for _, name := range AllSuites {
		sr, ok := rep.Suites[name]
		if !ok {
			continue
		}
		for _, c := range sr.Cases {
			status := "FAIL"
			if c.Skipped {
				status = "SKIP"
			} else if c.Success {
				status = "PASS"
			}
			fmt.Fprintf(&b, "- `%s` **%s**", c.ID, status)
			if c.SkipWhy != "" {
				fmt.Fprintf(&b, " (%s)", c.SkipWhy)
			}
			if len(c.Notes) > 0 {
				fmt.Fprintf(&b, " — %s", strings.Join(c.Notes, "; "))
			}
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// ListCases prints case ids for -list.
func ListCases(w io.Writer, suite string) {
	for _, c := range CasesForSuite(suite) {
		exa := ""
		if c.RequiresExa {
			exa = " [requires_exa]"
		}
		fmt.Fprintf(w, "%s\t%s%s\n", c.Suite, c.ID, exa)
	}
}
