package agentbench

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestReport_writeAndList(t *testing.T) {
	rep := Report{
		Model:      "m",
		EmbedModel: "e",
		StartedAt:  time.Unix(0, 0).UTC(),
		Duration:   time.Second,
		GatesOK:    true,
		GateNotes:  []string{"note"},
		Suites: map[string]SuiteResult{
			SuiteMemory: {
				Passed: 1, Failed: 0, Skipped: 1, SuccessRate: 1,
				Cases: []CaseResult{
					{ID: "mem.ok", Success: true},
					{ID: "mem.skip", Skipped: true, SkipWhy: "no exa"},
				},
			},
		},
	}
	var buf bytes.Buffer
	if err := WriteJSON(&buf, rep); err != nil || !strings.Contains(buf.String(), `"gates_ok": true`) {
		t.Fatalf("json: %v %s", err, buf.String())
	}
	md := FormatMarkdown(rep)
	if !strings.Contains(md, "agent-bench scorecard") || !strings.Contains(md, "mem.ok") {
		t.Fatalf("md: %s", md)
	}
	buf.Reset()
	ListCases(&buf, SuiteMemory)
	if !strings.Contains(buf.String(), SuiteMemory) {
		t.Fatalf("list: %s", buf.String())
	}
}
