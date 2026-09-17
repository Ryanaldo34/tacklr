package tacklr

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestSystemPrompt_knowledgeGuidanceWhenBrainSet(t *testing.T) {
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithLexicalOnly())
	if err != nil {
		t.Fatal(err)
	}
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model:  &mockStrategy{},
		Brain:  eng,
	})
	t.Cleanup(h.Close)
	prompt := h.constructSystemPrompt()
	if !strings.Contains(prompt, "knowledge store") {
		t.Fatalf("want knowledge-store guidance when Brain is set:\n%s", prompt)
	}
}
