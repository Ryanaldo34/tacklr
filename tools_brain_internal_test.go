package tacklr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
)

// TestNewBrainTools_saveAndLinkDescribeTheHostSchema: save describes each
// kind it writes with its fields; link offers only the host's relations
// and says which kinds each joins.
func TestNewBrainTools_saveAndLinkDescribeTheHostSchema(t *testing.T) {
	t.Parallel()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(brain.NewMemoryGraph(brain.MemoryGraphOptions{})), brain.WithKinds(
		brain.KindSpec{Kind: "Deal", Description: "An open opportunity.", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "status", Type: brain.FieldTypeString, Required: true, Enum: []string{"open", "closed"}, Description: "Where it stands."},
		}},
		brain.KindSpec{Kind: "Person", IsParent: true},
	))
	if err != nil {
		t.Fatal(err)
	}

	tools := map[string]*Tool{}
	for _, tool := range newBrainTools(eng, nil, "s", []string{"Deal", "Person", "Memory"}, []brain.RelationSpec{
		{Relation: "stakeholder", Description: "a person on the deal", From: []string{"Deal"}, To: []string{"Person"}},
		{Relation: "mentions"},
	}) {
		tools[tool.Name()] = tool
	}

	save, link := tools["save"].Description(), tools["link"].Description()
	for _, want := range []string{"Kinds you may save:", "- Deal: An open opportunity.", "status (string, required, one of open|closed) Where it stands.", "- Person", "- Memory"} {
		if !strings.Contains(save, want) {
			t.Errorf("save description = %q, want %q", save, want)
		}
	}
	for _, want := range []string{"Relations (from → to):", "- stakeholder (Deal → Person): a person on the deal", "- mentions (any → any)"} {
		if !strings.Contains(link, want) {
			t.Errorf("link description = %q, want %q", link, want)
		}
	}
	if raw, _ := json.Marshal(tools["link"].Parameters()); !strings.Contains(string(raw), `"enum":["stakeholder","mentions"]`) {
		t.Errorf("link parameters = %s, want relation to be one of the host's relations", raw)
	}
}
