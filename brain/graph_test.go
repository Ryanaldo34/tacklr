package brain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSplitRelationTypes_separatesContainmentFromGraphLabels(t *testing.T) {
	wantC, labels := SplitRelationTypes([]string{"contains", "REFERENCES", "part_of", "depends_on", "contains"})
	if !wantC || len(labels) != 2 {
		t.Fatalf("mixed = %v %v", wantC, labels)
	}
	for name, rels := range map[string][]string{"nil": nil, "blank": {"", "  "}} {
		if wantC, labels := SplitRelationTypes(rels); !wantC || len(labels) != 0 {
			t.Fatalf("%s relations = %v %v, want containment only", name, wantC, labels)
		}
	}
}

func TestEdgeMeta_IsZero(t *testing.T) {
	id := uuid.New()
	if !(EdgeMeta{}).IsZero() {
		t.Fatal("empty meta is not zero")
	}
	for name, m := range map[string]EdgeMeta{
		"note": {Note: "x"}, "status": {Status: "active"}, "role": {Role: "primary"}, "confidence": {Confidence: 0.1},
		"evidence": {EvidenceID: &id}, "created": {CreatedAt: time.Now()}, "updated": {UpdatedAt: time.Now()},
	} {
		if m.IsZero() {
			t.Errorf("meta with only %s is zero", name)
		}
	}
}
