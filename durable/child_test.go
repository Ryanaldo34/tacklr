package durable

import (
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
)

func TestOverlaySpecialist_inheritsParentAndNests(t *testing.T) {
	parentModel := &testkit.ScriptedModel{}
	childModel := &testkit.ScriptedModel{}
	grand := &tacklr.Specialist{Name: "grand", Instructions: "g"}
	parent := AgentSpec{
		Name: "parent",
		Options: tacklr.AgentOptions{
			Model:  parentModel,
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "parent"},
			Specialists: []*tacklr.Specialist{nil, {
				Name:         "researcher",
				Instructions: "research",
				Model:        childModel,
				Specialists:  []*tacklr.Specialist{grand},
			}},
		},
	}
	got, err := OverlaySpecialist(parent, "researcher")
	if err != nil {
		t.Fatal(err)
	}
	if got.Options.Model != childModel || got.Options.Config.SystemPrompt != "research" {
		t.Fatalf("overlay: %+v", got.Options)
	}
	if tacklr.FindSpecialist(got.Options.Specialists, "grand") == nil {
		t.Fatal("nested worker missing")
	}
	if _, err := OverlaySpecialist(parent, "missing"); err == nil {
		t.Fatal("want missing worker")
	}
}
