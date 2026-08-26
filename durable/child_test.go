package durable

import (
	"errors"
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
	spec, task, err := NormalizeSpawn("  researcher ", "  do it  ")
	if err != nil || spec != "researcher" || task != "do it" {
		t.Fatalf("normalize = %q %q %v", spec, task, err)
	}
	if _, _, err := NormalizeSpawn("", "x"); err == nil {
		t.Fatal("want specialist required")
	}
	if err := UnknownChild("z"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	if ChildState(SessionComplete) != tacklr.ChildCompleted || ChildState(SessionFailed) != tacklr.ChildFailed || ChildState(SessionRunning) != tacklr.ChildRunning {
		t.Fatal(ChildState(SessionRunning))
	}
}
