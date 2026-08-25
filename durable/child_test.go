package durable

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
)

func TestChildSessionID_andSpawnParse(t *testing.T) {
	id := ChildSessionID("parent", "researcher", "call1")
	if id != "parent/w/researcher/call1" {
		t.Fatalf("id=%s", id)
	}
	call, err := ParseSpawnCall(`{"specialist":"researcher","task_description_and_context":"dig","block":false}`)
	if err != nil || call.Block || call.Specialist != "researcher" || call.Task != "dig" {
		t.Fatalf("parse: %+v %v", call, err)
	}
	def, err := ParseSpawnCall(`{"specialist":"w","task_description_and_context":"t"}`)
	if err != nil || !def.Block {
		t.Fatalf("default block: %+v %v", def, err)
	}
	if _, err := ParseSpawnCall(`{}`); err == nil {
		t.Fatal("empty")
	}
}

func TestOverlaySpecialist_inheritsParentAndNests(t *testing.T) {
	parentModel := &testkit.ScriptedModel{}
	childModel := &testkit.ScriptedModel{}
	grand := &tacklr.Specialist{Name: "grand", Instructions: "g"}
	parent := AgentSpec{
		Name: "parent",
		Options: tacklr.AgentOptions{
			Model:  parentModel,
			Config: tacklr.Config{MaxWindowSize: 8192, SystemPrompt: "parent"},
			Specialists: []*tacklr.Specialist{{
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
	list := FormatChildList(nil)
	if list != "No child sessions." {
		t.Fatalf("empty list=%q", list)
	}
	rows := []SessionStatus{{ID: "c1", Specialist: "researcher", State: SessionRunning, Waiting: true}}
	if !strings.Contains(FormatChildList(rows), "status=running") {
		t.Fatalf("HITL still running: %s", FormatChildList(rows))
	}
	if !strings.Contains(FormatChild(rows[0]), "Still running") {
		t.Fatalf("format job: %s", FormatChild(rows[0]))
	}
	done := []SessionStatus{{ID: "c1", State: SessionComplete, Result: "ok"}}
	nudge := ChildrenNudge(done)
	if nudge == "" || !strings.Contains(nudge, "c1") || !strings.Contains(nudge, "completed") {
		t.Fatalf("nudge must include uncollected complete children: %q", nudge)
	}
}
