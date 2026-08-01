package control

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestPlanStore_setGet(t *testing.T) {
	p := NewPlanStore()
	if p.HasActive() {
		t.Fatal("empty store should not be active")
	}
	p.Set([]Todo{{Title: "A", Status: streaming.TodoStatusPending}})
	if !p.HasActive() {
		t.Fatal("expected active after Set")
	}
	plan := p.Get()
	if len(plan) != 1 || plan[0].Title != "A" {
		t.Fatalf("plan = %+v", plan)
	}
	plan[0].Title = "mutated"
	if p.Get()[0].Title != "A" {
		t.Fatal("Get must return a copy")
	}
}

func TestPlanStore_documentUpdatedFlag(t *testing.T) {
	p := NewPlanStore()
	if p.Document() != "" {
		t.Fatal("expected empty document")
	}
	// Initial install does not mark updated (create_plan path).
	p.SetDocument("draft v1")
	if p.Document() != "draft v1" {
		t.Fatalf("got %q", p.Document())
	}
	if p.ConsumeDocumentUpdated() {
		t.Fatal("initial set must not mark updated")
	}
	p.SetDocument("draft v1")
	if p.ConsumeDocumentUpdated() {
		t.Fatal("identical body should not mark updated")
	}
	p.SetDocument("draft v2")
	if !p.ConsumeDocumentUpdated() {
		t.Fatal("expected updated after change to existing draft")
	}
	if p.ConsumeDocumentUpdated() {
		t.Fatal("flag should clear after consume")
	}
}

func TestPlanStore_exportImportRoundTrip(t *testing.T) {
	p := NewPlanStore()
	p.Set([]Todo{
		{Title: "Task 1", Status: streaming.TodoStatusCompleted, Description: "done"},
		{Title: "Task 2", Status: streaming.TodoStatusInProgress, Description: "next"},
	})
	p.SetDocument("full plan")
	p.SetDocument("full plan v2") // mark updated

	state := map[string]any{}
	p.ExportInto(state)

	// Simulate checkpoint JSON round-trip.
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var reloaded map[string]any
	if err := json.Unmarshal(raw, &reloaded); err != nil {
		t.Fatal(err)
	}

	p2 := NewPlanStore()
	p2.LoadFromState(reloaded)
	plan := p2.Get()
	if len(plan) != 2 {
		t.Fatalf("plan len = %d, want 2", len(plan))
	}
	if plan[0].Title != "Task 1" || plan[0].Status != streaming.TodoStatusCompleted {
		t.Errorf("plan[0] = %+v", plan[0])
	}
	if plan[1].Title != "Task 2" || plan[1].Status != streaming.TodoStatusInProgress {
		t.Errorf("plan[1] = %+v", plan[1])
	}
	if p2.Document() != "full plan v2" {
		t.Fatalf("document = %q", p2.Document())
	}
	if !p2.ConsumeDocumentUpdated() {
		t.Fatal("expected documentUpdated restored from checkpoint")
	}
}

func TestEmitPlanUpdate_withChannel(t *testing.T) {
	ch := make(chan streaming.StreamEvent, 1)
	rt := NewRuntime(ch, nil, nil)
	rt.EnsureInitialized()
	rt.EmitPlanUpdate([]Todo{
		{Title: "Ship", Status: streaming.TodoStatusInProgress, Description: "d"},
	})
	select {
	case ev := <-ch:
		if ev.Type != streaming.StreamEventPlanUpdate {
			t.Fatalf("type = %v, want plan update", ev.Type)
		}
		if !strings.Contains(string(ev.Data), "Ship") {
			t.Fatalf("data = %s", ev.Data)
		}
	default:
		t.Fatal("expected plan update on output channel")
	}
	rt.SetOutputChannel(nil)
	rt.EmitPlanUpdate([]Todo{{Title: "Ship", Status: streaming.TodoStatusCompleted}})
}

func TestRuntime_reservedPlanKeysBlocked(t *testing.T) {
	rt := HarnessRuntime{}
	rt.EnsureInitialized()
	rt.StateSet("_plan", "should-not-store")
	if _, ok := rt.StateGet("_plan"); ok {
		t.Fatal("StateGet must hide reserved plan keys")
	}
	// Direct map poison is stripped from SnapshotState.
	rt.State["_plan"] = "poison"
	state, _, _ := rt.SnapshotState()
	if _, ok := state["_plan"]; ok {
		t.Fatal("SnapshotState must omit reserved plan keys")
	}
}

func TestStripPlanKeys(t *testing.T) {
	state := map[string]any{
		"_plan":                  []Todo{{Title: "x"}},
		"_plan_document":         "doc",
		"_plan_document_updated": true,
		"user_key":               1,
	}
	StripPlanKeys(state)
	if len(state) != 1 || state["user_key"] != 1 {
		t.Fatalf("state = %+v", state)
	}
}
