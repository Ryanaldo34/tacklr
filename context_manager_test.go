package tacklr

import (
	"strings"
	"testing"
)

func TestModelContextManager_Add_and_Messages(t *testing.T) {
	mgr := NewModelContextManager()
	if len(mgr.Messages()) != 0 {
		t.Fatal("expected empty")
	}
	mgr.Add(&Message{Role: RoleUser, Content: "hi"})
	mgr.Add(&Message{Role: RoleAssistant, Content: "yo"})
	if len(mgr.Messages()) != 2 {
		t.Fatalf("len = %d", len(mgr.Messages()))
	}
}

func TestModelContextManager_Snapshot_isCopy(t *testing.T) {
	mgr := NewModelContextManager()
	mgr.Restore([]*Message{{Role: RoleUser, Content: "a"}})
	snap := mgr.Snapshot()
	snap[0] = &Message{Role: RoleUser, Content: "mutated"}
	if mgr.Messages()[0].Content != "a" {
		t.Fatal("snapshot must not share slice header")
	}
}

func TestModelContextManager_InstallPlanDocument(t *testing.T) {
	mgr := NewModelContextManager()
	mgr.Restore([]*Message{
		{Role: RoleUser, Content: "task"},
		{Role: RoleAssistant, Content: "planning noise"},
	})
	if err := mgr.InstallPlanDocument("CoS: ship"); err != nil {
		t.Fatal(err)
	}
	msgs := mgr.Messages()
	if len(msgs) != 2 || msgs[0].Content != "task" || !isPlanDocument(msgs[1]) {
		t.Fatalf("%+v", msgs)
	}
	if rawPlanFromDocumentMessage(msgs[1]) != "CoS: ship" {
		t.Fatalf("plan = %q", msgs[1].Content)
	}
}

func TestModelContextManager_InstallPlanDocument_errors(t *testing.T) {
	mgr := NewModelContextManager()
	if err := mgr.InstallPlanDocument("x"); err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("err = %v", err)
	}
	mgr.Restore([]*Message{{Role: RoleUser, Content: "u"}})
	if err := mgr.InstallPlanDocument(""); err == nil || !strings.Contains(err.Error(), "no plan document") {
		t.Fatalf("err = %v", err)
	}
}

func TestProtectedPrefixLen(t *testing.T) {
	if protectedPrefixLen(nil) != 0 {
		t.Fatal()
	}
	w := []*Message{{Role: RoleUser, Content: "u"}, {Role: RoleAssistant, Content: "a"}}
	if protectedPrefixLen(w) != 1 {
		t.Fatalf("got %d", protectedPrefixLen(w))
	}
	w[1] = buildPlanDocumentMessage("p")
	if protectedPrefixLen(w) != 2 {
		t.Fatalf("got %d", protectedPrefixLen(w))
	}
}
