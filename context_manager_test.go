package tacklr

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/message"
)

func TestModelContextManager_enforcesMessageInvariantsAtMutation(t *testing.T) {
	// Arrange
	manager := newModelContextManager()
	assertPanics := func(name string, fn func()) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}

	// Act and assert
	assertPanics("nil add", func() { manager.Add(nil) })
	assertPanics("nil restore", func() { manager.Restore([]*message.Message{nil}) })
	assertPanics("invalid replace", func() {
		manager.Replace([]*message.Message{{Role: message.RoleTool, Content: "missing id"}})
	})

	manager.Add(&message.Message{Role: message.RoleUser, Content: "valid"})
	if got := manager.Messages(); len(got) != 1 || got[0].Content != "valid" {
		t.Fatalf("messages = %#v", got)
	}
}

func TestModelContextManager_installPlanDocument(t *testing.T) {
	manager := newModelContextManager()

	if err := manager.InstallPlanDocument(""); err == nil || !strings.Contains(err.Error(), "no plan document") {
		t.Fatalf("empty plan error = %v", err)
	}
	if err := manager.InstallPlanDocument("PROJECT PLAN"); err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("empty window error = %v", err)
	}

	manager.Restore([]*message.Message{{Role: message.RoleUser, Content: "goal"}})
	if err := manager.InstallPlanDocument("Ship the release"); err != nil {
		t.Fatal(err)
	}
	window := manager.Messages()
	if len(window) != 2 || !IsPlanDocument(window[1]) || !strings.HasSuffix(window[1].Content, "Ship the release") {
		t.Fatalf("window = %+v", window)
	}
}
