package tacklr

import "testing"

func TestModelContextManager_enforcesMessageInvariantsAtMutation(t *testing.T) {
	// Arrange
	manager := NewModelContextManager()
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
	assertPanics("nil restore", func() { manager.Restore([]*Message{nil}) })
	assertPanics("invalid replace", func() {
		manager.Replace([]*Message{{Role: RoleTool, Content: "missing id"}})
	})

	manager.Add(&Message{Role: RoleUser, Content: "valid"})
	if got := manager.Messages(); len(got) != 1 || got[0].Content != "valid" {
		t.Fatalf("messages = %#v", got)
	}
}

func TestContextPolicy_validateRejectsInvalidRatios(t *testing.T) {
	// Act
	compressErr := ContextPolicy{CompressFraction: 2}.Validate()
	negativeCompressErr := ContextPolicy{CompressFraction: -1}.Validate()
	negativePressureErr := ContextPolicy{PressureRatio: -1}.Validate()

	// Assert
	if compressErr == nil || negativeCompressErr == nil || negativePressureErr == nil {
		t.Fatalf("errors = %v %v %v", compressErr, negativeCompressErr, negativePressureErr)
	}
	if err := DefaultContextPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
}
