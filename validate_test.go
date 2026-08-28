package tacklr

import (
	"testing"
)

func TestValidateMessages_rejectsUnsupportedRolesAndInvalidToolFields(t *testing.T) {
	// Act
	userToolErr := ValidateMessages([]*Message{{Role: RoleUser, ToolCallID: "call"}})
	unsupportedErr := ValidateMessages([]*Message{{Role: "invalid"}})

	// Assert
	if userToolErr == nil {
		t.Fatal("user message with tool_call_id was accepted")
	}
	if unsupportedErr == nil {
		t.Fatal("unsupported role was accepted")
	}
}

func TestValidateMessages_acceptsInterruptedWindowAndRejectsCorruption(t *testing.T) {
	// Arrange
	valid := []*Message{
		{Role: RoleUser, Content: "run"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "call", Name: "tool"}}},
	}

	// Act
	validErr := ValidateMessages(valid)
	nilErr := ValidateMessages([]*Message{nil})
	toolErr := ValidateMessages([]*Message{{Role: RoleTool}})

	// Assert
	if validErr != nil {
		t.Fatal(validErr)
	}
	if nilErr == nil {
		t.Fatal("nil message was accepted")
	}
	if toolErr == nil {
		t.Fatal("tool message without tool_call_id was accepted")
	}
}
