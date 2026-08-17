package streaming_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestValidateMessages_rejectsUnsupportedRolesAndInvalidToolFields(t *testing.T) {
	// Act
	userToolErr := streaming.ValidateMessages([]*streaming.Message{{Role: streaming.RoleUser, ToolCallID: "call"}})
	unsupportedErr := streaming.ValidateMessages([]*streaming.Message{{Role: "invalid"}})

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
	valid := []*streaming.Message{
		{Role: streaming.RoleUser, Content: "run"},
		{Role: streaming.RoleAssistant, ToolCalls: []streaming.ToolCall{{ID: "call", Name: "tool"}}},
	}

	// Act
	validErr := streaming.ValidateMessages(valid)
	nilErr := streaming.ValidateMessages([]*streaming.Message{nil})
	toolErr := streaming.ValidateMessages([]*streaming.Message{{Role: streaming.RoleTool}})

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
