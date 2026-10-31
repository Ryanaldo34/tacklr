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
