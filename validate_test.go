package tacklr

import (
	"testing"

	"github.com/ryanaldo34/tacklr/message"
)

func TestValidateMessages_rejectsUnsupportedRolesAndInvalidToolFields(t *testing.T) {
	// Act
	userToolErr := ValidateMessages([]*message.Message{{Role: message.RoleUser, ToolCallID: "call"}})
	unsupportedErr := ValidateMessages([]*message.Message{{Role: "invalid"}})

	// Assert
	if userToolErr == nil {
		t.Fatal("user message with tool_call_id was accepted")
	}
	if unsupportedErr == nil {
		t.Fatal("unsupported role was accepted")
	}
}
