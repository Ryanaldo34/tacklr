package tacklr

import (
	"fmt"

	"github.com/ryanaldo34/tacklr/message"
)

// ValidateMessages validates structural invariants shared by live context and
// durable checkpoints. Open assistant tool calls are valid while interrupted;
// pairing is repaired by the harness before the next model invocation.
func ValidateMessages(messages []*message.Message) error {
	for i, m := range messages {
		if m == nil {
			return fmt.Errorf("message %d is nil", i)
		}
		switch m.Role {
		case message.RoleUser, message.RoleAssistant, message.RoleReasoning, message.RoleSystem, message.RoleDeveloper:
			if m.ToolCallID != "" {
				return fmt.Errorf("message %d: role %q cannot have tool_call_id", i, m.Role)
			}
		case message.RoleTool:
			if m.ToolCallID == "" {
				return fmt.Errorf("message %d: tool message requires tool_call_id", i)
			}
		default:
			return fmt.Errorf("message %d: unsupported role %q", i, m.Role)
		}
	}
	return nil
}
