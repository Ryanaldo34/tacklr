package streaming

import "fmt"

// ValidateMessages validates structural invariants shared by live context and
// durable checkpoints. Open assistant tool calls are valid while interrupted;
// pairing is repaired by the harness before the next model invocation.
func ValidateMessages(messages []*Message) error {
	for i, message := range messages {
		if message == nil {
			return fmt.Errorf("message %d is nil", i)
		}
		switch message.Role {
		case RoleUser, RoleAssistant, RoleReasoning, RoleSystem, RoleDeveloper:
			if message.ToolCallID != "" {
				return fmt.Errorf("message %d: role %q cannot have tool_call_id", i, message.Role)
			}
		case RoleTool:
			if message.ToolCallID == "" {
				return fmt.Errorf("message %d: tool message requires tool_call_id", i)
			}
		default:
			return fmt.Errorf("message %d: unsupported role %q", i, message.Role)
		}
	}
	return nil
}
