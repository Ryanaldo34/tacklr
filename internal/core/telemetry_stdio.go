package core

import "log/slog"

type StdioWatchDog struct{}

func (s *StdioWatchDog) RecordThinking(msg *Message) error {
	slog.Info("agent thinking", "content", msg.Content)
	return nil
}

func (s *StdioWatchDog) RecordOutput(msg *Message) error {
	if msg.Role == RoleAssistant {
		slog.Info("agent output", "content", msg.Content, "tool_calls", len(msg.ToolCalls))
	}
	return nil
}

func (s *StdioWatchDog) RecordError(err error) error {
	slog.Error("agent error", "error", err)
	return nil
}

func (s *StdioWatchDog) RecordTokens(input, output int) error {
	slog.Info("token usage", "input", input, "output", output)
	return nil
}

func (s *StdioWatchDog) RecordToolCalls(msg *Message) error {
	names := make([]string, len(msg.ToolCalls))
	for i, tc := range msg.ToolCalls {
		names[i] = tc.Name
	}
	slog.Info("tool calls", "tools", names)
	return nil
}

func (s *StdioWatchDog) RecordToolResult(msg *Message) error {
	slog.Info("tool result", "tool_call_id", msg.ToolCallID, "content", msg.Content)
	return nil
}
