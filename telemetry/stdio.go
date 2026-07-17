package telemetry

import (
	"log/slog"

	"github.com/ryanaldo34/tacklr"
)

type StdioWatchDog struct{}

func New() *StdioWatchDog { return &StdioWatchDog{} }

func (s *StdioWatchDog) RecordThinking(msg *tacklr.Message) error {
	slog.Debug("agent thinking", "content", msg.Content)
	return nil
}

func (s *StdioWatchDog) RecordOutput(msg *tacklr.Message) error {
	if msg.Role == tacklr.RoleAssistant {
		slog.Info("agent output", "content", msg.Content, "tool_calls", len(msg.ToolCalls))
	}
	return nil
}

func (s *StdioWatchDog) RecordError(err error) error {
	slog.Error("agent error", "error", err)
	return nil
}

func (s *StdioWatchDog) RecordTokens(input, output int) error {
	slog.Debug("token usage", "input", input, "output", output)
	return nil
}

func (s *StdioWatchDog) RecordToolCalls(msg *tacklr.Message) error {
	names := make([]string, len(msg.ToolCalls))
	for i, tc := range msg.ToolCalls {
		names[i] = tc.Name
	}
	slog.Debug("tool calls", "tools", names)
	return nil
}

func (s *StdioWatchDog) RecordToolResult(msg *tacklr.Message) error {
	slog.Info("tool result", "tool_call_id", msg.ToolCallID, "content", msg.Content)
	return nil
}
