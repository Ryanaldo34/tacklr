package telemetry

import (
	"log/slog"

	"github.com/ryanaldo34/tacklr/streaming"
)

// StdioWatchDog implements tacklr.AgentWatchDog via streaming.Message (type alias).
type StdioWatchDog struct{}

// NewStdioWatchDog returns a watchdog that logs agent activity to slog.
func NewStdioWatchDog() *StdioWatchDog { return &StdioWatchDog{} }

func (s *StdioWatchDog) RecordThinking(msg *streaming.Message) error {
	slog.Debug("agent thinking", "content", msg.Content)
	return nil
}

func (s *StdioWatchDog) RecordOutput(msg *streaming.Message) error {
	if msg.Role == streaming.RoleAssistant {
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

func (s *StdioWatchDog) RecordToolCalls(msg *streaming.Message) error {
	names := make([]string, len(msg.ToolCalls))
	for i, tc := range msg.ToolCalls {
		names[i] = tc.Name
	}
	slog.Debug("tool calls", "tools", names)
	return nil
}

func (s *StdioWatchDog) RecordToolResult(msg *streaming.Message) error {
	slog.Info("tool result", "tool_call_id", msg.ToolCallID, "content", msg.Content)
	return nil
}
