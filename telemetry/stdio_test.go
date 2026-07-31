package telemetry

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func TestNew_returnsStdioWatchDog(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

func TestAllMethods_returnNil(t *testing.T) {
	s := New()

	cases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "RecordThinking",
			fn: func() error {
				return s.RecordThinking(&tacklr.Message{Role: tacklr.RoleAssistant, Content: "thinking"})
			},
		},
		{
			name: "RecordOutput_assistant",
			fn: func() error {
				return s.RecordOutput(&tacklr.Message{Role: tacklr.RoleAssistant, Content: "output", ToolCalls: []tacklr.ToolCall{}})
			},
		},
		{
			name: "RecordOutput_user_guard",
			fn:   func() error { return s.RecordOutput(&tacklr.Message{Role: tacklr.RoleUser, Content: "user msg"}) },
		},
		{
			name: "RecordError",
			fn:   func() error { return s.RecordError(errors.New("test error")) },
		},
		{
			name: "RecordTokens",
			fn:   func() error { return s.RecordTokens(100, 50) },
		},
		{
			name: "RecordToolCalls",
			fn: func() error {
				return s.RecordToolCalls(&tacklr.Message{Role: tacklr.RoleAssistant, ToolCalls: []tacklr.ToolCall{{Name: "tool_a"}, {Name: "tool_b"}}})
			},
		},
		{
			name: "RecordToolCalls_nil",
			fn:   func() error { return s.RecordToolCalls(&tacklr.Message{Role: tacklr.RoleAssistant, ToolCalls: nil}) },
		},
		{
			name: "RecordToolResult",
			fn: func() error {
				return s.RecordToolResult(&tacklr.Message{Role: tacklr.RoleTool, ToolCallID: "call_1", Content: "result"})
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s panicked: %v", c.name, r)
				}
			}()
			if err := c.fn(); err != nil {
				t.Fatalf("%s returned error: %v", c.name, err)
			}
		})
	}
}

func TestRecordToolCalls_nilToolCallsDoesNotPanic(t *testing.T) {
	s := New()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RecordToolCalls panicked with nil ToolCalls: %v", r)
		}
	}()
	if err := s.RecordToolCalls(&tacklr.Message{Role: tacklr.RoleAssistant}); err != nil {
		t.Fatalf("RecordToolCalls returned error: %v", err)
	}
}

func TestRecordOutput_roleGuard(t *testing.T) {
	s := New()

	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil))) // restore

	// RoleUser should NOT log "agent output".
	buf.Reset()
	if err := s.RecordOutput(&tacklr.Message{Role: tacklr.RoleUser, Content: "user msg"}); err != nil {
		t.Fatalf("RecordOutput(user) returned error: %v", err)
	}
	if bytes.Contains(buf.Bytes(), []byte("agent output")) {
		t.Fatalf("expected RoleUser to NOT log 'agent output', got: %s", buf.String())
	}

	// RoleAssistant SHOULD log "agent output".
	buf.Reset()
	if err := s.RecordOutput(&tacklr.Message{Role: tacklr.RoleAssistant, Content: "assistant msg"}); err != nil {
		t.Fatalf("RecordOutput(assistant) returned error: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("agent output")) {
		t.Fatalf("expected RoleAssistant to log 'agent output', got: %s", buf.String())
	}
}
