package tacklr

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
)

type zeroWindowStrategy struct{ mockStrategy }

func (*zeroWindowStrategy) MaxContextWindow() (int, error) { return 0, nil }

// TestNewAgent_constructFailClosed is the fail-closed construct surface:
// missing skill dir, missing session, and resume without a model.
func TestNewAgent_constructFailClosed(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, SkillDirectories: []string{missing}},
		Model:  &mockStrategy{},
	})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}

	_, err = NewAgentFromSession(context.Background(), "no-such-session", AgentOptions{
		Model: &mockStrategy{},
		Store: testStore(t),
	})
	if err == nil || !strings.Contains(err.Error(), "no-such-session") {
		t.Fatalf("want missing session error, got %v", err)
	}

	store := testStore(t)
	if err := store.SaveSession(context.Background(), "sess-nomodel", stores.SessionCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	_, err = NewAgentFromSession(context.Background(), "sess-nomodel", AgentOptions{Store: store})
	if err == nil || !strings.Contains(err.Error(), "Model is required") {
		t.Fatalf("want model required, got %v", err)
	}
}

func TestNewAgent_configurationInvariants(t *testing.T) {
	// Arrange
	validModel := &mockStrategy{}
	cases := []struct {
		name string
		opts AgentOptions
	}{
		{
			name: "model without context limit",
			opts: AgentOptions{Model: &zeroWindowStrategy{}},
		},
		{
			name: "negative max window",
			opts: AgentOptions{Model: validModel, Config: Config{MaxWindowSize: -1}},
		},
		{
			name: "invalid pressure ratio",
			opts: AgentOptions{Model: validModel, ContextPolicy: ContextPolicy{PressureRatio: 2}},
		},
		{
			name: "nil tool",
			opts: AgentOptions{Model: validModel, Tools: []*Tool{nil}},
		},
		{
			name: "invalid MCP transport",
			opts: AgentOptions{
				Model:      validModel,
				MCPConfigs: []mcp.MCPConfig{{Name: "stdio"}},
			},
		},
	}

	// Act and assert
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewAgent(t.Context(), tc.opts); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}

	h, err := NewAgent(t.Context(), AgentOptions{Model: validModel})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if h.maxWindowSize != 8192 {
		t.Fatalf("resolved max window = %d", h.maxWindowSize)
	}
}

func TestAgentHarness_checkpointFailureIsHostVisibleAndClearsAfterRecovery(t *testing.T) {
	// Arrange
	h := mustNewAgent(t, AgentOptions{
		SessionID: "checkpoint-health",
		Model:     &mockStrategy{},
		Store:     failSaveStore{InMemoryStore: stores.NewInMemoryStore()},
	})

	// Act
	saveErr := h.persistSession(t.Context())
	reported := h.CheckpointError()
	h.store = stores.NewInMemoryStore()
	recoveryErr := h.persistSession(t.Context())

	// Assert
	if saveErr == nil || !errors.Is(reported, saveErr) {
		t.Fatalf("save error = %v reported = %v", saveErr, reported)
	}
	if recoveryErr != nil || h.CheckpointError() != nil {
		t.Fatalf("recovery error = %v reported = %v", recoveryErr, h.CheckpointError())
	}
}
