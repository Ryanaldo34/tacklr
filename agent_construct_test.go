package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

type zeroWindowStrategy struct{ mockStrategy }

func (*zeroWindowStrategy) MaxContextWindow() (int, error) { return 0, nil }

// TestNewAgent_constructFailClosed is the fail-closed construct surface:
// broken /skills pack, missing session, and resume without a model.
func TestNewAgent_constructFailClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "skills", Base: root, Skills: "."}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession(t.Name(), reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.AttachSkills(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	_, err = NewAgent(context.Background(), AgentOptions{
		Config:       Config{MaxWindowSize: 8192},
		Model:        &mockStrategy{},
		MountSession: ms,
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
		{
			name: "duplicate MCP server",
			opts: AgentOptions{
				Model: validModel,
				MCPConfigs: []mcp.MCPConfig{
					{Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test/a"},
					{Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test/b"},
				},
			},
		},
		{
			name: "credential ref without resolver",
			opts: AgentOptions{
				Model: validModel,
				MCPConfigs: []mcp.MCPConfig{
					{Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test", CredentialRef: "vault://remote"},
				},
			},
		},
		{
			name: "negative max turn requests",
			opts: AgentOptions{Model: validModel, Config: Config{MaxTurnRequests: -1}},
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
		Store:     stores.FaultyStore{Inner: stores.NewInMemoryStore(), SaveErr: errors.New("checkpoint disk full")},
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

func TestNewAgentFromSession_rejectsCorruptCheckpointModules(t *testing.T) {
	// Arrange
	store := testStore(t)
	sm := session.NewSessionManager()
	valid, err := session.NewCheckpointer().Capture(
		[]*streaming.Message{{Role: streaming.RoleUser, Content: "go"}},
		sm,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	state := wire["state"].(map[string]any)
	modules := state["modules"].(map[string]any)
	modules["plan"] = `{"todos":`
	raw, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint stores.SessionCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(t.Context(), "sess-corrupt", checkpoint); err != nil {
		t.Fatal(err)
	}

	// Act
	_, err = NewAgentFromSession(t.Context(), "sess-corrupt", AgentOptions{
		Model: &mockStrategy{},
		Store: store,
	})

	// Assert
	if err == nil {
		t.Fatal("corrupt checkpoint was accepted")
	}
}

func TestNewAgent_runCheckpointsToStore(t *testing.T) {
	store := testStore(t)
	h, err := NewAgent(t.Context(), AgentOptions{
		SessionID: "path-a",
		Model:     &mockStrategy{},
		Store:     store,
		Config:    Config{MaxWindowSize: 8192},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	events, err := h.Run(t.Context(), "hello path a")
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	cp, err := store.LoadSession(t.Context(), "path-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.ContextWindow) == 0 {
		t.Fatal("Path A Run must checkpoint the conversation on Store")
	}
}
