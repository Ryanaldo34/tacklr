package tacklr

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestNewAgent_missingSkillDirectory_constructError is the fail-closed
// construct outcome: a missing SkillDirectories path surfaces from NewAgent.
func TestNewAgent_missingSkillDirectory_constructError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 8192, SkillDirectories: []string{missing}},
		Model:  &mockStrategy{},
	})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}
}

// TestNewAgent_sessionIDAndMount_visibleOnHarness is the host-wiring outcome:
// SessionID and a borrowed MountSession are visible on the public harness.
func TestNewAgent_sessionIDAndMount_visibleOnHarness(t *testing.T) {
	ms := vfs.MustNewMountSession("sess-host", vfs.NewBackendRegistry())
	h := mustNewAgent(t, context.Background(), AgentOptions{
		Config:       Config{MaxWindowSize: 8192},
		SessionID:    "sess-host",
		Model:        &mockStrategy{},
		MountSession: ms,
	})
	if h.SessionID() != "sess-host" {
		t.Fatalf("SessionID = %q", h.SessionID())
	}
	if h.VFS() != ms {
		t.Fatal("VFS must be the host mount table")
	}
}

// TestNewAgentFromSession_missingSession_error is the resume outcome when the
// store has no checkpoint for the thread id.
func TestNewAgentFromSession_missingSession_error(t *testing.T) {
	_, err := NewAgentFromSession(context.Background(), "no-such-session", AgentOptions{
		Model: &mockStrategy{},
		Store: testStore(t),
	})
	if err == nil || !strings.Contains(err.Error(), "no-such-session") {
		t.Fatalf("want missing session error, got %v", err)
	}
}

// TestNewAgentFromSession_nilModel_error is the resume fail-closed outcome:
// a stored checkpoint still requires AgentOptions.Model.
func TestNewAgentFromSession_nilModel_error(t *testing.T) {
	store := testStore(t)
	if err := store.SaveSession(context.Background(), "sess-nomodel", stores.SessionCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	_, err := NewAgentFromSession(context.Background(), "sess-nomodel", AgentOptions{Store: store})
	if err == nil || !strings.Contains(err.Error(), "Model is required") {
		t.Fatalf("want model required, got %v", err)
	}
}
