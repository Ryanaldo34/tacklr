package tacklr

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
)

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
