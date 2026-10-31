package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/vfs"
)

type failMCP struct{}

func (failMCP) ResolveMCP(context.Context, string) (mcp.Credentials, error) {
	return mcp.Credentials{}, errors.New("expired")
}

type zeroWindowStrategy struct{ scriptedModel }

func (*zeroWindowStrategy) MaxContextWindow() (int, error) { return 0, nil }

type errWindowStrategy struct{ scriptedModel }

func (*errWindowStrategy) MaxContextWindow() (int, error) { return 0, errors.New("no window") }

// TestNewTurnManager_constructFailClosed is the fail-closed construct surface:
// broken /skills pack, missing session, and resume without a model.
func TestNewTurnManager_constructFailClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, t.Name(), vfs.At("skills", vfs.Local(root)))
	_, err := NewTurnManager(context.Background(), AgentOptions{
		MaxWindowSize: 8192,
		Model:         &scriptedModel{},
		mountSession:  ms,
	})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}
}

func TestNewTurnManager_configurationInvariants(t *testing.T) {
	// Arrange
	validModel := &scriptedModel{}
	ms := mustMountTree(t, "overflow-config", vfs.At("work", vfs.Local(t.TempDir())), vfs.At("ro", vfs.Local(t.TempDir())).ReadOnly())
	cases := []struct {
		name string
		opts AgentOptions
	}{
		{
			name: "nil model",
			opts: AgentOptions{},
		},
		{
			name: "model without context limit",
			opts: AgentOptions{Model: &zeroWindowStrategy{}},
		},
		{
			name: "model window lookup fails",
			opts: AgentOptions{Model: &errWindowStrategy{}},
		},
		{
			name: "an allowed tool that is not registered",
			opts: AgentOptions{Model: validModel, AllowTools: []string{"nope"}},
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
			name: "overflow path without a workspace",
			opts: AgentOptions{Model: validModel, OverflowPath: "/workspace/work/overflow"},
		},
		{
			name: "overflow path outside the mounts",
			opts: AgentOptions{Model: validModel, mountSession: ms, OverflowPath: "/elsewhere"},
		},
		{
			name: "overflow path on a read-only mount",
			opts: AgentOptions{Model: validModel, mountSession: ms, OverflowPath: "/workspace/ro/overflow"},
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
	}

	// Act and assert
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTurnManager(t.Context(), tc.opts); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}

	h, err := NewTurnManager(t.Context(), AgentOptions{Model: validModel})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if h.maxWindowSize != 8192 {
		t.Fatalf("resolved max window = %d", h.maxWindowSize)
	}
}

func TestRestoreCheckpoint_rejectsACorruptOrOtherVersionCheckpoint(t *testing.T) {
	h := mustNewTurnManager(t, AgentOptions{Model: &scriptedModel{}})
	for name, raw := range map[string]string{
		"corrupt":                      `{"version":4,"window":"x"}`,
		"other version":                `{"version":3,"window":[]}`,
		"an invalid window":            `{"version":4,"window":[{"role":"tool","content":"x"}]}`,
		"a park that is not an object": `{"version":4,"window":[],"round":{"reply":[],"calls":[{"call":{},"park":"x"}]}}`,
		"a park of an unknown type":    `{"version":4,"window":[],"round":{"reply":[],"calls":[{"call":{},"park":{"type":"nope","data":{}}}]}}`,
		"a park whose data is bad":     `{"version":4,"window":[],"round":{"reply":[],"calls":[{"call":{},"park":{"type":"tool_permission","data":"x"}}]}}`,
	} {
		var cp SessionCheckpoint
		if err := json.Unmarshal([]byte(raw), &cp); err != nil {
			t.Fatal(err)
		}
		if err := h.RestoreCheckpoint(cp); err == nil {
			t.Errorf("%s checkpoint was accepted", name)
		}
	}
}

// skillPack is a local skills directory with one skill.
func skillPack(t *testing.T, name, description, instructions string) skills.Loader {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: " + description + "\n---\n\n" + instructions + "\n"
	if err := os.WriteFile(filepath.Join(root, name, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return skills.Loader{Root: root}
}

func TestTurnManager_checkpointAfterRun(t *testing.T) {
	mock := &scriptedModel{SupportsMIMEFn: IsTextMIME}
	h, err := NewTurnManager(t.Context(), AgentOptions{
		sessionID:     "sess",
		Model:         mock,
		MaxWindowSize: 8192, SystemPrompt: "be brief",
		SkillsLoader: skillPack(t, "pack", "research", "read first"),
		Specialists: []*Specialist{{
			Name:        "researcher",
			Description: "Looks things up.",
			Model:       mock,
		}},
		MCPConfigs: []mcp.MCPConfig{{
			Name: "remote", Type: mcp.TransportHTTP, URL: "https://example.test", CredentialRef: "vault://remote",
		}},
		MCPCredentialResolver: failMCP{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	out := make(chan StreamEvent, 16)
	go func() {
		for range out {
		}
	}()
	if err := h.absorbUser(t.Context(), &Message{Role: RoleUser, Content: "hello"}, out); err != nil {
		t.Fatal(err)
	}
	img := &Message{Role: RoleUser, ContentParts: []ContentPart{{
		Type:     ContentTypeInputImage,
		FileData: &FileData{MIMEType: "image/png"},
	}}}
	if err := h.absorbUser(t.Context(), img, out); err == nil {
		t.Fatal("text-only model must reject image input")
	}
	write := NewTool(ToolConfig{
		Name: "mutate", Access: ToolWriteAccess,
		Handler: func(context.Context) (string, error) { return "ok", nil },
	})
	_, err = h.planningWriteLock(t.Context(), ToolInvocation{Tool: write}, func(context.Context, ToolInvocation) (ToolOutcome, error) {
		return ToolOutcome{Output: "ran"}, nil
	})
	if !errors.Is(err, ErrToolPermissionDenied) {
		t.Fatalf("write locked until create_plan: %v", err)
	}
	if sk := h.findTool("read_skill", ""); sk != nil {
		if _, err := sk.invoke(t.Context(), `{"name":"missing"}`, turnRuntime(h)); err == nil {
			t.Fatal("unknown skill")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.absorbUser(ctx, &Message{Role: RoleUser, Content: "x"}, out); err == nil {
		t.Fatal("cancelled absorb")
	}

	if _, err := h.runTools(t.Context(), 1, map[string][]byte{"missing": []byte(`{}`)}, nil, out); !errors.Is(err, ErrInterruptNotFound) {
		t.Fatalf("resume of an unknown interrupt = %v", err)
	}
	parkID := "ask1"
	h.session.round = newOpenRound(nil, []ToolCall{{ID: parkID, CallID: parkID, Name: "ask_user_choice"}})
	_ = h.session.Park(parkID, &UserSelectionInterrupt{Options: []UserChoice{{Title: "a"}, {Title: "b"}}})
	if _, err := h.runTools(t.Context(), 2, map[string][]byte{parkID: []byte(`{"selectionIdx":9}`)}, nil, out); err == nil {
		t.Fatal("invalid selection")
	}
	h.session.round = nil

	mock.InvokeFn = func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "hi", IsComplete: true}
	}
	if _, err := h.runInference(t.Context(), out); err != nil {
		t.Fatal(err)
	}
	// The window ends on a tool round: the next request answers tools.
	call := ToolCall{ID: "c1", CallID: "c1", Name: "x", Arguments: "{}"}
	result, _ := h.toolResultMessage(call, "ok", "success")
	h.context.Add(&Message{Role: RoleAssistant, ToolCalls: []ToolCall{call}}, result)
	mock.InvokeErr = errors.New("upstream")
	if _, err := h.runInference(t.Context(), out); !errors.Is(err, ErrModelAfterTools) {
		t.Fatalf("want ErrModelAfterTools, got %v", err)
	}
	mock.InvokeErr = nil
	mock.InvokeFn = func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventError, Error: errors.New("provider"), IsComplete: true}
	}
	if _, err := h.runInference(t.Context(), out); !errors.Is(err, ErrModelAfterTools) {
		t.Fatalf("want an after-tools stream error, got %v", err)
	}
	prompt := strings.Join(mock.SystemPrompts, "\n")
	if !strings.Contains(prompt, "pack") {
		t.Fatal("system prompt missing skill catalog")
	}
	if !strings.Contains(prompt, "researcher") {
		t.Fatal("system prompt missing specialists")
	}

	cp, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored := mustNewTurnManager(t, AgentOptions{Model: &scriptedModel{}})
	if err := restored.RestoreCheckpoint(cp); err != nil || len(restored.context.Messages()) != len(h.context.Messages()) {
		t.Fatalf("restore = %v with %d messages, want the window back", err, len(restored.context.Messages()))
	}
}
