package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

type stubSkills struct{}

func (stubSkills) Load(context.Context) ([]skills.Skill, error) {
	return []skills.Skill{{Name: "pack", Description: "research", Instructions: "read first"}}, nil
}

type failMCP struct{}

func (failMCP) ResolveMCP(context.Context, string) (mcp.Credentials, error) {
	return mcp.Credentials{}, errors.New("expired")
}

type zeroWindowStrategy struct{ mockStrategy }

func (*zeroWindowStrategy) MaxContextWindow() (int, error) { return 0, nil }

type errWindowStrategy struct{ mockStrategy }

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
		Config:        Config{MaxWindowSize: 8192},
		Model:         &mockStrategy{},
		SkillsSession: ms,
	})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}
}

func TestNewTurnManager_skillsIsolatedFromWorkspace(t *testing.T) {
	ctx := t.Context()
	pack := t.TempDir()
	d := filepath.Join(pack, "research")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: research\ndescription: Research carefully\n---\n\nAlways verify claims.\n"
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	ms := mustMountTree(t, t.Name(), vfs.At("work", vfs.Local(t.TempDir())))
	skillsMS := mustMountTree(t, t.Name()+"-skills", vfs.At("skills", vfs.Local(pack)))

	h := mustNewTurnManager(t, AgentOptions{
		Config:        Config{MaxWindowSize: 8192},
		Model:         &mockStrategy{},
		MountSession:  ms,
		SkillsSession: skillsMS,
	})
	t.Cleanup(h.Close)

	skill := h.findTool("read_skill", "")
	if skill == nil {
		t.Fatal("read_skill missing")
	}
	res, err := skill.invoke(ctx, `{"name":"research"}`, turnRuntime(h))
	if err != nil || !strings.Contains(res.output, "Always verify claims") {
		t.Fatalf("read_skill: %v %s", err, res.output)
	}

	read := h.findTool("read", "")
	if read == nil {
		t.Fatal("read missing")
	}
	_, err = read.invoke(ctx, `{"path":"/workspace/skills/research/SKILL.md"}`, turnRuntime(h))
	if !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("workspace read of skill path: %v", err)
	}
}

func TestNewTurnManager_configurationInvariants(t *testing.T) {
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
			name: "model window lookup fails",
			opts: AgentOptions{Model: &errWindowStrategy{}},
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

func TestRestoreCheckpoint_rejectsCorruptModules(t *testing.T) {
	sm := session.NewSessionManager()
	valid, err := session.CaptureCheckpoint(
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
	h := mustNewTurnManager(t, AgentOptions{Model: &mockStrategy{}})
	if err := h.RestoreCheckpoint(checkpoint); err == nil {
		t.Fatal("corrupt checkpoint was accepted")
	}
}

func TestTurnManager_checkpointAfterRun(t *testing.T) {
	wd := &recordingWatchdog{}
	mock := &mockStrategy{}
	h, err := NewTurnManager(t.Context(), AgentOptions{
		SessionID:    "sess",
		Model:        mock,
		WatchDog:     wd,
		Config:       Config{MaxWindowSize: 8192, SystemPrompt: "be brief"},
		SkillsLoader: stubSkills{},
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
	h.injectBuiltinTools() // already injected at construct
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
	_, err = h.planningWriteLock(t.Context(), ToolInvocation{Tool: write}, func(context.Context, ToolInvocation) (string, error) {
		return "ran", nil
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
	if err := h.addToContext(ctx, &Message{Role: RoleUser, Content: "x"}, out); err == nil {
		t.Fatal("cancelled absorb")
	}

	pendingID := "call_pending"
	h.pendingToolCalls[pendingID] = stores.PendingToolCall{
		ToolCall: &ToolCall{ID: pendingID, CallID: pendingID, Name: "x", Arguments: "{}"},
	}
	h.context.Add(&Message{Role: RoleAssistant, ToolCalls: []ToolCall{
		{ID: pendingID, CallID: pendingID, Name: "x", Arguments: "{}"},
		{ID: "fc_open", CallID: "call_open", Name: "y", Arguments: "{}"},
	}})
	h.pairOpenToolCalls("unpaired tool call")
	openPaired := false
	for _, m := range h.context.Messages() {
		if m != nil && m.Role == RoleTool && m.ToolCallID == "call_open" {
			openPaired = true
		}
		if m != nil && m.Role == RoleTool && m.ToolCallID == pendingID {
			t.Fatal("pending call must not be paired")
		}
	}
	if !openPaired {
		t.Fatal("unpaired tool call must get a result before the next model turn")
	}
	if err := h.absorbUser(t.Context(), &Message{Role: RoleUser, Content: "steer"}, out); err != nil {
		t.Fatal(err)
	}
	if h.hasOpenToolWork() {
		t.Fatal("new prompt must clear leftover tool work")
	}

	if err := h.applyResume(map[string][]byte{"missing": []byte(`{}`)}); err == nil {
		t.Fatal("unknown interrupt id")
	}
	parkID := "ask1"
	_ = h.session.Park(parkID, &interrupt.UserSelectionInterrupt{
		Options: []interrupt.UserChoice{{Title: "a"}, {Title: "b"}},
	})
	h.pendingToolCalls[parkID] = stores.PendingToolCall{
		ToolCall: &ToolCall{ID: parkID, CallID: parkID, Name: "ask_user_choice"}, InterruptActive: true,
	}
	if err := h.applyResume(map[string][]byte{parkID: []byte(`{"selectionIdx":9}`)}); err == nil {
		t.Fatal("invalid selection")
	}

	h.toolResultMessage(ToolCall{ID: "wd", CallID: "wd", Name: "x"}, "ok", "success")
	mock.invokeFn = func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "hi", IsComplete: true}
	}
	if _, err := h.runInference(t.Context(), &drive.TurnState{}, out); err != nil {
		t.Fatal(err)
	}
	if len(wd.toolResults) == 0 || len(wd.outputs) == 0 {
		t.Fatalf("watchdog tool=%d out=%d", len(wd.toolResults), len(wd.outputs))
	}
	mock.invokeErr = errors.New("upstream")
	if _, err := h.runInference(t.Context(), &drive.TurnState{HadToolRound: true}, out); err == nil || !errors.Is(err, ErrModelAfterTools) {
		t.Fatalf("want ErrModelAfterTools, got %v", err)
	}
	mock.invokeErr = nil
	mock.invokeFn = func(_ context.Context, _ []*Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: StreamEventError, Error: errors.New("provider"), IsComplete: true}
	}
	if _, err := h.runInference(t.Context(), &drive.TurnState{HadToolRound: true}, out); err == nil {
		t.Fatal("want after-tools stream error")
	}
	prompt := strings.Join(mock.systemPrompts, "\n")
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
	if len(cp.ContextWindow) == 0 {
		t.Fatal("absorbUser must leave a conversation on Checkpoint")
	}
}
