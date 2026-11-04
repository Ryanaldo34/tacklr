package tacklr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/skills"
)

type failMCP struct{}

func (failMCP) ResolveMCP(context.Context, string) (session.MCPCredentials, error) {
	return session.MCPCredentials{}, errors.New("expired")
}

type zeroWindowStrategy struct{ scriptedModel }

func (*zeroWindowStrategy) MaxContextWindow() (int, error) { return 0, nil }

type errWindowStrategy struct{ scriptedModel }

func (*errWindowStrategy) MaxContextWindow() (int, error) { return 0, errors.New("no window") }

// TestNewHarness_constructFailClosed is the fail-closed construct surface:
// broken /skills pack, missing session, and resume without a model.
func TestNewHarness_constructFailClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := NewHarness(context.Background(), AgentOptions{
		MaxWindowSize: 8192,
		Model:         &scriptedModel{},
		SkillsLoader:  skills.Loader{Root: root},
	})
	if err == nil || !strings.Contains(err.Error(), "initialize skills") {
		t.Fatalf("want skills construct error, got %v", err)
	}
}

func TestNewHarness_configurationInvariants(t *testing.T) {
	// Arrange
	validModel := &scriptedModel{}
	noNamespaceBrain, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
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
			name: "a nil nested specialist",
			opts: AgentOptions{Model: validModel, Specialists: []*Specialist{{Name: "lead", Model: validModel, Specialists: []*Specialist{nil}}}},
		},
		{
			name: "nil tool",
			opts: AgentOptions{Model: validModel, Tools: []*Tool{nil}},
		},
		{
			name: "a brain without a search namespace",
			opts: AgentOptions{Model: validModel, Brain: noNamespaceBrain},
		},
		{
			name: "invalid MCP transport",
			opts: AgentOptions{
				Model:      validModel,
				MCP:        mcp.Client{},
				MCPConfigs: []session.MCPServer{{Name: "stdio"}},
			},
		},
		{
			name: "MCP servers without a client",
			opts: AgentOptions{
				Model:      validModel,
				MCPConfigs: []session.MCPServer{{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test"}},
			},
		},
		{
			name: "duplicate MCP server",
			opts: AgentOptions{
				Model: validModel,
				MCP:   mcp.Client{},
				MCPConfigs: []session.MCPServer{
					{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test/a"},
					{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test/b"},
				},
			},
		},
		{
			name: "credential ref without resolver",
			opts: AgentOptions{
				Model: validModel,
				MCP:   mcp.Client{},
				MCPConfigs: []session.MCPServer{
					{Name: "remote", Type: session.MCPHTTP, URL: "https://example.test", CredentialRef: "vault://remote"},
				},
			},
		},
	}

	// Act and assert
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewHarness(t.Context(), tc.opts); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}

	h, err := NewHarness(t.Context(), AgentOptions{Model: validModel})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	if h.maxWindowSize != 8192 {
		t.Fatalf("resolved max window = %d", h.maxWindowSize)
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

func TestHarness_checkpointAfterRun(t *testing.T) {
	mock := &scriptedModel{SupportsMIMEFn: message.IsTextMIME}
	h, err := NewHarness(t.Context(), AgentOptions{
		sessionID:     "sess",
		Model:         mock,
		MaxWindowSize: 8192, SystemPrompt: "be brief",
		SkillsLoader: skillPack(t, "pack", "research", "read first"),
		Specialists: []*Specialist{{
			Name:        "researcher",
			Description: "Looks things up.",
			Model:       mock,
		}},
		MCP: mcp.Client{},
		MCPConfigs: []session.MCPServer{{
			Name: "remote", Type: session.MCPHTTP, URL: "https://example.test", CredentialRef: "vault://remote",
		}},
		MCPCredentialResolver: failMCP{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	out := make(chan session.Event, 16)
	go func() {
		for range out {
		}
	}()
	if err := h.Commit(t.Context(), CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "hello"}}}, out); err != nil {
		t.Fatal(err)
	}
	img := &message.Message{Role: message.RoleUser, ContentParts: []message.ContentPart{{
		Type:     message.ContentTypeInputImage,
		FileData: &message.FileData{MIMEType: "image/png"},
	}}}
	if err := h.Commit(t.Context(), CommitIn{Users: []*message.Message{img}}, out); err == nil {
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
	if err := h.Commit(ctx, CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "x"}}}, out); err == nil {
		t.Fatal("cancelled absorb")
	}

	ask := message.ToolCall{ID: "ask1", CallID: "ask1", Name: "ask_user_choice", Arguments: `{"question":"which?","choices":[{"title":"a"},{"title":"b"}]}`}
	parked, err := h.RunTool(t.Context(), ToolRun{Call: ask}, out)
	if err != nil || parked.Park == nil {
		t.Fatalf("ask = %+v %v, want a park", parked, err)
	}
	if _, err := h.RunTool(t.Context(), ToolRun{Call: ask, Prior: &parked, Answer: []byte(`{"selectionIdx":9}`)}, out); err == nil {
		t.Fatal("invalid selection")
	}

	mock.InvokeFn = func(_ context.Context, _ []*message.Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: session.EventMessage, Content: "hi", IsComplete: true}
	}
	if _, err := h.Infer(t.Context(), out); err != nil {
		t.Fatal(err)
	}
	// The window ends on a tool round: the next request answers tools.
	call := message.ToolCall{ID: "c1", CallID: "c1", Name: "x", Arguments: "{}"}
	result, _ := h.toolResultMessage(call, "ok", "success")
	h.context.Add(&message.Message{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{call}}, result)
	mock.InvokeErr = errors.New("upstream")
	if _, err := h.Infer(t.Context(), out); !errors.Is(err, ErrModelAfterTools) {
		t.Fatalf("want ErrModelAfterTools, got %v", err)
	}
	mock.InvokeErr = nil
	mock.InvokeFn = func(_ context.Context, _ []*message.Message, _ []*Tool, ch chan<- LLMResponseChunk) {
		ch <- LLMResponseChunk{Type: session.EventError, Error: errors.New("provider"), IsComplete: true}
	}
	if _, err := h.Infer(t.Context(), out); !errors.Is(err, ErrModelAfterTools) {
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
	restored := mustNewHarness(t, AgentOptions{Model: &scriptedModel{}})
	if err := restored.RestoreCheckpoint(cp); err != nil || len(restored.context.Messages()) != len(h.context.Messages()) {
		t.Fatalf("restore = %v with %d messages, want the window back", err, len(restored.context.Messages()))
	}
}

func TestHarness_readSkillReturnsTheBodyOfASkill(t *testing.T) {
	h := mustNewHarness(t, AgentOptions{MaxWindowSize: 8192, Model: &scriptedModel{}, SkillsLoader: skillPack(t, "research", "Research carefully", "Always verify claims.")})

	res, err := h.findTool("read_skill", "").invoke(t.Context(), `{"name":"research"}`, turnRuntime(h))

	if err != nil || !strings.Contains(res.Output, "Always verify claims") {
		t.Fatalf("read_skill = %q %v, want the skill's body", res.Output, err)
	}
}
