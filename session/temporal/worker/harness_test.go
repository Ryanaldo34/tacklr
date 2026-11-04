package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session/temporal/internal/protocol"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/loop"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/workspace"
)

// mcpServer is an MCP server over HTTP with one tool, ping. It counts the
// sessions clients open (initialize) and end (DELETE).
type mcpServer struct {
	config      session.MCPServer
	opens, ends atomic.Int32
	pings       atomic.Int32
}

func newMCPServer(t *testing.T, name string) *mcpServer {
	t.Helper()
	m := &mcpServer{}
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: name, Version: "v0.0.1"}, nil)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "ping", Description: "answers pong"},
		func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
			m.pings.Add(1)
			return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "pong"}}}, struct{}{}, nil
		})
	h := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			m.ends.Add(1)
		case http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			if bytes.Contains(body, []byte(`"method":"initialize"`)) {
				m.opens.Add(1)
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(hs.Close)
	m.config = session.MCPServer{Name: name, Type: session.MCPHTTP, URL: hs.URL}
	return m
}

// ended waits for clients to have ended n sessions: a harness closes after
// its last step returns.
func (m *mcpServer) ended(t *testing.T, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for m.ends.Load() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.ends.Load(); got != n {
		t.Fatalf("MCP sessions ended = %d, want %d", got, n)
	}
}

// A reply's calls and the steps around them share one harness: the VFS opens
// once and each MCP server is connected once. A prompt that changes the MCP
// servers builds a new harness, and the old one closes.
func TestSessionWorkflow_aSessionsStepsShareOneHarness(t *testing.T) {
	env := newTestWorkflow(t)
	first, second := newMCPServer(t, "first"), newMCPServer(t, "second")
	var opens atomic.Int32
	tree := vfs.Tree(vfs.At("work", vfs.Local(t.TempDir())))
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if last := lastMsg(msgs); last != nil && last.Role == message.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done", IsComplete: true}
			return
		}
		ping := func(id string) message.ToolCall {
			return message.ToolCall{ID: id, CallID: id, Name: "ping", Namespace: "first", Arguments: `{}`}
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
			ToolCalls: []message.ToolCall{ping("p1"), ping("p2"), ping("p3")}}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, MCP: mcp.Client{}}
	acts, _ := registerSession(env, agent)
	acts.Workspace = workspace.Config{Open: func(ctx context.Context, id string, req vfs.Request) (*vfs.MountSession, error) {
		opens.Add(1)
		return tree(ctx, id, req)
	}}
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "go", MCPServers: []session.MCPServer{first.config}}})
	// A steer that changes the MCP servers joins the running turn (queued
	// before its first request, it is in that request), which keeps its
	// harness; the next idle prompt gets the new servers.
	signalAt(env, time.Millisecond, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "steer", MCPServers: []session.MCPServer{first.config, second.config}}})
	when(env, func() bool { return querySession(t, env).State == session.Complete }, func() {
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: "again"}})
		env.SignalWorkflow(protocol.SignalWake, session.Wake{Kind: session.WakeClose})
	})
	env.ExecuteWorkflow(protocol.Workflow, protocol.Input{SessionID: "sess-one-harness"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if first.pings.Load() != 6 {
		t.Fatalf("pings = %d, want three calls in each of two replies: the turn with its steer, and the next prompt's", first.pings.Load())
	}
	if opens.Load() != 2 || first.opens.Load() != 2 || second.opens.Load() != 1 {
		t.Fatalf("VFS opens = %d, MCP sessions first = %d second = %d; want one harness per MCP config: 2, 2, 1",
			opens.Load(), first.opens.Load(), second.opens.Load())
	}
	first.ended(t, 2)
	second.ended(t, 1)
}

// A harness nobody uses for the idle time closes.
func TestActivities_anIdleHarnessCloses(t *testing.T) {
	server := newMCPServer(t, "idle")
	acts := newActs(tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192, MCP: mcp.Client{}}, session.NewMemoryEventLog(), true)
	acts.harnesses = newHarnessCache(20*time.Millisecond, 0)
	in := commitInput{stepSession: stepSession{SessionID: "idle", MCPServers: []session.MCPServer{server.config}},
		In: tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: "hi"}}}}

	if _, err := acts.Commit(t.Context(), in); err != nil {
		t.Fatal(err)
	}

	server.ended(t, 1)
}

// countingStore is a session.MemoryStore that counts full loads.
type countingStore struct {
	*session.MemoryStore
	loads atomic.Int32
}

func (s *countingStore) Load(ctx context.Context, id session.ID) ([]session.CheckpointEntry, session.Revision, error) {
	s.loads.Add(1)
	return s.MemoryStore.Load(ctx, id)
}

// Two workers share the store. A worker loads the snapshot only when another
// committed since it last held it, and fails a step once the snapshot is
// deleted.
func TestActivities_aHarnessLoadsCommittedStateOnlyWhenTheRevisionMoved(t *testing.T) {
	var seen []string
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		seen = seen[:0]
		for _, m := range msgs {
			if m.Role == message.RoleUser {
				seen = append(seen, m.Content)
			}
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "ok", IsComplete: true}
	})
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}
	store := &countingStore{MemoryStore: session.NewMemoryStore()}
	one, two := newActs(agent, session.NewMemoryEventLog(), true), newActs(agent, session.NewMemoryEventLog(), true)
	one.Store, two.Store = store, store
	sess := stepSession{SessionID: "shared"}
	user := func(text string) tacklr.CommitIn {
		return tacklr.CommitIn{Users: []*message.Message{{Role: message.RoleUser, Content: text}}}
	}

	rev, err := one.Commit(t.Context(), commitInput{stepSession: sess, In: user("first")})
	if err == nil {
		_, err = two.Commit(t.Context(), commitInput{stepSession: sess, Base: rev, In: user("second")})
	}
	if err != nil {
		t.Fatal(err)
	}
	loadsBefore := store.loads.Load()
	_, inferErr := one.Inference(t.Context(), sess)
	afterOtherCommit := store.loads.Load() - loadsBefore
	sawBoth := slices.Equal(seen, []string{"first", "second"})
	_, againErr := one.Inference(t.Context(), sess)
	unchanged := store.loads.Load() - loadsBefore - afterOtherCommit
	_ = store.Delete(t.Context(), "shared")
	_, deletedErr := one.Inference(t.Context(), sess)

	if inferErr != nil || againErr != nil || !sawBoth {
		t.Fatalf("inference = %v, %v; model saw %q; want both commits seen", inferErr, againErr, seen)
	}
	if afterOtherCommit != 1 || unchanged != 0 {
		t.Fatalf("loads after the other worker committed = %d, with nothing new = %d; want 1 and 0", afterOtherCommit, unchanged)
	}
	if deletedErr == nil || !strings.Contains(deletedErr.Error(), "was deleted") {
		t.Fatalf("step after the snapshot was deleted = %v, want it named", deletedErr)
	}
}

// unreadableSecrets is a SecretStorage that cannot read the session it names.
type unreadableSecrets struct {
	*session.MemorySecretStorage
	id session.ID
}

func (s unreadableSecrets) Get(ctx context.Context, id session.ID) (session.Secrets, error) {
	if id == s.id {
		return session.Secrets{}, errors.New("secrets unavailable")
	}
	return s.MemorySecretStorage.Get(ctx, id)
}

// A step whose harness cannot be named (its secrets, or its parent's, cannot
// be read) or built (an unknown specialist) fails.
func TestActivities_aStepFailsWhenItsHarnessCannotBeNamedOrBuilt(t *testing.T) {
	acts := newActs(tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, session.NewMemoryEventLog(), true)
	acts.Secrets = unreadableSecrets{MemorySecretStorage: session.NewMemorySecretStorage(), id: "locked"}
	locked := stepSession{SessionID: "locked"}
	child := stepSession{SessionID: "child", Identity: loop.Identity{Parent: "locked"}}
	unknown := stepSession{SessionID: "unknown", Identity: loop.Identity{Specialist: "nobody"}}

	_, inferErr := acts.Inference(t.Context(), locked)
	_, toolErr := acts.Tool(t.Context(), toolInput{stepSession: locked})
	_, commitErr := acts.Commit(t.Context(), commitInput{stepSession: locked})
	_, parentErr := acts.Commit(t.Context(), commitInput{stepSession: child})
	_, specialistErr := acts.Commit(t.Context(), commitInput{stepSession: unknown})

	for name, err := range map[string]error{"inference": inferErr, "tool": toolErr, "commit": commitErr, "a child of it": parentErr} {
		if err == nil || !strings.Contains(err.Error(), "secrets unavailable") {
			t.Errorf("%s = %v, want the secrets failure", name, err)
		}
	}
	if specialistErr == nil || !strings.Contains(specialistErr.Error(), "nobody") {
		t.Errorf("unknown specialist = %v, want it named", specialistErr)
	}
}
