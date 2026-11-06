package worker

import (
	"bytes"
	"context"
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
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/orchestrator"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

// mcpServer is an MCP server over HTTP with one tool, ping. It counts the
// sessions clients open (initialize) and end (DELETE); deleted has a value
// for each end.
type mcpServer struct {
	config      session.MCPServer
	opens, ends atomic.Int32
	pings       atomic.Int32
	deleted     chan struct{}
}

func newMCPServer(t *testing.T, name string) *mcpServer {
	t.Helper()
	m := &mcpServer{deleted: make(chan struct{}, 16)}
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
			m.deleted <- struct{}{}
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
	timeout := time.After(5 * time.Second)
	for range n {
		select {
		case <-m.deleted:
		case <-timeout:
		}
	}
	if got := m.ends.Load(); got != n {
		t.Fatalf("MCP sessions ended = %d, want %d", got, n)
	}
}

// TestSessionWorkflow_aSessionsStepsShareOneHarness: a reply's calls and the
// steps around them share one harness: the VFS opens once and each MCP
// server is connected once. A prompt that changes the MCP servers builds a
// new harness, and the old one closes.
func TestSessionWorkflow_aSessionsStepsShareOneHarness(t *testing.T) {
	t.Parallel()
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

// TestActivities_anIdleHarnessCloses: a harness that no step uses for the
// idle time closes.
func TestActivities_anIdleHarnessCloses(t *testing.T) {
	t.Parallel()
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

// TestActivities_aWorkerSeesWhatAnotherCommittedAndFailsOnceTheSnapshotIsDeleted:
// two workers share the store. A step of one sees what the other committed,
// and a step fails once the snapshot is deleted.
func TestActivities_aWorkerSeesWhatAnotherCommittedAndFailsOnceTheSnapshotIsDeleted(t *testing.T) {
	t.Parallel()
	// Arrange
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
	store := session.NewMemoryStore()
	one, two := newActs(agent, session.NewMemoryEventLog(), true), newActs(agent, session.NewMemoryEventLog(), true)
	one.Store, two.Store = store, store
	sess := stepSession{SessionID: "shared"}
	rev, err := one.Commit(t.Context(), commitInput{stepSession: sess, In: say("first")})
	if err == nil {
		_, err = two.Commit(t.Context(), commitInput{stepSession: sess, Base: rev, In: say("second")})
	}
	if err != nil {
		t.Fatal(err)
	}

	// Act
	_, inferErr := one.Inference(t.Context(), sess)
	sawBoth := slices.Equal(seen, []string{"first", "second"})
	_ = store.Delete(t.Context(), "shared")
	_, deletedErr := one.Inference(t.Context(), sess)

	// Assert
	if inferErr != nil || !sawBoth {
		t.Fatalf("inference = %v, model saw %q; want both commits seen", inferErr, seen)
	}
	if deletedErr == nil || !strings.Contains(deletedErr.Error(), "was deleted") {
		t.Fatalf("step after the snapshot was deleted = %v, want it named", deletedErr)
	}
}

// TestActivities_aChildUsesItsParentsSecretsAndAStepFailsOnWhatItCannotRead:
// a child step without secrets of its own opens its workspace with its
// parent's token. A step whose secrets, or whose parent's secrets, do not
// open fails, and so do a step for an unknown specialist and a new turn's
// commit whose event log cannot give its head.
func TestActivities_aChildUsesItsParentsSecretsAndAStepFailsOnWhatItCannotRead(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx := t.Context()
	db, pool, schema := postgresDB(t)
	aes, err := session.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	secrets := db.Secrets(aes)
	parentAuth := session.AuthContext{Bindings: []mount.Binding{{
		Provider: "local",
		Params:   map[string]string{mount.ParamName: "docs"},
		Auth:     mount.Credential{Token: "parent-tok"},
	}}}
	if err := secrets.Put(ctx, "parent", session.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	// A row sealed for another session does not open as the row of "locked".
	foreign, _ := aes.Seal(ctx, []byte("{}"), []byte("other"))
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.session_secrets (session_id, sealed) VALUES ('locked', $1)`, foreign); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", vfs.Local(t.TempDir())))
	acts := newActs(tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, session.NewMemoryEventLog(), true)
	acts.Secrets = secrets
	acts.Workspace = workspace.Config{Open: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
		if len(req.Bindings) > 0 {
			gotToken = req.Bindings[0].Auth.Token
		}
		return open(ctx, sessionID, req)
	}}
	child := stepSession{SessionID: "child", Identity: orchestrator.Identity{Parent: "parent", Mounts: adapter.ApplyAuth(nil, parentAuth)}}
	locked := stepSession{SessionID: "locked"}
	lockedChild := stepSession{SessionID: "locked-child", Identity: orchestrator.Identity{Parent: "locked"}}
	unknown := stepSession{SessionID: "unknown", Identity: orchestrator.Identity{Specialist: "nobody"}}
	dropTable(t, pool, schema, "session_event_heads")
	log := db.EventLog()
	t.Cleanup(log.Close)
	headless := newActs(tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192}, log, true)

	// Act
	_, childErr := acts.Commit(ctx, commitInput{stepSession: child, In: say("hi")})
	_, inferErr := acts.Inference(ctx, locked)
	_, toolErr := acts.Tool(ctx, toolInput{stepSession: locked})
	_, commitErr := acts.Commit(ctx, commitInput{stepSession: locked})
	_, parentErr := acts.Commit(ctx, commitInput{stepSession: lockedChild})
	_, specialistErr := acts.Commit(ctx, commitInput{stepSession: unknown})
	_, headErr := headless.Commit(ctx, commitInput{stepSession: stepSession{SessionID: "fresh"}, In: say("hi"), NewTurn: true})

	// Assert
	if childErr != nil || gotToken != "parent-tok" {
		t.Fatalf("child commit %v, workspace token %q; want the parent's token", childErr, gotToken)
	}
	for name, err := range map[string]error{"inference": inferErr, "tool": toolErr, "commit": commitErr, "a child of it": parentErr} {
		if err == nil || !strings.Contains(err.Error(), `secrets "locked": open`) {
			t.Errorf("%s = %v, want the secrets of locked refused", name, err)
		}
	}
	if specialistErr == nil || !strings.Contains(specialistErr.Error(), "nobody") {
		t.Errorf("unknown specialist = %v, want it named", specialistErr)
	}
	if headErr == nil || !strings.Contains(headErr.Error(), "session_event_heads") {
		t.Errorf("a new turn's commit without the log's head = %v, want the database's error", headErr)
	}
}
