package tacklr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// scriptedModel is the package-tacklr test Model. Root tests cannot
// import internal/testkit (cycle: testkit → tacklr). Same shape as testkit.ScriptedModel.
type scriptedModel struct {
	InvokeFn       func(ctx context.Context, msgs []*message.Message, tools []*Tool, ch chan<- LLMResponseChunk)
	InvokeErr      error
	InvokeErrFn    func(ctx context.Context, msgs []*message.Message, tools []*Tool) error
	CountTokensFn  func(ctx context.Context, msgs []*message.Message, tools []*Tool) (int, error)
	SupportsMIMEFn func(string) bool

	CallNum         atomic.Int64
	mu              sync.Mutex
	SystemPrompts   []string
	LastInvokeMsgs  []*message.Message
	LastInvokeTools []*Tool
}

func (m *scriptedModel) ModelTelemetryIdentity() telemetry.ModelIdentity {
	return telemetry.ModelIdentity{Provider: "unknown", Model: "scripted"}
}

func (m *scriptedModel) SupportsMIME(mimeType string) bool {
	if m.SupportsMIMEFn != nil {
		return m.SupportsMIMEFn(mimeType)
	}
	return true
}
func (m *scriptedModel) MaxContextWindow() (int, error) { return 8192, nil }

func (m *scriptedModel) CountTokens(ctx context.Context, req ModelRequest) (int, error) {
	if m.CountTokensFn != nil {
		return m.CountTokensFn(ctx, req.Messages, req.Tools)
	}
	n := 0
	for _, msg := range req.Messages {
		if msg != nil {
			n += len(msg.Content)
		}
	}
	return n, nil
}

func (m *scriptedModel) Invoke(ctx context.Context, req ModelRequest) (chan LLMResponseChunk, error) {
	msgs, tools, systemPrompt := req.Messages, req.Tools, req.SystemPrompt
	if m.InvokeErr != nil {
		return nil, m.InvokeErr
	}
	if m.InvokeErrFn != nil {
		if err := m.InvokeErrFn(ctx, msgs, tools); err != nil {
			return nil, err
		}
	}
	m.CallNum.Add(1)
	m.mu.Lock()
	m.LastInvokeMsgs = msgs
	m.LastInvokeTools = tools
	if systemPrompt != "" {
		m.SystemPrompts = append(m.SystemPrompts, systemPrompt)
	}
	m.mu.Unlock()
	ch := make(chan LLMResponseChunk)
	go func() {
		defer close(ch)
		if m.InvokeFn != nil {
			m.InvokeFn(ctx, msgs, tools, ch)
		}
	}()
	return ch, nil
}

func (m *scriptedModel) LastSystemPrompt() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := len(m.SystemPrompts); n > 0 {
		return m.SystemPrompts[n-1]
	}
	return ""
}

func reloadHarness(t *testing.T, h *Harness, opts AgentOptions) *Harness {
	t.Helper()
	cp, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	n, err := NewHarness(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.RestoreCheckpoint(cp); err != nil {
		t.Fatal(err)
	}
	return n
}

// callRuntime is the runtime of one call, id, on sm (events drained).
func callRuntime(sm *sessionManager, id string) *CallContext {
	return newCallContext(drainEventCh(), &Harness{session: sm}, ToolRun{}, roundCall{Call: message.ToolCall{ID: id, CallID: id}}, callWrites{})
}

// turnRuntime is the runtime of one call on h.
func turnRuntime(h *Harness) *CallContext {
	return newCallContext(drainEventCh(), h, ToolRun{}, roundCall{}, callWrites{})
}

// commit applies what the call wrote to the committed state, as Commit does.
func (cc *CallContext) commit() {
	cc.session.apply(cc.writes)
}

func mustNewHarness(t testing.TB, opts AgentOptions) *Harness {
	t.Helper()
	h, err := NewHarness(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

func mustMountTree(t testing.TB, sessionID string, req vfs.Request, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...)(t.Context(), sessionID, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

// set and setDocument seed a plan; tools change it through planStore.Change.
func (p *planStore) set(todos []Todo) {
	_ = p.Change(func(_ []Todo, doc string) ([]Todo, string, error) { return todos, doc, nil })
}

func (p *planStore) setDocument(doc string) {
	_ = p.Change(func(todos []Todo, _ string) ([]Todo, string, error) { return todos, doc, nil })
}

// drainEventCh is an event channel nothing reads.
func drainEventCh() chan session.Event {
	c := make(chan session.Event, 8)
	go func() {
		for range c {
		}
	}()
	return c
}
