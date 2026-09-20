package tacklr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// scriptedModel is the package-tacklr test InferenceStrategy. Root tests cannot
// import internal/testkit (cycle: testkit → tacklr). Same shape as testkit.ScriptedModel.
type scriptedModel struct {
	InvokeFn       func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk)
	InvokeErr      error
	InvokeErrFn    func(ctx context.Context, msgs []*Message, tools []*Tool) error
	CountTokensFn  func(ctx context.Context, msgs []*Message, tools []*Tool) (int, error)
	SupportsMIMEFn func(string) bool

	CallNum         atomic.Int64
	mu              sync.Mutex
	SystemPrompts   []string
	LastInvokeMsgs  []*Message
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

func (m *scriptedModel) CountTokens(ctx context.Context, msgs []*Message, tools []*Tool) (int, error) {
	if m.CountTokensFn != nil {
		return m.CountTokensFn(ctx, msgs, tools)
	}
	n := 0
	for _, msg := range msgs {
		if msg != nil {
			n += len(msg.Content)
		}
	}
	return n, nil
}

func (m *scriptedModel) Invoke(ctx context.Context, msgs []*Message, tools []*Tool, systemPrompt string) (chan LLMResponseChunk, error) {
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

type recordingWatchdog struct {
	mu          sync.Mutex
	outputs     []*Message
	toolResults []*Message
}

func (w *recordingWatchdog) RecordOutput(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.outputs = append(w.outputs, msg)
	return nil
}
func (w *recordingWatchdog) RecordToolResult(msg *Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.toolResults = append(w.toolResults, msg)
	return nil
}

func reloadHarness(t *testing.T, h *TurnManager, opts AgentOptions) *TurnManager {
	t.Helper()
	cp, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	n, err := NewTurnManager(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.RestoreCheckpoint(*cp); err != nil {
		t.Fatal(err)
	}
	return n
}

// turnRuntime builds a turn-scoped Runtime for tests (events drained).
func turnRuntime(h *TurnManager) HarnessRuntime {
	ch := make(chan StreamEvent, 64)
	go func() {
		for range ch {
		}
	}()
	return newToolRuntime(ch, h.session, h.jobHost)
}

func mustNewTurnManager(t testing.TB, opts AgentOptions) *TurnManager {
	t.Helper()
	h, err := NewTurnManager(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func mustMountTree(t testing.TB, sessionID string, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	return mustMountTreeReq(t, sessionID, vfs.Request{}, members...)
}

func mustNS(t testing.TB, nv ...string) brain.Namespace {
	t.Helper()
	ns, err := brain.ParseNamespace(nv...)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func mustMountTreeReq(t testing.TB, sessionID string, req vfs.Request, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...)(t.Context(), sessionID, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}
