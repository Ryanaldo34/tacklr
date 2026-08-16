// Package testkit provides shared test doubles for harness and server integration tests.
package testkit

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ryanaldo34/tacklr"
)

// ScriptedModel is a cancel-aware InferenceStrategy for tests.
// Default CountTokens is sum of message content lengths (not zero), so window-pressure
// paths can fire when MaxWindowSize is small. Override CountTokensFn to customize.
type ScriptedModel struct {
	InvokeFn      func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk)
	InvokeErr     error
	InvokeErrFn   func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) error
	CountTokensFn func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (int, error)

	CallNum         atomic.Int64
	mu              sync.Mutex
	SystemPrompts   []string
	LastInvokeMsgs  []*tacklr.Message
	LastInvokeTools []*tacklr.Tool
}

func (m *ScriptedModel) SupportsMIME(mimeType string) bool {
	// Scripted models accept common binary types unless overridden later.
	return true
}
func (m *ScriptedModel) MaxContextWindow() (int, error) {
	return 0, nil
}

func (m *ScriptedModel) CountTokens(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool) (int, error) {
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

func (m *ScriptedModel) Invoke(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, systemPrompt string) (chan tacklr.LLMResponseChunk, error) {
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
	ch := make(chan tacklr.LLMResponseChunk)
	go func() {
		defer close(ch)
		if m.InvokeFn == nil {
			return
		}
		// InvokeFn may ignore ctx; still stop writing when cancelled if it uses select.
		m.InvokeFn(ctx, msgs, tools, ch)
	}()
	return ch, nil
}

// ContentTokenEstimate is a shared length-based token stand-in for pressure tests.
func ContentTokenEstimate(msgs []*tacklr.Message) int {
	n := 0
	for _, m := range msgs {
		if m != nil {
			n += len(m.Content)
		}
	}
	return n
}
