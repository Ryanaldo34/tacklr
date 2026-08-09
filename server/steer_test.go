package server

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestRegistry_promptWhileBusy_steers: session/prompt while a turn is in flight
// cancels the prior turn, pairs cancelled tool results, and runs the new prompt.
// ACP has no session/steer — busy prompt is the steer surface.
func TestRegistry_promptWhileBusy_steers(t *testing.T) {
	toolStarted := make(chan struct{})
	var toolStartedOnce sync.Once
	slow := tacklr.NewTool(tacklr.ToolConfig{
		Name: "slow",
		Handler: func(ctx context.Context) (string, error) {
			toolStartedOnce.Do(func() { close(toolStarted) })
			<-ctx.Done()
			return "", ctx.Err()
		},
	})

	var invokes atomic.Int32
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := invokes.Add(1)
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventFunctionCall,
					ToolCalls: []tacklr.ToolCall{
						{ID: "c1", CallID: "c1", Name: "slow", Arguments: `{}`},
					},
					IsComplete: true,
				}
				return
			}
			// Steer turn: model sees the new user text.
			var lastUser string
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser {
					lastUser = m.Content
				}
			}
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventMessage,
				Content:    "steered:" + lastUser,
				IsComplete: true,
			}
		},
	}

	store := testStore(t)
	r := newTestRegistry(store, strategy, []*tacklr.Tool{slow})
	const sessionID = "steer-session-1"

	// First turn: blocks in tool.
	s1, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID:              sessionID,
		AgentID:                "default",
		Prompt:                 "first",
		Load:                   false,
		AllowMissingCheckpoint: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-toolStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("tool did not start")
	}

	// Drain first stream in background (ends cancelled when second prompt steers).
	firstDone := make(chan []streaming.StreamEvent, 1)
	go func() {
		var evs []streaming.StreamEvent
		for ev := range s1.Events {
			evs = append(evs, ev)
		}
		firstDone <- evs
	}()

	// Second prompt steers.
	s2, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID:              sessionID,
		AgentID:                "default",
		Prompt:                 "do something else",
		Load:                   true,
		AllowMissingCheckpoint: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	var sawSteerMsg bool
	for ev := range s2.Events {
		if ev.Type == streaming.StreamEventMessage && strings.Contains(ev.Content, "steered:do something else") {
			sawSteerMsg = true
		}
	}
	if !sawSteerMsg {
		t.Fatal("expected steer turn to produce message with new user text")
	}

	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first turn did not finish after steer")
	}

	// Checkpoint has cancelled tool pairing + steer user message.
	loaded, err := tacklr.NewAgentFromSession(context.Background(), sessionID, tacklr.AgentOptions{
		Config:    tacklr.Config{MaxWindowSize: 8192},
		Model:     strategy,
		Tools:     []*tacklr.Tool{slow},
		Store:     store,
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawCancelled, sawSteerUser bool
	for _, m := range loaded.Messages() {
		if m.Role == tacklr.RoleTool && strings.Contains(m.Content, "cancelled") {
			sawCancelled = true
		}
		if m.Role == tacklr.RoleUser && m.Content == "do something else" {
			sawSteerUser = true
		}
	}
	if !sawCancelled {
		t.Fatalf("want cancelled tool result in window, got %+v", loaded.Messages())
	}
	if !sawSteerUser {
		t.Fatalf("want steer user message in window, got %+v", loaded.Messages())
	}
}

// TestRegistry_sessionCancel_doesNotStartNewTurn: session/cancel aborts only.
func TestRegistry_sessionCancel_doesNotStartNewTurn(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			once.Do(func() { close(started) })
			<-ctx.Done()
		},
	}
	r := newTestRegistry(testStore(t), strategy, nil)
	s, err := r.RunTurn(context.Background(), TurnRequest{
		SessionID:              "cancel-only",
		AgentID:                "default",
		Prompt:                 "hi",
		AllowMissingCheckpoint: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("turn did not start")
	}
	r.CancelSession("cancel-only")
	for range s.Events {
	}
	// No second turn was started; activeTurns should be clear.
	if _, ok := r.activeTurns.Load("cancel-only"); ok {
		t.Fatal("expected active turn cleared after cancel drain")
	}
}
