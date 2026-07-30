package stores

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestInMemoryStore_saveLoadRoundTrip(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	cp, err := NewCheckpoint(
		[]*streaming.Message{
			{Role: streaming.RoleUser, Content: "hello"},
			{Role: streaming.RoleAssistant, Content: "world"},
		},
		map[string]PendingToolCall{
			"call_1": {ToolCall: &streaming.ToolCall{ID: "call_1", Name: "greet"}, InterruptActive: true},
		},
		map[string]string{"intr-1": "call_1"},
		map[string]any{"plan": []any{map[string]any{"title": "Ship", "status": "completed"}}},
		map[string]any{"pending": true},
		map[string]any{"resolved": "x"},
	)
	if err != nil {
		t.Fatalf("NewCheckpoint: %v", err)
	}

	if err := store.SaveSession(ctx, "sess-1", *cp); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}

	loaded, err := store.LoadSession(ctx, "sess-1")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(loaded.ContextWindow) != 2 {
		t.Fatalf("context len = %d, want 2", len(loaded.ContextWindow))
	}
	if loaded.ContextWindow[0].Content != "hello" || loaded.ContextWindow[1].Content != "world" {
		t.Errorf("context = %+v", loaded.ContextWindow)
	}
	ptc, ok := loaded.State.PendingToolCalls["call_1"]
	if !ok || ptc.ToolCall == nil || ptc.ToolCall.Name != "greet" || !ptc.InterruptActive {
		t.Errorf("pending tool calls = %+v", loaded.State.PendingToolCalls)
	}
	if loaded.State.InterruptToRequester["intr-1"] != "call_1" {
		t.Errorf("interrupt map = %+v", loaded.State.InterruptToRequester)
	}
	if loaded.State.RuntimeState["plan"] == nil {
		t.Errorf("runtime state missing plan: %+v", loaded.State.RuntimeState)
	}
	if len(loaded.State.PendingInterrupts) == 0 || len(loaded.State.ResolvedInterrupts) == 0 {
		t.Errorf("interrupt blobs empty: pending=%d resolved=%d",
			len(loaded.State.PendingInterrupts), len(loaded.State.ResolvedInterrupts))
	}
}

func TestInMemoryStore_loadMissing_returnsErrSessionNotFound(t *testing.T) {
	store := NewInMemoryStore()
	_, err := store.LoadSession(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("err = %v, want ErrSessionNotFound", err)
	}
}

func TestInMemoryStore_saveOverwritesSession(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	first, err := NewCheckpoint([]*streaming.Message{{Role: streaming.RoleUser, Content: "v1"}}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCheckpoint([]*streaming.Message{{Role: streaming.RoleUser, Content: "v2"}}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(ctx, "sess", *first); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(ctx, "sess", *second); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadSession(ctx, "sess")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ContextWindow[0].Content != "v2" {
		t.Errorf("content = %q, want v2", loaded.ContextWindow[0].Content)
	}
}

func TestInMemoryStore_concurrentSaveLoad(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cp, err := NewCheckpoint([]*streaming.Message{
				{Role: streaming.RoleUser, Content: "n"},
			}, nil, nil, map[string]any{"i": i}, nil, nil)
			if err != nil {
				t.Errorf("checkpoint: %v", err)
				return
			}
			if err := store.SaveSession(ctx, "shared", *cp); err != nil {
				t.Errorf("save: %v", err)
			}
			if _, err := store.LoadSession(ctx, "shared"); err != nil && !errors.Is(err, ErrSessionNotFound) {
				t.Errorf("load: %v", err)
			}
		}(i)
	}
	wg.Wait()
	loaded, err := store.LoadSession(ctx, "shared")
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if len(loaded.ContextWindow) != 1 {
		t.Fatalf("final context len = %d", len(loaded.ContextWindow))
	}
}
