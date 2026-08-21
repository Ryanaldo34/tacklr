package stores

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func rawState(t *testing.T, values map[string]any) map[string]json.RawMessage {
	t.Helper()
	out := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		out[key] = raw
	}
	return out
}

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
		rawState(t, map[string]any{"plan": "Ship"}),
		rawState(t, map[string]any{"module": map[string]any{"enabled": true}}),
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
	ptc, ok := loaded.PendingToolCalls()["call_1"]
	if !ok || ptc.ToolCall == nil || ptc.ToolCall.Name != "greet" || !ptc.InterruptActive {
		t.Errorf("pending tool calls = %+v", loaded.PendingToolCalls())
	}
	if len(loaded.UserState()["plan"]) == 0 || len(loaded.Modules()["module"]) == 0 {
		t.Errorf("checkpoint state missing: user=%+v modules=%+v", loaded.UserState(), loaded.Modules())
	}
	if len(loaded.PendingInterrupts()) == 0 || len(loaded.ResolvedInterrupts()) == 0 {
		t.Errorf("interrupt blobs empty: pending=%d resolved=%d",
			len(loaded.PendingInterrupts()), len(loaded.ResolvedInterrupts()))
	}
}

func TestFaultyStore_saveAndLoadFaults(t *testing.T) {
	ctx := context.Background()
	inner := NewInMemoryStore()
	cp, err := NewCheckpoint([]*streaming.Message{{Role: streaming.RoleUser, Content: "hi"}}, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	save := FaultyStore{Inner: inner, SaveErr: errors.New("disk full")}
	if err := save.SaveSession(ctx, "s1", *cp); err == nil || err.Error() != "disk full" {
		t.Fatalf("save fault: %v", err)
	}
	if err := inner.SaveSession(ctx, "s1", *cp); err != nil {
		t.Fatal(err)
	}
	load := FaultyStore{Inner: inner, LoadErr: errors.New("db down")}
	if _, err := load.LoadSession(ctx, "s1"); err == nil || err.Error() != "db down" {
		t.Fatalf("load fault: %v", err)
	}
	ok := FaultyStore{Inner: inner}
	got, err := ok.LoadSession(ctx, "s1")
	if err != nil || len(got.ContextWindow) != 1 {
		t.Fatalf("passthrough load: %+v err=%v", got, err)
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

func TestNewCheckpoint_nilInterruptBlobsAndMarshalError(t *testing.T) {
	cp, err := NewCheckpoint(nil, nil, rawState(t, map[string]any{"k": 1}), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cp.PendingInterrupts() != nil || cp.ResolvedInterrupts() != nil {
		t.Fatalf("nil blobs should stay nil: pending=%v resolved=%v",
			cp.PendingInterrupts(), cp.ResolvedInterrupts())
	}
	// json.Marshal of a channel fails — exercise error return path.
	if _, err := NewCheckpoint(nil, nil, nil, nil, make(chan int), nil); err == nil {
		t.Fatal("expected marshal error for pending interrupts")
	}
	if _, err := NewCheckpoint(nil, nil, nil, nil, nil, make(chan int)); err == nil {
		t.Fatal("expected marshal error for resolved interrupts")
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
			}, nil, rawState(t, map[string]any{"i": i}), nil, nil, nil)
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
