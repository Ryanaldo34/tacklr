package inprocess

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestNew_requiresCatalog(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic on nil catalog")
		}
	}()
	New(nil)
}

func TestNew_nilOptionAndNilProjection(t *testing.T) {
	cat := newCatalog(t, scriptedComplete("x"), durable.AgentSpec{})
	snaps := NewMemorySnapshot()
	rt := New(cat, nil, WithSnapshotStore(nil), WithProjection(nil), WithSnapshotStore(snaps))
	if rt.Catalog() != cat {
		t.Fatal("catalog")
	}
}

func TestCreateSession_errors(t *testing.T) {
	ctx := t.Context()
	cat := newCatalog(t, scriptedComplete("x"), durable.AgentSpec{})
	rt := New(cat, WithProjection(vfs.DirectProjection{}))
	ctxDone, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := rt.CreateSession(ctxDone, durable.CreateSession{AgentID: "default"}); err == nil {
		t.Fatal("want canceled")
	}
	if _, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "missing"}); !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default", SessionID: "dup"})
	if err != nil || id != "dup" {
		t.Fatal(err)
	}
	if _, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default", SessionID: "dup"}); !errors.Is(err, durable.ErrSessionExists) {
		t.Fatalf("dup: %v", err)
	}
	if err := rt.Prompt(ctx, "nope", durable.Prompt{Text: "x"}); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("prompt missing: %v", err)
	}
	if _, err := rt.Head(ctx, "nope"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("head missing: %v", err)
	}
	if _, err := rt.Subscribe(ctx, "nope", 0); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("sub missing: %v", err)
	}
	if err := rt.Close(ctx, "nope"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("close missing: %v", err)
	}
}

func TestPrompt_noAgentConfigured(t *testing.T) {
	ctx := t.Context()
	rt := New(durable.NewCatalog(""), WithProjection(vfs.DirectProjection{}))
	id, err := rt.CreateSession(ctx, durable.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("closed")
			}
			if ev.Type == streaming.StreamEventError {
				return
			}
		case <-deadline:
			t.Fatal("want error event")
		}
	}
}

func TestPrompt_userMessageAndUnknownAgent(t *testing.T) {
	ctx := t.Context()
	rt := New(newCatalog(t, scriptedComplete("ok"), durable.AgentSpec{}), WithProjection(vfs.DirectProjection{}))
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{AgentID: "ghost"}); !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("ghost agent: %v", err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{UserMessage: &tacklr.Message{Role: tacklr.RoleUser, Content: "via-msg"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	got := waitEvents(t, sub, 5*time.Second)
	var saw bool
	for _, ev := range got {
		if ev.Content == "ok" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want completion, got %+v", got)
	}
}

func TestEventLog_headEndAndCloseUnknown(t *testing.T) {
	ctx := t.Context()
	log := NewMemoryEventLog()
	seq, err := log.Head(ctx, "missing")
	if err != nil || seq != 0 {
		t.Fatalf("head missing: %v %v", seq, err)
	}
	log.EndSubscribers("missing")
	if err := log.CloseSession(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_usesDefaultAgentWhenEmpty(t *testing.T) {
	ctx := t.Context()
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: scriptedComplete("hi"), Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	rt := New(cat, WithProjection(vfs.DirectProjection{}))
	id, err := rt.CreateSession(ctx, durable.CreateSession{})
	if err != nil || id == "" {
		t.Fatal(err)
	}
}
