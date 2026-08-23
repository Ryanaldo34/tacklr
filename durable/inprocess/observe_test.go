package inprocess

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

type countingInstrumentor struct {
	n atomic.Int64
}

func (c *countingInstrumentor) StartTurn(ctx context.Context, attrs telemetry.TurnAttrs) (context.Context, *telemetry.TurnSpan) {
	c.n.Add(1)
	if attrs.Runtime != telemetry.RuntimeInProcess {
		panic("runtime")
	}
	return telemetry.DefaultInstrumentor().StartTurn(ctx, attrs)
}

func TestPrompt_usesInstrumentorHook(t *testing.T) {
	ctx := t.Context()
	hook := &countingInstrumentor{}
	rt := New(
		newCatalog(t, scriptedComplete("hello from agent"), durable.AgentSpec{}),
		WithProjection(vfs.DirectProjection{}),
		WithInstrumentor(nil),
		WithInstrumentor(hook),
	)
	id, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(ctx, id, durable.Prompt{Text: "hi there"}); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	_ = waitEvents(t, sub, 5*time.Second)
	if hook.n.Load() != 1 {
		t.Fatalf("want 1 turn start, got %d", hook.n.Load())
	}
}
