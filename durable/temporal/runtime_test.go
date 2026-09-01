package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_unknownAgentAndDirectCall(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	acts := &activities{Catalog: cat, Snapshots: snaps, Fallback: log, DisableStreams: true, Secrets: durable.NewMemorySecretStorage()}
	_, err := acts.Inference(t.Context(), inferenceInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	_, err = acts.Tool(t.Context(), toolInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}, Call: tacklr.ToolCall{ID: "c", Name: "x"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("tool missing agent: %v", err)
	}
	out, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "s", Rec: durable.Snapshot{AgentID: "default"},
		User:  &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
		State: map[string]any{"user": "Ryan"},
	})
	if err != nil || !out.Complete {
		t.Fatalf("direct inference: %+v %v", out, err)
	}
	snap, _, err := snaps.Load(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Checkpoint.UserState()["user"]) != `"Ryan"` {
		t.Fatalf("userState=%s", snap.Checkpoint.UserState()["user"])
	}
	if snap.AgentID != "default" {
		t.Fatalf("snapshot agent=%q", snap.AgentID)
	}
}

func TestNew_panicsWithoutClientOrCatalog(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	stub := &struct{ client.Client }{}
	snaps := inprocess.NewMemorySnapshot()
	secrets := durable.NewMemorySecretStorage()
	mustPanic(t, func() { New(nil, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets}) })
	mustPanic(t, func() { New(stub, Config{}) })
	mustPanic(t, func() { New(stub, Config{Catalog: cat}) })
	mustPanic(t, func() { New(stub, Config{Catalog: cat, Snapshots: snaps}) })
	mustPanic(t, func() { NewWorker(stub, Config{}) })
	log := inprocess.NewMemoryEventLog()
	rt := New(stub, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets, DisableStreams: true, TurnLocality: time.Minute, Fallback: log})
	if rt.taskQueue != "tacklr" || !rt.disableStreams {
		t.Fatalf("defaults tq=%q streams=%v", rt.taskQueue, rt.disableStreams)
	}
	if rt.activityTimeout != 10*time.Minute || rt.heartbeatTimeout != 30*time.Second || rt.activityAttempts != 3 {
		t.Fatalf("activity defaults timeout=%v heartbeat=%v attempts=%d", rt.activityTimeout, rt.heartbeatTimeout, rt.activityAttempts)
	}
	hour := New(stub, Config{Catalog: cat, Snapshots: snaps, Secrets: secrets, TaskQueue: "q", ActivityTimeout: time.Hour, HeartbeatTimeout: time.Minute, ActivityAttempts: 1})
	if hour.activityTimeout != time.Hour || hour.heartbeatTimeout != time.Minute || hour.activityAttempts != 1 {
		t.Fatalf("cfg timeout=%v heartbeat=%v attempts=%d", hour.activityTimeout, hour.heartbeatTimeout, hour.activityAttempts)
	}
	ctx := t.Context()
	if _, err := rt.Head(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	sub, err := rt.Subscribe(ctx, "gone", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = sub.Close()
	rt.markClosed("gone")
	if err := rt.Prompt(ctx, "gone", durable.Prompt{Text: "x"}); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed session: %v", err)
	}
	if _, err := rt.Status(ctx, "gone"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed status: %v", err)
	}
	if _, err := rt.Children(ctx, "gone"); !errors.Is(err, durable.ErrSessionNotFound) {
		t.Fatalf("closed children: %v", err)
	}
	if _, err := rt.CreateSession(ctx, durable.CreateSession{AgentID: "missing"}); !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("unknown agent: %v", err)
	}
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	fn()
}

func lastMsg(msgs []*tacklr.Message) *tacklr.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			return msgs[i]
		}
	}
	return nil
}

func drainLog(t *testing.T, log durable.EventLog, id durable.SessionID) []tacklr.StreamEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ch, err := log.Subscribe(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []tacklr.StreamEvent
	for ev := range ch {
		got = append(got, ev)
	}
	return got
}

func querySession(t *testing.T, env *testsuite.TestWorkflowEnvironment) durable.SessionStatus {
	t.Helper()
	val, err := env.QueryWorkflow(queryStatus)
	if err != nil {
		t.Fatal(err)
	}
	var st durable.SessionStatus
	if err := val.Get(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

type retryLog struct {
	durable.EventLog
	retry []tacklr.StreamEvent
}

func (l *retryLog) Append(ctx context.Context, id durable.SessionID, topic string, ev tacklr.StreamEvent) error {
	if topic == durable.TopicRetry {
		l.retry = append(l.retry, ev)
	}
	return l.EventLog.Append(ctx, id, topic, ev)
}

func newActs(cat *durable.MemoryCatalog, log durable.EventLog, disableStreams bool) *activities {
	return &activities{
		Catalog:        cat,
		Snapshots:      inprocess.NewMemorySnapshot(),
		Projection:     vfs.DirectProjection{},
		Fallback:       log,
		DisableStreams: disableStreams,
		Secrets:        durable.NewMemorySecretStorage(),
	}
}
