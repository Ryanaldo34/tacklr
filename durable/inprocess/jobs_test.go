package inprocess

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestJobs_workerChildSteerAndHostList(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	schedule := tacklr.NewTool(tacklr.ToolConfig{
		Name: "watch_ci",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			job, err := runtime.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "pipe-1"})
			if err != nil {
				return "", err
			}
			return "Job " + job.ID + " scheduled (name=ci).", nil
		},
	})
	var sawJob atomic.Bool
	parent := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			for _, m := range msgs {
				if m != nil && m.Role == tacklr.RoleUser && strings.Contains(m.Content, "completed:") && strings.Contains(m.Content, "green") {
					sawJob.Store(true)
				}
			}
			if sawJob.Load() {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "parent-done", IsComplete: true}
				return
			}
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool && strings.Contains(last.Content, "scheduled") {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "too-soon", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ci1", CallID: "ci1", Name: "watch_ci", Arguments: `{}`,
				}},
				IsComplete: true,
			}
		},
	}
	snaps := NewMemorySnapshot()
	rt := New(Config{
		Catalog: newCatalog(t, parent, durable.AgentSpec{
			Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{schedule}},
		}),
		Snapshots:  snaps,
		Projection: vfs.DirectProjection{},
		Jobs: map[string]durable.JobHandler{
			"ci": func(ctx context.Context, task string) (string, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return "", ctx.Err()
				}
				return "green", nil
			},
		},
	})
	var id durable.SessionID
	sub := begin(t, rt, &id)
	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && ev.Content == "too-soon"
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	jobs, err := rt.Jobs(t.Context(), id)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	if jobs[0].Kind != durable.SessionKindWorker || jobs[0].Specialist != "ci" || jobs[0].State != durable.SessionRunning {
		t.Fatalf("worker status=%+v", jobs[0])
	}
	kids, err := rt.Children(t.Context(), id)
	if err != nil || len(kids) != 1 || kids[0] != jobs[0].ID {
		t.Fatalf("children=%v job=%s err=%v", kids, jobs[0].ID, err)
	}
	snap, _, err := snaps.Load(t.Context(), jobs[0].ID)
	if err != nil || snap.Worker != "ci" || snap.Parent != id {
		t.Fatalf("worker snapshot=%+v err=%v", snap, err)
	}
	close(release)
	waitParentEvent(t, rt, id, sub, 8*time.Second, func(ev tacklr.StreamEvent) bool {
		return ev.Type == tacklr.StreamEventMessage && ev.Content == "parent-done"
	})
	if !sawJob.Load() {
		t.Fatal("parent did not see job steer")
	}
	jobs, err = rt.Jobs(t.Context(), id)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("harvested jobs=%+v err=%v", jobs, err)
	}
}

func TestJobs_workerRetryThenComplete(t *testing.T) {
	var n atomic.Int32
	rt := New(Config{
		Catalog:    newCatalog(t, scriptedComplete("unused"), durable.AgentSpec{}),
		Snapshots:  NewMemorySnapshot(),
		Projection: vfs.DirectProjection{},
		Jobs: map[string]durable.JobHandler{
			"flaky": func(ctx context.Context, task string) (string, error) {
				if n.Add(1) == 1 {
					return "", context.DeadlineExceeded
				}
				return "ok-" + task, nil
			},
		},
	})
	parent, err := rt.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	wid, err := rt.CreateSession(t.Context(), durable.CreateSession{
		Parent:    parent,
		AgentID:   "default",
		Worker:    "flaky",
		SessionID: durable.JobID(parent, "flaky", "c1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Prompt(t.Context(), wid, durable.Prompt{Text: "pipe"}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, rt, wid, func(st durable.SessionStatus) bool { return st.State == durable.SessionFailed })
	if err := rt.Prompt(t.Context(), wid, durable.Prompt{Text: "pipe"}); err != nil {
		t.Fatal(err)
	}
	st := waitStatus(t, rt, wid, func(st durable.SessionStatus) bool { return st.State == durable.SessionComplete })
	if st.Result != "ok-pipe" || n.Load() != 2 {
		t.Fatalf("status=%+v attempts=%d", st, n.Load())
	}
}
