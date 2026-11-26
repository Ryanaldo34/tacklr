package orchestrator_test

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
	"github.com/ryanaldo34/tacklr/session/orchestrator"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// step is what the test sends when the session waits for a wake and none
// is queued: the session's next wakes, given what it did so far.
type step func(rt *refRuntime) []session.Wake

// refRuntime is an orchestrator.Runtime in memory: one real harness over a
// memory store, each step run once, and a mailbox that the test's steps
// fill. A wake sent while a step runs (send) is queued, and a cancel among
// them stops the step. A job runs until a step sends its end; a worker
// session's job runs its handler.
type refRuntime struct {
	t     *testing.T
	id    session.ID
	agent tacklr.AgentOptions
	jobs  map[string]session.JobHandler
	store session.Store
	h     *tacklr.Harness
	snap  adapter.Snapshot
	state *orchestrator.State
	steps []step

	mu        sync.Mutex
	queue     []session.Wake
	notify    chan struct{}
	events    []session.Event
	cancelled []session.ID
	// startErr is the error Start fails with.
	startErr error
	// mcp is the names of the MCP servers each turn began with.
	mcp [][]string
}

func newRef(t *testing.T, agent tacklr.AgentOptions, jobs map[string]session.JobHandler) *refRuntime {
	t.Helper()
	if agent.MaxWindowSize == 0 {
		agent.MaxWindowSize = 8192
	}
	rt := &refRuntime{t: t, id: "sess", agent: agent, jobs: jobs, store: session.NewMemoryStore(), notify: make(chan struct{}, 1)}
	t.Cleanup(func() {
		if rt.h != nil {
			rt.h.Close()
		}
	})
	return rt
}

// run runs a session of ident over rt until it ends, sending steps in turn
// each time it waits.
func (rt *refRuntime) run(ident orchestrator.Identity, steps ...step) (string, error) {
	rt.t.Helper()
	return rt.runFrom(orchestrator.NewState(rt.id, ident, nil, nil, nil), steps...)
}

// runFrom is run from state st.
func (rt *refRuntime) runFrom(st *orchestrator.State, steps ...step) (string, error) {
	rt.t.Helper()
	rt.state, rt.steps = st, steps
	return orchestrator.Run(rt, st)
}

// send queues w, as a client that sends it while a step runs.
func (rt *refRuntime) send(w session.Wake) {
	rt.mu.Lock()
	rt.queue = append(rt.queue, w)
	rt.mu.Unlock()
	select {
	case rt.notify <- struct{}{}:
	default:
	}
}

// durable are the events the session made that its log keeps, in order.
func (rt *refRuntime) durable() []session.Event {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(rt.events), session.Event.Live)
}

func (rt *refRuntime) emit(ev session.Event) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.events = append(rt.events, ev)
}

func (rt *refRuntime) harness() *tacklr.Harness {
	if rt.h == nil {
		opts := tacklr.BindTurn(rt.agent, string(rt.id), slices.Sorted(maps.Keys(rt.jobs)), nil)
		h, err := tacklr.NewHarness(rt.t.Context(), opts)
		if err != nil {
			rt.t.Fatal(err)
		}
		rt.h = h
	}
	return rt.h
}

// do runs fn and waits for it, or for a cancel in the mailbox, which stops
// it.
func (rt *refRuntime) do(fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancel(rt.t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	for {
		rt.mu.Lock()
		kept, _, ok := orchestrator.TakeCancel(rt.queue)
		rt.queue = kept
		rt.mu.Unlock()
		if ok {
			cancel()
			<-done
			return context.Canceled
		}
		select {
		case err := <-done:
			return err
		case <-rt.notify:
		}
	}
}

func (rt *refRuntime) Begin(orchestrator.TurnStart) telemetry.Carrier {
	var names []string
	for _, m := range rt.state.MCP {
		names = append(names, m.Name)
	}
	rt.mcp = append(rt.mcp, names)
	return nil
}

func (rt *refRuntime) End() {}

func (rt *refRuntime) Commit(base session.Revision, in tacklr.CommitIn) (session.Revision, error) {
	out, stop := tacklr.PipeEvents(rt.emit)
	rev, err := rt.snap.Commit(rt.t.Context(), rt.store, rt.id, rt.harness(), base, in, out)
	stop()
	if rt.snap.Stale() {
		rt.h.Close()
		rt.h, rt.snap = nil, adapter.Snapshot{}
	}
	return rev, err
}

func (rt *refRuntime) Infer() (tacklr.Reply, error) {
	h := rt.harness()
	var reply tacklr.Reply
	err := rt.do(func(ctx context.Context) error {
		if err := rt.snap.Sync(ctx, rt.store, rt.id, h); err != nil {
			return err
		}
		out, stop := tacklr.PipeEvents(rt.emit)
		defer stop()
		var err error
		reply, err = h.Infer(ctx, out)
		return err
	})
	return reply, err
}

func (rt *refRuntime) Tools(calls []tacklr.Call) []tacklr.Call {
	h := rt.harness()
	out := slices.Clone(calls)
	var finished sync.Map
	cancelled := rt.do(func(ctx context.Context) error {
		if err := rt.snap.Sync(ctx, rt.store, rt.id, h); err != nil {
			return err
		}
		events, stop := tacklr.PipeEvents(rt.emit)
		defer stop()
		var wg sync.WaitGroup
		for i := range calls {
			wg.Go(func() {
				c, err := adapter.RunCall(ctx, rt.store, rt.id, h, calls[i], events)
				if c.Err = cmp.Or(err, ctx.Err()); c.Err == nil {
					finished.Store(i, c)
				}
			})
		}
		wg.Wait()
		return nil
	})
	for i := range calls {
		if c, ok := finished.Load(i); ok {
			out[i] = c.(tacklr.Call)
		} else if cancelled != nil {
			out[i].Err = cancelled
		}
	}
	return out
}

func (rt *refRuntime) Emit(ev session.Event) { rt.emit(ev) }

func (rt *refRuntime) Job(name, task string) (string, error) {
	fn, ok := rt.jobs[name]
	if !ok {
		return "", fmt.Errorf("%w: job %s", tacklr.ErrNotFound, name)
	}
	var out string
	err := rt.do(func(ctx context.Context) error {
		var err error
		out, err = fn(ctx, task)
		return err
	})
	return out, err
}

// Next is the oldest queued wake. With block and none queued, it sends the
// test's next step; a session that waits once the steps ran out fails the
// test.
func (rt *refRuntime) Next(block bool) session.Wake {
	for {
		rt.mu.Lock()
		if len(rt.queue) > 0 {
			w := rt.queue[0]
			rt.queue = rt.queue[1:]
			rt.mu.Unlock()
			return w
		}
		rt.mu.Unlock()
		if !block {
			return session.Wake{}
		}
		if len(rt.steps) == 0 {
			rt.t.Fatalf("the session waits for a wake after the test's last; events %+v", rt.durable())
		}
		next := rt.steps[0]
		rt.steps = rt.steps[1:]
		for _, w := range next(rt) {
			rt.send(w)
		}
	}
}

func (rt *refRuntime) Start(tacklr.JobStart) error { return rt.startErr }

func (rt *refRuntime) Cancel(id session.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.cancelled = append(rt.cancelled, id)
}

// wakes is a step that sends ws.
func wakes(ws ...session.Wake) step {
	return func(*refRuntime) []session.Wake { return ws }
}

// answer is a step that answers the session's last interrupt with answer,
// then sends then.
func answer(answer string, then ...session.Wake) step {
	return func(rt *refRuntime) []session.Wake {
		id := ""
		for _, ev := range rt.durable() {
			if ev.Type == session.EventInterrupt {
				id = ev.MessageID
			}
		}
		return append([]session.Wake{resume(map[string][]byte{id: []byte(answer)})}, then...)
	}
}

func prompt(text string) session.Wake {
	return session.Wake{Kind: session.WakePrompt, Prompt: session.Prompt{Text: text}}
}

func resume(answers map[string][]byte) session.Wake {
	return session.Wake{Kind: session.WakeResume, Resume: session.Resume{Responses: answers}}
}

func jobEnd(id, output string, failed bool) session.Wake {
	return session.Wake{Kind: session.WakeJob, Job: session.JobEnd{ID: session.ID(id), Result: session.JobResult{Output: output, Failed: failed}}}
}

// wake is a wake of kind that carries nothing: a close, a cancel, a
// suspend.
func wake(kind string) session.Wake { return session.Wake{Kind: kind} }
