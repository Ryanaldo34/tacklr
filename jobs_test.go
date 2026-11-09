package tacklr_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/session"
)

// TestJobTools_startListAndCancelTheSessionsJobs: spawn_specialist starts a
// specialist the call waits on, or a job whose result arrives later; a
// tool's Schedule starts a job worker once, within the session's limit,
// and the tool's Jobs lists the job it started; list_children lists the running jobs and cancel_child stops one. A
// finished job's output is its call's result, even when the Brain cannot
// keep it as an Episode. Each bad request is the model's to correct.
func TestJobTools_startListAndCancelTheSessionsJobs(t *testing.T) {
	t.Parallel()
	build := tacklr.NewTool(tacklr.ToolConfig{Name: "build", Handler: func(ctx context.Context, args struct {
		Name string `json:"name"`
		Task string `json:"task"`
	}, cc *tacklr.CallContext) (string, error) {
		job, err := cc.Schedule(ctx, tacklr.JobRequest{Name: args.Name, Task: args.Task})
		return job.ID, err
	}})
	twice := tacklr.NewTool(tacklr.ToolConfig{Name: "twice", Handler: func(ctx context.Context, cc *tacklr.CallContext) (string, error) {
		for range 2 {
			if _, err := cc.Schedule(ctx, tacklr.JobRequest{Name: "ci", Task: "test"}); err != nil {
				return "", err
			}
		}
		var ids []string
		for _, j := range cc.Jobs() {
			ids = append(ids, j.ID)
		}
		return strings.Join(ids, ","), nil
	}})
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{Kind: "Fact", IsParent: true}))
	if err != nil {
		t.Fatal(err)
	}
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{build, twice}, Brain: eng, SearchNamespace: storetest.Namespace(t, "org", "acme"),
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(opts, "jobs", []string{"ci"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	prompt(t, h, "go")
	run := func(c tacklr.Call) tacklr.Call {
		t.Helper()
		c, err := h.RunCall(t.Context(), c, newStream(t).out)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	with := func(c tacklr.Call, jobs []string, most int) tacklr.Call {
		c.Jobs, c.MaxJobs = jobs, most
		return c
	}
	spawn := func(id, args string) tacklr.Call { return tacklr.NewCall(call(id, "spawn_specialist", args), 0) }
	worker := tacklr.ChildID("jobs", "researcher", "sp1", true)
	ci := tacklr.ChildID("jobs", "ci", "b1", false)

	none := run(tacklr.NewCall(call("l1", "list_children", "{}"), 0)).Body.Text()
	later := run(spawn("sp1", `{"specialist":"researcher","task_description_and_context":"look","block":false}`))
	waits := run(spawn("sp1", `{"specialist":"researcher","task_description_and_context":"look"}`))
	listed := run(with(tacklr.NewCall(call("l2", "list_children", "{}"), 0), []string{worker}, 0)).Body.Text()
	cancelled := run(with(tacklr.NewCall(call("x1", "cancel_child", `{"child_id":"`+worker+`"}`), 0), []string{worker}, 0))
	scheduled := run(tacklr.NewCall(call("b1", "build", `{"name":"ci","task":"test"}`), 0))
	scheduledTwice := run(tacklr.NewCall(call("t1", "twice", "{}"), 0))
	running := run(with(tacklr.NewCall(call("b1", "build", `{"name":"ci","task":"test"}`), 0), []string{ci}, 1))
	r := tacklr.NewRound(reply(call("sp2", "spawn_specialist", "{}")))
	r.Calls[0] = run(tacklr.Call{Tool: r.Calls[0].Tool, JobEnd: &session.JobResult{Output: "found it"}})
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, newStream(t).out)
	failedJob := run(tacklr.Call{Tool: call("sp3", "spawn_specialist", "{}"), JobEnd: &session.JobResult{Output: "it broke", Failed: true}})
	bad := map[string]tacklr.Call{
		"task_description_and_context is required": spawn("s", `{"specialist":"researcher","task_description_and_context":" "}`),
		"specialist is required":                   spawn("s", `{"specialist":"","task_description_and_context":"x"}`),
		"that specialist is not registered":        spawn("s", `{"specialist":"nobody","task_description_and_context":"x"}`),
		"child_id is required":                     tacklr.NewCall(call("x", "cancel_child", `{"child_id":""}`), 0),
		"that job is unknown":                      tacklr.NewCall(call("x", "cancel_child", `{"child_id":"job-1"}`), 0),
		"already runs 1 jobs":                      with(tacklr.NewCall(call("b2", "build", `{"name":"ci","task":"test"}`), 0), []string{ci}, 1),
		"researcher is a specialist":               tacklr.NewCall(call("b", "build", `{"name":"researcher","task":"x"}`), 0),
		"nightly is not a registered job":          tacklr.NewCall(call("b", "build", `{"name":"nightly","task":"x"}`), 0),
		"job name is required":                     tacklr.NewCall(call("b", "build", `{"name":" ","task":"x"}`), 0),
		"task is required":                         tacklr.NewCall(call("b", "build", `{"name":"ci","task":""}`), 0),
	}

	if none != "No jobs." || !later.Done || !strings.Contains(later.Body.Text(), "Job "+worker+" scheduled") || len(later.Starts) != 1 || !later.Starts[0].Specialist {
		t.Fatalf("list %q, spawn later %+v; want no jobs, then a started specialist job", none, later)
	}
	if !waits.Waits() || waits.Await != worker || !strings.Contains(listed, "id="+worker) || !slices.Equal(cancelled.Cancels, []string{worker}) {
		t.Fatalf("spawn that waits %+v, list %q, cancel %+v; want the wait, the job listed, then stopped", waits, listed, cancelled.Cancels)
	}
	if scheduled.Body.Text() != ci || len(scheduled.Starts) != 1 || scheduled.Starts[0].Task != "test" || running.Body.Text() != ci || len(running.Starts) != 0 {
		t.Fatalf("schedule %+v, again while it runs %+v; want one start", scheduled, running)
	}
	if want := tacklr.ChildID("jobs", "ci", "t1", false); scheduledTwice.Body.Text() != want || len(scheduledTwice.Starts) != 1 {
		t.Fatalf("schedule twice in one call = %+v, want one start of %s that Jobs lists", scheduledTwice, want)
	}
	if commitErr != nil || r.Calls[0].Body.Text() != "found it" || failedJob.Body.Text() != "it broke" {
		t.Fatalf("job ends = %q (%v), %q; want each output as the result", r.Calls[0].Body.Text(), commitErr, failedJob.Body.Text())
	}
	for want, c := range bad {
		if got := run(c).Body.Text(); !strings.Contains(got, want) {
			t.Errorf("result = %q, want %q", got, want)
		}
	}
}
