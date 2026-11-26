package tacklr_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// jobs is a harness of session "jobs" with the researcher specialist, the
// ci job and a Brain with no Episode kind, whose build tool schedules a
// job and twice schedules ci twice in one call.
func jobs(t *testing.T) *tacklr.Harness {
	t.Helper()
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
	return h
}

// runJobCall runs c on h with the session's running jobs and how many it
// may run at once.
func runJobCall(t *testing.T, h *tacklr.Harness, c tacklr.Call, running []string, most int) tacklr.Call {
	t.Helper()
	c.Jobs, c.MaxJobs = running, most
	c, err := h.RunCall(t.Context(), c, newStream(t).out)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

var (
	researcher = tacklr.ChildID("jobs", "researcher", "sp1", true)
	ciJob      = tacklr.ChildID("jobs", "ci", "b1", false)
)

// TestSpawnSpecialist_withoutBlockStartsAJobWhoseResultArrivesLater: a
// spawn that does not block starts the specialist and is done at once.
func TestSpawnSpecialist_withoutBlockStartsAJobWhoseResultArrivesLater(t *testing.T) {
	t.Parallel()
	h := jobs(t)

	got := runJobCall(t, h, tacklr.NewCall(call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"look","block":false}`), 0), nil, 0)

	if !got.Done || !strings.Contains(got.Body.Text(), "Job "+researcher+" scheduled") || len(got.Starts) != 1 || !got.Starts[0].Specialist {
		t.Fatalf("spawn = %+v, want a started specialist job", got)
	}
}

// TestSpawnSpecialist_aBlockingSpawnWaitsOnItsSpecialist: a spawn that
// blocks waits on the job it starts.
func TestSpawnSpecialist_aBlockingSpawnWaitsOnItsSpecialist(t *testing.T) {
	t.Parallel()
	h := jobs(t)

	got := runJobCall(t, h, tacklr.NewCall(call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"look"}`), 0), nil, 0)

	if !got.Waits() || got.Await != researcher {
		t.Fatalf("spawn = %+v, want it to wait on %s", got, researcher)
	}
}

// TestListChildren_listsTheSessionsRunningJobs: list_children says there
// are none, or lists each running job.
func TestListChildren_listsTheSessionsRunningJobs(t *testing.T) {
	t.Parallel()
	h := jobs(t)

	none := runJobCall(t, h, tacklr.NewCall(call("l1", "list_children", "{}"), 0), nil, 0).Body.Text()
	listed := runJobCall(t, h, tacklr.NewCall(call("l2", "list_children", "{}"), 0), []string{researcher}, 0).Body.Text()

	if none != "No jobs." || !strings.Contains(listed, "id="+researcher) {
		t.Fatalf("list = %q, then %q; want no jobs, then the running job", none, listed)
	}
}

// TestCancelChild_stopsARunningJob: cancel_child stops the job it names.
func TestCancelChild_stopsARunningJob(t *testing.T) {
	t.Parallel()
	h := jobs(t)

	got := runJobCall(t, h, tacklr.NewCall(call("x1", "cancel_child", `{"child_id":"`+researcher+`"}`), 0), []string{researcher}, 0)

	if !slices.Equal(got.Cancels, []string{researcher}) {
		t.Fatalf("cancel = %+v, want %s stopped", got.Cancels, researcher)
	}
}

// TestSchedule_startsAJobOnceWithinTheSessionsLimit: a tool's Schedule
// starts a job; one that already runs is not started again, two of one
// call start it once and Jobs lists it, and a session at its limit
// refuses another.
func TestSchedule_startsAJobOnceWithinTheSessionsLimit(t *testing.T) {
	t.Parallel()
	h := jobs(t)
	build := func(id string) tacklr.Call {
		return tacklr.NewCall(call(id, "build", `{"name":"ci","task":"test"}`), 0)
	}

	scheduled := runJobCall(t, h, build("b1"), nil, 0)
	again := runJobCall(t, h, build("b1"), []string{ciJob}, 1)
	twice := runJobCall(t, h, tacklr.NewCall(call("t1", "twice", "{}"), 0), nil, 0)
	full := runJobCall(t, h, build("b2"), []string{ciJob}, 1)

	if scheduled.Body.Text() != ciJob || len(scheduled.Starts) != 1 || scheduled.Starts[0].Task != "test" || again.Body.Text() != ciJob || len(again.Starts) != 0 {
		t.Fatalf("schedule %+v, again while it runs %+v; want one start", scheduled, again)
	}
	if want := tacklr.ChildID("jobs", "ci", "t1", false); twice.Body.Text() != want || len(twice.Starts) != 1 {
		t.Fatalf("schedule twice in one call = %+v, want one start of %s that Jobs lists", twice, want)
	}
	if !strings.Contains(full.Body.Text(), "already runs 1 jobs") {
		t.Fatalf("schedule at the limit = %q, want it refused", full.Body.Text())
	}
}

// TestRunCall_aFinishedJobsOutputIsItsCallsResult: a finished job's
// output, or a failed one's, is its call's result, even when the Brain
// cannot keep it as an Episode.
func TestRunCall_aFinishedJobsOutputIsItsCallsResult(t *testing.T) {
	t.Parallel()
	h := jobs(t)

	found := runJobCall(t, h, tacklr.Call{Tool: call("sp2", "spawn_specialist", "{}"), JobEnd: &session.JobResult{Output: "found it"}}, nil, 0)
	broke := runJobCall(t, h, tacklr.Call{Tool: call("sp3", "spawn_specialist", "{}"), JobEnd: &session.JobResult{Output: "it broke", Failed: true}}, nil, 0)

	if found.Body.Text() != "found it" || broke.Body.Text() != "it broke" {
		t.Fatalf("job ends = %q, %q; want each output as the result", found.Body.Text(), broke.Body.Text())
	}
}

// TestJobTools_aBadRequestIsTheModelsToCorrect: a spawn with no task, no
// specialist or an unknown one, a cancel with no id or an unknown one, and
// a Schedule a job cannot start name what is wrong.
func TestJobTools_aBadRequestIsTheModelsToCorrect(t *testing.T) {
	t.Parallel()
	h := jobs(t)
	for want, tc := range map[string]message.ToolCall{
		"task_description_and_context is required": call("s", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":" "}`),
		"specialist is required":                   call("s", "spawn_specialist", `{"specialist":"","task_description_and_context":"x"}`),
		"that specialist is not registered":        call("s", "spawn_specialist", `{"specialist":"nobody","task_description_and_context":"x"}`),
		"child_id is required":                     call("x", "cancel_child", `{"child_id":""}`),
		"that job is unknown":                      call("x", "cancel_child", `{"child_id":"job-1"}`),
		"nightly is not a registered job":          call("b", "build", `{"name":"nightly","task":"x"}`),
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			got := runJobCall(t, h, tacklr.NewCall(tc, 0), nil, 0).Body.Text()

			if !strings.Contains(got, want) {
				t.Fatalf("result = %q, want %q", got, want)
			}
		})
	}
}

// TestJobTools_aBlockingSpecialistsResultIsKeptAsAnEpisode: with the
// Episode kinds in the brain's catalog, a blocking specialist's result,
// once its round commits, is an Episode that later sessions can find.
func TestJobTools_aBlockingSpecialistsResultIsKeptAsAnEpisode(t *testing.T) {
	t.Parallel()
	// Arrange
	const token = "unique-episode-token"
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(tacklr.EpisodeKinds()...))
	if err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", "acme")
	model, _ := scripted(t)
	opts := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Brain: eng, SearchNamespace: ns,
		Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}}}
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(opts, "episodes", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	prompt(t, h, "go")
	r := tacklr.NewRound(reply(call("sp1", "spawn_specialist", `{"specialist":"researcher","task_description_and_context":"look"}`)))
	r.Calls[0], err = h.RunCall(t.Context(), tacklr.Call{Tool: r.Calls[0].Tool, JobEnd: &session.JobResult{Output: token}}, newStream(t).out)
	if err != nil {
		t.Fatal(err)
	}

	// Act
	commitErr := h.Commit(t.Context(), tacklr.CommitIn{Round: r}, newStream(t).out)

	// Assert
	page, err := eng.Search(t.Context(), brain.Scope{Namespace: ns}, brain.SearchRequest{Query: token, Kind: tacklr.KindEpisode})
	if commitErr != nil || err != nil {
		t.Fatal(commitErr, err)
	}
	hits := slices.Collect(page.Results.Objects())
	if len(hits) != 1 {
		t.Fatalf("episodes %+v; want the specialist's result kept", hits)
	}
	if kept, err := eng.Get(t.Context(), brain.Scope{Namespace: ns}, hits[0].ID); err != nil || kept.Content != token {
		t.Fatalf("episode %+v %v; want the specialist's result", kept, err)
	}
}
