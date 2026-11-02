package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/ryanaldo34/tacklr/brain"
)

// HarnessRuntime is what a tool call's tool gets: progress events, the
// session's user state, Park, and the session's Runtime jobs. Each call of
// a reply gets its own, so calls run at once without sharing state. A call
// reads its own writes first, then the committed state; it writes only
// here, and Commit applies its writes in call order. A tool may use it from
// goroutines. Only the harness makes one.
type HarnessRuntime struct {
	out     chan StreamEvent
	host    JobHost
	session *sessionManager
	mu      sync.Mutex
	// call is the call with its park and the OnCall layers it completed.
	call   roundCall
	writes callWrites
	// user is the call's own user-state values by key, as its tools set
	// them; a deleted key maps to deleted.
	user    map[string]userValue
	starts  []JobStart
	cancels []string
	await   string
}

type userValue struct {
	value   any
	deleted bool
}

// newHarnessRuntime opens call's run on sm. prior is what the call's
// earlier runs wrote (before a park), which this run reads and keeps.
func newHarnessRuntime(out chan StreamEvent, host JobHost, sm *sessionManager, call roundCall, prior callWrites) *HarnessRuntime {
	rt := &HarnessRuntime{out: out, host: host, session: sm, call: call, writes: prior, user: map[string]userValue{}}
	for _, w := range prior.State {
		v := userValue{deleted: w.Deleted}
		// The value is JSON a StateSet of this call encoded.
		_ = json.Unmarshal(w.Value, &v.value)
		rt.user[w.Key] = v
	}
	return rt
}

// CurrentToolCallID is this call's id. A runtime may run a call's step
// again when it did not record the step's result (a worker died), so a
// tool with external side effects (a write, a send, a charge) passes the
// id as its idempotency key.
func (rt *HarnessRuntime) CurrentToolCallID() string {
	return rt.call.Call.Key()
}

// EmitUpdate sends a non-blocking progress update for the call.
func (rt *HarnessRuntime) EmitUpdate(message string) {
	select {
	case rt.out <- StreamEvent{Type: StreamEventToolUpdate, Content: message, MessageID: rt.CurrentToolCallID()}:
	default:
	}
}

// StateGet reads the call's own write of key first, then committed state.
func (rt *HarnessRuntime) StateGet(key string) (any, bool) {
	rt.mu.Lock()
	v, ok := rt.user[key]
	rt.mu.Unlock()
	if ok {
		return v.value, !v.deleted
	}
	return rt.session.state(key)
}

// StateSet records a write of key on the call; Commit applies it.
func (rt *HarnessRuntime) StateSet(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("state %q: %w", key, err)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.user[key] = userValue{value: value}
	rt.writes.State = append(rt.writes.State, stateWrite{Key: key, Value: raw})
	return nil
}

// StateDelete records a delete of key on the call; Commit applies it.
func (rt *HarnessRuntime) StateDelete(key string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.user[key] = userValue{deleted: true}
	rt.writes.State = append(rt.writes.State, stateWrite{Key: key, Deleted: true})
}

// Park parks the call on an interrupt of kind and returns it as the error
// the tool propagates. The call's run after resume gets the answered
// interrupt and a nil error.
func (rt *HarnessRuntime) Park(kind string, payload []byte) (Interrupt, error) {
	if answered, ok := rt.takeAnswer(kind); ok {
		return answered, nil
	}
	intr, ok := NewInterrupt(kind)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid interrupt type", kind)
	}
	if init, ok := intr.(PayloadInitializer); ok {
		_ = init.InitFromPayload(payload)
	}
	return nil, rt.park(intr)
}

// park makes intr the call's park and returns it as the error tools
// propagate.
func (rt *HarnessRuntime) park(intr Interrupt) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.call.Park = &callPark{Interrupt: intr}
	return intr
}

// resume answers the call's park through Interrupt.Return. An invalid
// payload leaves it unanswered.
func (rt *HarnessRuntime) resume(payload []byte) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	p := rt.call.Park
	if p == nil || p.Answered {
		return fmt.Errorf("interrupt %q: %w", rt.call.Call.Key(), ErrInterruptNotFound)
	}
	if err := p.Interrupt.Return(payload); err != nil {
		return err
	}
	p.Answered = true
	return nil
}

// takeAnswer removes and returns the call's answered park of typeName.
func (rt *HarnessRuntime) takeAnswer(typeName string) (Interrupt, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	p := rt.call.Park
	if p == nil || !p.Answered || p.Interrupt.TypeName() != typeName {
		return nil, false
	}
	rt.call.Park = nil
	return p.Interrupt, true
}

// layer is the call's completed OnCall layer of typeName.
func (rt *HarnessRuntime) layer(typeName string) (onCallLayer, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.call.layer(typeName)
}

// recordLayer keeps a completed OnCall layer, so the call's run after a
// later park skips it.
func (rt *HarnessRuntime) recordLayer(l onCallLayer) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.call.Layers = append(rt.call.Layers, l)
}

// decision is the permission answer for the tool qualified-named name: the
// call's own answer, else the committed one.
func (rt *HarnessRuntime) decision(name string) permissionDecision {
	rt.mu.Lock()
	for i := len(rt.writes.Permissions) - 1; i >= 0; i-- {
		if w := rt.writes.Permissions[i]; w.Tool == name {
			rt.mu.Unlock()
			return w.Decision
		}
	}
	rt.mu.Unlock()
	return rt.session.Permissions.Decision(name)
}

// rememberAnswer keeps the always-answer a resolved permission prompt
// carries; a once-answer is not remembered.
func (rt *HarnessRuntime) rememberAnswer(perm *ToolPermissionInterrupt) {
	var d permissionDecision
	switch perm.SelectedKind {
	case PermissionAllowAlways:
		d = permAllowAlways
	case PermissionRejectAlways:
		d = permDenyAlways
	default:
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.writes.Permissions = append(rt.writes.Permissions, permissionWrite{Tool: perm.ToolName, Decision: d})
}

// rest is the ranked remainder of the last call of tool: the call's own,
// else the committed one.
func (rt *HarnessRuntime) rest(tool string) brain.Rest {
	rt.mu.Lock()
	for i := len(rt.writes.Rests) - 1; i >= 0; i-- {
		if w := rt.writes.Rests[i]; w.Key == tool {
			rt.mu.Unlock()
			return w.Rest
		}
	}
	rt.mu.Unlock()
	return rt.session.Search.Rest(tool)
}

func (rt *HarnessRuntime) setRest(tool string, rest brain.Rest) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.writes.Rests = append(rt.writes.Rests, restWrite{Key: tool, Rest: rest})
}

// plan is the plan the call sees: the committed plan with the call's own
// plan changes on top.
func (rt *HarnessRuntime) plan() ([]Todo, string) {
	todos, doc := rt.session.Plan.snapshot()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, op := range rt.writes.Plan {
		if next, nextDoc, _, err := op.change()(todos, doc); err == nil {
			todos, doc = next, nextDoc
		}
	}
	return todos, doc
}

// changePlan applies a plan tool's change to the plan the call sees and
// keeps it for Commit, which replays it on the committed plan.
func (rt *HarnessRuntime) changePlan(op planOp) (ToolOutcome, error) {
	todos, doc := rt.plan()
	_, _, out, err := op.change()(todos, doc)
	if err != nil {
		return ToolOutcome{}, err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.writes.Plan = append(rt.writes.Plan, op)
	return out, nil
}

// Schedule starts a job of this session: a Runtime job worker (a shell
// pipeline, a long data job). Specialists are not scheduled by tools: the
// model starts them with spawn_specialist. With JobRequest.Wait the call
// waits and the job's output is its result; otherwise the output arrives
// later as an inbox message. Pass the id to Jobs or CancelJob.
func (rt *HarnessRuntime) Schedule(_ context.Context, job JobRequest) (Job, error) {
	return rt.startJob(job, false)
}

// startJob starts the child job names, once: a call that runs again names
// the same child. specialist says which kind job must name; only
// spawn_specialist starts specialists. With Wait the call waits for it and
// the child's output is the call's result.
func (rt *HarnessRuntime) startJob(job JobRequest, specialist bool) (Job, error) {
	if rt.host == nil {
		return Job{}, fmt.Errorf("jobs are not available: %w", ErrFailed)
	}
	s, err := rt.host.Resolve(job, rt.CurrentToolCallID())
	if err != nil {
		return Job{}, err
	}
	switch {
	case s.Specialist && !specialist:
		return Job{}, fmt.Errorf("%w: %s is a specialist; the model starts specialists with spawn_specialist", ErrInvalid, s.Name)
	case !s.Specialist && specialist:
		return Job{}, fmt.Errorf("%w: %s is not a specialist", ErrNotFound, s.Name)
	}
	running := slices.ContainsFunc(rt.host.Jobs(), func(j Job) bool { return j.ID == s.ID })
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !running && !slices.ContainsFunc(rt.starts, func(o JobStart) bool { return o.ID == s.ID }) {
		rt.starts = append(rt.starts, s)
	}
	if job.Wait {
		rt.await = s.ID
	}
	return Job{ID: s.ID, Name: s.Name, State: JobRunning}, nil
}

// Jobs is the session's running children and the ones this call started
// (a call records a start only for a child that is not running yet).
// Waiting specialist children appear as running.
func (rt *HarnessRuntime) Jobs() []Job {
	var out []Job
	if rt.host != nil {
		out = rt.host.Jobs()
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, s := range rt.starts {
		out = append(out, Job{ID: s.ID, Name: s.Name, State: JobRunning})
	}
	return out
}

// CancelJob stops one job of this session and drops it from Jobs.
func (rt *HarnessRuntime) CancelJob(_ context.Context, id string) error {
	if rt.host == nil {
		return fmt.Errorf("jobs are not available: %w", ErrFailed)
	}
	if !slices.ContainsFunc(rt.Jobs(), func(j Job) bool { return j.ID == id }) {
		return fmt.Errorf("job %q is unknown: %w", id, ErrNotFound)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.cancels = append(rt.cancels, id)
	return nil
}
