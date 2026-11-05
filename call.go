package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/brain"
)

// CallContext is what a tool call's tool gets: progress events, the
// session's user state, Park, and the session's Runtime jobs. Each call of
// a reply gets its own, so calls run at once without sharing state. A call
// reads its own writes first, then the committed state; it writes only
// here, and Commit applies its writes in call order. A tool may use it from
// goroutines. Only the harness makes one.
type CallContext struct {
	out     chan session.Event
	turn    *Harness
	session *sessionManager
	// running are the session's running jobs, and maxChildren how many it
	// may run at once (zero: no limit).
	running     []string
	maxChildren int
	mu          sync.Mutex
	// call is the call the run changes: its body (park, layers, writes),
	// the jobs it starts and cancels, and the job it waits on.
	call Call
	// user is the call's own user-state values by key, as its tools set
	// them; a deleted key maps to deleted.
	user map[string]userValue
}

type userValue struct {
	value   any
	deleted bool
}

// newCallContext opens a run of c on turn. It reads and keeps what c's
// earlier runs wrote (before a park).
func newCallContext(out chan session.Event, turn *Harness, c Call, running []string, maxChildren int) *CallContext {
	cc := &CallContext{out: out, turn: turn, session: turn.session, running: running, maxChildren: maxChildren, call: c, user: map[string]userValue{}}
	for _, w := range c.Body.writes.State {
		v := userValue{deleted: w.Deleted}
		// The value is JSON a StateSet of this call encoded.
		_ = json.Unmarshal(w.Value, &v.value)
		cc.user[w.Key] = v
	}
	return cc
}

// CurrentToolCallID is this call's id. A runtime may run a call's step
// again when it did not record the step's result (a worker died), so a
// tool with external side effects (a write, a send, a charge) passes the
// id as its idempotency key.
func (cc *CallContext) CurrentToolCallID() string {
	return cc.call.Key()
}

// EmitUpdate sends a non-blocking progress update for the call.
func (cc *CallContext) EmitUpdate(message string) {
	select {
	case cc.out <- session.Event{Type: session.EventToolUpdate, Content: message, MessageID: cc.CurrentToolCallID()}:
	default:
	}
}

// StateGet reads the call's own write of key first, then committed state.
func (cc *CallContext) StateGet(key string) (any, bool) {
	cc.mu.Lock()
	v, ok := cc.user[key]
	cc.mu.Unlock()
	if ok {
		return v.value, !v.deleted
	}
	return cc.session.state(key)
}

// StateSet records a write of key on the call; Commit applies it.
func (cc *CallContext) StateSet(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("state %q: %w", key, err)
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.user[key] = userValue{value: value}
	cc.call.Body.writes.State = append(cc.call.Body.writes.State, stateWrite{Key: key, Value: raw})
	return nil
}

// StateDelete records a delete of key on the call; Commit applies it.
func (cc *CallContext) StateDelete(key string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.user[key] = userValue{deleted: true}
	cc.call.Body.writes.State = append(cc.call.Body.writes.State, stateWrite{Key: key, Deleted: true})
}

// Park parks the call on intr and returns it as the error the tool
// propagates. The call's run after resume gets the answered interrupt of
// intr's type and a nil error. intr's type must be registered
// (RegisterInterrupt), so a parked call can be read back.
func (cc *CallContext) Park(intr Interrupt) (Interrupt, error) {
	if answered, ok := cc.takeAnswer(intr.TypeName()); ok {
		return answered, nil
	}
	if _, ok := NewInterrupt(intr.TypeName()); !ok {
		return nil, fmt.Errorf("interrupt type %q is not registered: %w", intr.TypeName(), ErrInvalid)
	}
	return nil, cc.park(intr)
}

// park makes intr the call's park and returns it as the error tools
// propagate.
func (cc *CallContext) park(intr Interrupt) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Body.park = &callPark{Interrupt: intr}
	return intr
}

// resume answers the call's park through Interrupt.Return. An invalid
// payload leaves it unanswered.
func (cc *CallContext) resume(payload []byte) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	p := cc.call.Body.park
	if p == nil || p.Answered {
		return fmt.Errorf("interrupt %q: %w", cc.call.Key(), ErrInterruptNotFound)
	}
	if err := p.Interrupt.Return(payload); err != nil {
		return err
	}
	p.Answered = true
	return nil
}

// takeAnswer removes and returns the call's answered park of typeName.
func (cc *CallContext) takeAnswer(typeName string) (Interrupt, bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	p := cc.call.Body.park
	if p == nil || !p.Answered || p.Interrupt.TypeName() != typeName {
		return nil, false
	}
	cc.call.Body.park = nil
	return p.Interrupt, true
}

// layer is the call's completed OnCall layer of typeName.
func (cc *CallContext) layer(typeName string) (onCallLayer, bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.call.Body.layer(typeName)
}

// recordLayer keeps a completed OnCall layer, so the call's run after a
// later park skips it.
func (cc *CallContext) recordLayer(l onCallLayer) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Body.layers = append(cc.call.Body.layers, l)
}

// decision is the permission answer for the tool qualified-named name: the
// call's own answer, else the committed one.
func (cc *CallContext) decision(name string) permissionDecision {
	cc.mu.Lock()
	for i := len(cc.call.Body.writes.Permissions) - 1; i >= 0; i-- {
		if w := cc.call.Body.writes.Permissions[i]; w.Tool == name {
			cc.mu.Unlock()
			return w.Decision
		}
	}
	cc.mu.Unlock()
	return cc.session.Permissions.Decision(name)
}

// rememberAnswer keeps the always-answer a resolved permission prompt
// carries; a once-answer is not remembered.
func (cc *CallContext) rememberAnswer(perm *ToolPermissionInterrupt) {
	var d permissionDecision
	switch perm.SelectedKind {
	case PermissionAllowAlways:
		d = permAllowAlways
	case PermissionRejectAlways:
		d = permDenyAlways
	default:
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Body.writes.Permissions = append(cc.call.Body.writes.Permissions, permissionWrite{Tool: perm.ToolName, Decision: d})
}

// rest is the ranked remainder of the last call of tool: the call's own,
// else the committed one.
func (cc *CallContext) rest(tool string) brain.Rest {
	cc.mu.Lock()
	for i := len(cc.call.Body.writes.Rests) - 1; i >= 0; i-- {
		if w := cc.call.Body.writes.Rests[i]; w.Key == tool {
			cc.mu.Unlock()
			return w.Rest
		}
	}
	cc.mu.Unlock()
	return cc.session.Search.Rest(tool)
}

func (cc *CallContext) setRest(tool string, rest brain.Rest) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Body.writes.Rests = append(cc.call.Body.writes.Rests, restWrite{Key: tool, Rest: rest})
}

// plan is the plan the call sees: the committed plan with the call's own
// plan changes on top.
func (cc *CallContext) plan() ([]Todo, string) {
	todos, doc := cc.session.Plan.snapshot()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, op := range cc.call.Body.writes.Plan {
		if next, nextDoc, _, err := op.change()(todos, doc); err == nil {
			todos, doc = next, nextDoc
		}
	}
	return todos, doc
}

// changePlan applies a plan tool's change to the plan the call sees and
// keeps it for Commit, which replays it on the committed plan.
func (cc *CallContext) changePlan(op planOp) (ToolOutcome, error) {
	todos, doc := cc.plan()
	_, _, out, err := op.change()(todos, doc)
	if err != nil {
		return ToolOutcome{}, err
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Body.writes.Plan = append(cc.call.Body.writes.Plan, op)
	return out, nil
}

// Schedule starts a job of this session: a Runtime job worker (a shell
// pipeline, a long data job). Specialists are not scheduled by tools: the
// model starts them with spawn_specialist. With JobRequest.Wait the call
// waits and the job's output is its result; otherwise the output arrives
// later as an inbox message. Pass the id to Jobs or CancelJob.
func (cc *CallContext) Schedule(_ context.Context, job JobRequest) (Job, error) {
	return cc.startJob(job, false)
}

// startJob starts the child job names, once: a call that runs again names
// the same child. specialist says which kind job must name; only
// spawn_specialist starts specialists. With Wait the call waits for it and
// the child's output is the call's result.
func (cc *CallContext) startJob(job JobRequest, specialist bool) (Job, error) {
	name, task := strings.TrimSpace(job.Name), strings.TrimSpace(job.Task)
	isSpecialist := cc.turn.specialists[name] != nil
	switch {
	case name == "":
		return Job{}, fmt.Errorf("job name is required: %w", ErrInvalid)
	case isSpecialist && !specialist:
		return Job{}, fmt.Errorf("%w: %s is a specialist; the model starts specialists with spawn_specialist", ErrInvalid, name)
	case !isSpecialist && specialist:
		return Job{}, fmt.Errorf("%w: %s is not a specialist", ErrNotFound, name)
	case !specialist && !slices.Contains(cc.turn.jobs, name):
		return Job{}, fmt.Errorf("%w: %s is not a registered job", ErrNotFound, name)
	case task == "":
		return Job{}, fmt.Errorf("task is required: %w", ErrInvalid)
	}
	s := JobStart{ID: ChildID(cc.turn.sessionId, name, cc.CurrentToolCallID(), specialist), Name: name, Task: task, Specialist: specialist}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if !slices.Contains(cc.running, s.ID) && !slices.ContainsFunc(cc.call.Starts, func(o JobStart) bool { return o.ID == s.ID }) {
		if n := len(cc.running) + len(cc.call.Starts); cc.maxChildren > 0 && n >= cc.maxChildren {
			return Job{}, Correctionf(ErrInvalid, "%s: this session already runs %d jobs, the most it may run at once. Wait for one to finish, or stop one with cancel_child", name, n)
		}
		cc.call.Starts = append(cc.call.Starts, s)
	}
	if job.Wait {
		cc.call.Await = s.ID
	}
	return Job{ID: s.ID, Name: s.Name, State: JobRunning}, nil
}

// Jobs is the session's running children and the ones this call started
// (a call records a start only for a child that is not running yet).
// Waiting specialist children appear as running.
func (cc *CallContext) Jobs() []Job {
	out := make([]Job, 0, len(cc.running))
	for _, id := range cc.running {
		out = append(out, Job{ID: id, State: JobRunning})
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, s := range cc.call.Starts {
		out = append(out, Job{ID: s.ID, Name: s.Name, State: JobRunning})
	}
	return out
}

// CancelJob stops one job of this session and drops it from Jobs.
func (cc *CallContext) CancelJob(_ context.Context, id string) error {
	if !slices.ContainsFunc(cc.Jobs(), func(j Job) bool { return j.ID == id }) {
		return fmt.Errorf("job %q is unknown: %w", id, ErrNotFound)
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.call.Cancels = append(cc.call.Cancels, id)
	return nil
}
