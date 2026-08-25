package inprocess

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/streaming"
)

func (r *Runtime) noteOutcome(p *sessionProc, o turnOutcome) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch o {
	case turnComplete:
		p.terminal = durable.SessionComplete
		p.yielded = false
	case turnError, turnCancelled:
		p.terminal = durable.SessionFailed
		p.yielded = false
	case turnYield:
		p.yielded = true
		p.terminal = ""
	}
}

func (r *Runtime) childStatuses(p *sessionProc) []durable.SessionStatus {
	p.mu.Lock()
	ids := append([]durable.SessionID(nil), p.children...)
	p.mu.Unlock()
	out := make([]durable.SessionStatus, 0, len(ids))
	for _, id := range ids {
		st, err := r.Status(context.Background(), id)
		if err != nil {
			continue
		}
		out = append(out, st)
	}
	return out
}

func (r *Runtime) runSessionTool(ctx context.Context, p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent) (drive.ToolStep, error) {
	switch tc.Name {
	case tacklr.SpawnSpecialistName:
		return r.spawnChild(ctx, p, eng, tc, out)
	case tacklr.ListChildrenName:
		return r.commitSessionTool(eng, tc, out, durable.FormatChildList(r.childStatuses(p)), nil)
	case tacklr.GetChildName:
		return r.getChild(ctx, p, eng, tc, out)
	case tacklr.CancelChildName:
		return r.cancelChild(ctx, p, eng, tc, out)
	default:
		return eng.RunToolCall(ctx, tc, out)
	}
}

func (r *Runtime) spawnChild(ctx context.Context, p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent) (drive.ToolStep, error) {
	call, err := durable.ParseSpawnCall(tc.Arguments)
	if err != nil {
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	childID := durable.ChildSessionID(p.id, call.Specialist, tc.Key())
	if r.ownsChild(p, childID) {
		if !call.Block {
			st, err := r.Status(ctx, childID)
			if err != nil {
				return r.commitSessionTool(eng, tc, out, err.Error(), err)
			}
			return r.commitSessionTool(eng, tc, out, durable.FormatChild(st), nil)
		}
		return r.waitChild(ctx, p, eng, tc, out, childID)
	}
	id, err := r.CreateSession(ctx, durable.CreateSession{
		SessionID:  childID,
		Parent:     p.id,
		AgentID:    p.agentID,
		Specialist: call.Specialist,
		MCPServers: p.mcp,
		Mounts:     p.mounts,
	})
	if err != nil {
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	if err := ctx.Err(); err != nil {
		_ = r.Close(context.WithoutCancel(ctx), id)
		r.dropChild(p, id)
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	// Child turns must not share the parent turn context: parent park/complete
	// cancels that context, and parked children have to keep running. Client
	// stop uses Runtime.Cancel / the original Prompt context, which Close the
	// children explicitly.
	if err := r.Prompt(context.WithoutCancel(ctx), id, durable.Prompt{Text: call.Task, Auth: p.auth}); err != nil {
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	if !call.Block {
		msg := durable.ScheduledChildMessage(id, call.Specialist)
		return r.commitSessionTool(eng, tc, out, msg, nil)
	}
	return r.waitChild(ctx, p, eng, tc, out, id)
}

func (r *Runtime) getChild(ctx context.Context, p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent) (drive.ToolStep, error) {
	jobID, block, err := durable.ParseChildCall(tc.Arguments)
	if err != nil {
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	id := durable.SessionID(jobID)
	if !r.ownsChild(p, id) {
		return r.commitSessionTool(eng, tc, out, fmt.Sprintf("job %q is unknown; call list_children and use an id from that list", jobID), durable.ErrSessionNotFound)
	}
	if !block {
		st, err := r.Status(ctx, id)
		if err != nil {
			return r.commitSessionTool(eng, tc, out, err.Error(), err)
		}
		if st.State == durable.SessionComplete {
			_ = r.Close(ctx, id)
			r.dropChild(p, id)
			return r.commitSessionTool(eng, tc, out, st.Result, nil)
		}
		return r.commitSessionTool(eng, tc, out, durable.FormatChild(st), nil)
	}
	return r.waitChild(ctx, p, eng, tc, out, id)
}

func (r *Runtime) cancelChild(ctx context.Context, p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent) (drive.ToolStep, error) {
	jobID, _, err := durable.ParseChildCall(tc.Arguments)
	if err != nil {
		return r.commitSessionTool(eng, tc, out, err.Error(), err)
	}
	id := durable.SessionID(jobID)
	if !r.ownsChild(p, id) {
		return r.commitSessionTool(eng, tc, out, fmt.Sprintf("job %q is unknown; call list_children and use an id from that list", jobID), durable.ErrSessionNotFound)
	}
	_ = r.Close(ctx, id)
	r.dropChild(p, id)
	msg := fmt.Sprintf("Child %s cancelled and removed.", id)
	return r.commitSessionTool(eng, tc, out, msg, nil)
}

func (r *Runtime) waitChild(ctx context.Context, p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent, child durable.SessionID) (drive.ToolStep, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		if err := ctx.Err(); err != nil {
			return drive.ToolStep{}, err
		}
		if time.Now().After(deadline) {
			return r.commitSessionTool(eng, tc, out, "job wait timed out", ctx.Err())
		}
		st, err := r.Status(ctx, child)
		if err != nil {
			return r.commitSessionTool(eng, tc, out, err.Error(), err)
		}
		switch st.State {
		case durable.SessionComplete:
			_ = r.Close(ctx, child)
			r.dropChild(p, child)
			return r.commitSessionTool(eng, tc, out, st.Result, nil)
		case durable.SessionFailed:
			r.dropChild(p, child)
			msg := "failed"
			if st.Err != nil {
				msg = st.Err.Error()
			}
			return r.commitSessionTool(eng, tc, out, msg, st.Err)
		}
		if st.Waiting {
			return r.parkOnChild(p, eng, tc, out, child)
		}
		select {
		case <-ctx.Done():
			return drive.ToolStep{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (r *Runtime) parkOnChild(p *sessionProc, eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent, child durable.SessionID) (drive.ToolStep, error) {
	key := tc.Key()
	p.mu.Lock()
	if p.childParks == nil {
		p.childParks = make(map[string]durable.SessionID)
	}
	p.childParks[key] = child
	p.mu.Unlock()
	if err := eng.ParkTool(tc); err != nil {
		return drive.ToolStep{}, err
	}
	payload, _ := json.Marshal(map[string]any{
		"interruptId": key,
		"type":        "child_waiting",
		"data":        json.RawMessage(`{}`),
	})
	out <- streaming.StreamEvent{Type: streaming.StreamEventInterrupt, MessageID: key, Data: payload}
	return drive.ToolStep{Interrupted: true, InterruptID: key, InterruptData: payload}, nil
}

func (r *Runtime) commitSessionTool(eng drive.Engine, tc streaming.ToolCall, out chan streaming.StreamEvent, output string, err error) (drive.ToolStep, error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	presented := tc
	presented.Status = status
	out <- streaming.StreamEvent{
		Type:      streaming.StreamEventToolResult,
		MessageID: tc.Key(),
		Content:   output,
		ToolCalls: []streaming.ToolCall{presented},
	}
	eng.RecordToolResult(tc, output)
	return drive.ToolStep{}, err
}

func (r *Runtime) ownsChild(p *sessionProc, id durable.SessionID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Contains(p.children, id)
}

func (r *Runtime) dropChild(p *sessionProc, id durable.SessionID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.children = slices.DeleteFunc(p.children, func(c durable.SessionID) bool { return c == id })
}

func (r *Runtime) forwardChildResume(ctx context.Context, p *sessionProc, resume durable.Resume) {
	p.mu.Lock()
	parks := make(map[string]durable.SessionID, len(p.childParks))
	for k, v := range p.childParks {
		parks[k] = v
	}
	p.mu.Unlock()
	for callID, payload := range resume.Responses {
		childID, ok := parks[callID]
		if !ok {
			continue
		}
		mapped := map[string][]byte{callID: payload}
		if snap, _, err := r.snapshots.Load(context.Background(), childID); err == nil {
			pending := snap.Checkpoint.PendingToolCalls()
			ids := make(map[string][]byte, len(pending))
			for k, ptc := range pending {
				if ptc.InterruptActive {
					ids[k] = payload
				}
			}
			if len(ids) > 0 {
				mapped = ids
			}
		}
		_ = r.Resume(ctx, childID, durable.Resume{Responses: mapped, Auth: resume.Auth})
		p.mu.Lock()
		delete(p.childParks, callID)
		p.mu.Unlock()
	}
}
