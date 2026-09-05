package inprocess

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/interrupt"
)

func (r *Runtime) childStatuses(p *sessionProc) []durable.SessionStatus {
	p.mu.Lock()
	ids := slices.Clone(p.children)
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

func (r *Runtime) harvestJobs(p *sessionProc) []durable.SessionStatus {
	rows := r.childStatuses(p)
	if len(rows) == 0 {
		return nil
	}
	live := make([]durable.SessionStatus, 0, len(rows))
	var jobs []*tacklr.Message
	var drop []durable.SessionID
	for _, st := range rows {
		if st.State != durable.SessionComplete && st.State != durable.SessionFailed {
			live = append(live, st)
			continue
		}
		jobs = append(jobs, adapter.ChildJobMessage(st))
		drop = append(drop, st.ID)
	}
	if len(jobs) == 0 {
		return live
	}
	p.mu.Lock()
	p.inbox.Push(jobs...)
	p.children = slices.DeleteFunc(p.children, func(c durable.SessionID) bool {
		return slices.Contains(drop, c)
	})
	p.mu.Unlock()
	return live
}

func (r *Runtime) drainInbox(ctx context.Context, p *sessionProc, eng tacklr.Engine, out chan tacklr.StreamEvent) (int, []durable.SessionStatus, error) {
	live := r.harvestJobs(p)
	msgs := p.inbox.Take()
	n, err := adapter.AbsorbAll(ctx, eng.AbsorbUser, msgs, out)
	return n, live, err
}

// sessionChildren is the nested-session childHost for one parent session.
type sessionChildren struct {
	r *Runtime
	p *sessionProc
}

func (s sessionChildren) SpawnChild(ctx context.Context, specialist, task, callID string) (string, error) {
	specialist, task, err := adapter.NormalizeSpawn(specialist, task)
	if err != nil {
		return "", err
	}
	childID := durable.ChildSessionID(s.p.id, specialist, callID)
	if s.r.ownsChild(s.p, childID) {
		return string(childID), nil
	}
	if task == "" {
		return "", fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
	}
	s.p.mu.Lock()
	agentID := s.p.agentID
	mcp := slices.Clone(s.p.mcp)
	mounts := slices.Clone(s.p.mounts)
	auth := s.p.auth
	parent := s.p.id
	s.p.mu.Unlock()
	id, err := s.r.CreateSession(ctx, durable.CreateSession{
		SessionID:  childID,
		Parent:     parent,
		AgentID:    agentID,
		Specialist: specialist,
		MCPServers: mcp,
		Mounts:     mounts,
	})
	if err != nil {
		if errors.Is(err, durable.ErrAgentNotFound) {
			return "", fmt.Errorf("%w: %w", tacklr.ErrNotFound, err)
		}
		return "", err
	}
	if err := ctx.Err(); err != nil {
		_ = s.r.Close(context.WithoutCancel(ctx), id)
		s.r.dropChild(s.p, id)
		return "", err
	}
	if err := s.r.Prompt(s.p.childCtx(), id, durable.Prompt{Text: task, Auth: auth}); err != nil {
		return "", err
	}
	return string(id), nil
}

func (s sessionChildren) Children() []tacklr.Child {
	rows := s.r.childStatuses(s.p)
	out := make([]tacklr.Child, 0, len(rows))
	for _, st := range rows {
		out = append(out, childFromStatus(st))
	}
	return out
}

func (s sessionChildren) CancelChild(ctx context.Context, id string) error {
	sid := durable.SessionID(id)
	if !s.r.ownsChild(s.p, sid) {
		return adapter.UnknownChild(id)
	}
	_ = s.r.Close(ctx, sid)
	s.r.dropChild(s.p, sid)
	return nil
}

func (s sessionChildren) AwaitChild(ctx context.Context, id, callID string) (tacklr.Child, error) {
	sid := durable.SessionID(id)
	if !s.r.ownsChild(s.p, sid) {
		return tacklr.Child{}, adapter.UnknownChild(id)
	}
	return s.r.waitChild(ctx, s.p, sid, callID)
}

func childFromStatus(st durable.SessionStatus) tacklr.Child {
	c := tacklr.Child{ID: string(st.ID), Specialist: st.Specialist, State: adapter.ChildState(st.State), Result: st.Result}
	if st.State == durable.SessionFailed && c.Result == "" && st.Err != nil {
		c.Result = st.Err.Error()
	}
	return c
}

func (r *Runtime) waitChild(ctx context.Context, p *sessionProc, child durable.SessionID, callID string) (tacklr.Child, error) {
	for {
		if err := ctx.Err(); err != nil {
			return tacklr.Child{}, err
		}
		st, err := r.Status(ctx, child)
		if err != nil {
			return tacklr.Child{}, err
		}
		switch st.State {
		case durable.SessionComplete, durable.SessionFailed:
			r.dropChild(p, child)
			c := childFromStatus(st)
			if st.State == durable.SessionFailed {
				err = st.Err
				if err == nil {
					err = fmt.Errorf("failed: %w", tacklr.ErrFailed)
				}
				return c, err
			}
			return c, nil
		}
		if st.Waiting {
			p.mu.Lock()
			p.childParks[callID] = child
			p.mu.Unlock()
			return tacklr.Child{}, &interrupt.ChildWaiting{Kind: interrupt.TypeChildWaiting}
		}
		cp, err := r.get(child)
		if err != nil {
			return tacklr.Child{}, err
		}
		cp.mu.Lock()
		done := cp.turnDone
		cp.mu.Unlock()
		if done == nil {
			<-ctx.Done()
			return tacklr.Child{}, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return tacklr.Child{}, ctx.Err()
		case <-done:
		}
	}
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
	parks := maps.Clone(p.childParks)
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
