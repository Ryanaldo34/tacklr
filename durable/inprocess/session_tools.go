package inprocess

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
)

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

// sessionChildren is the nested-session childHost for one parent session.
type sessionChildren struct {
	r *Runtime
	p *sessionProc
}

func (s sessionChildren) SpawnChild(ctx context.Context, specialist, task, callID string) (string, error) {
	specialist, task, err := durable.NormalizeSpawn(specialist, task)
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
	id, err := s.r.CreateSession(ctx, durable.CreateSession{
		SessionID:  childID,
		Parent:     s.p.id,
		AgentID:    s.p.agentID,
		Specialist: specialist,
		MCPServers: s.p.mcp,
		Mounts:     s.p.mounts,
	})
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		_ = s.r.Close(context.WithoutCancel(ctx), id)
		s.r.dropChild(s.p, id)
		return "", err
	}
	if err := s.r.Prompt(s.p.childCtx(), id, durable.Prompt{Text: task, Auth: s.p.auth}); err != nil {
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
		return durable.UnknownChild(id)
	}
	_ = s.r.Close(ctx, sid)
	s.r.dropChild(s.p, sid)
	return nil
}

func (s sessionChildren) AwaitChild(ctx context.Context, id, callID string) (tacklr.Child, bool, error) {
	sid := durable.SessionID(id)
	if !s.r.ownsChild(s.p, sid) {
		return tacklr.Child{}, false, durable.UnknownChild(id)
	}
	return s.r.waitChild(ctx, s.p, sid, callID)
}

func childFromStatus(st durable.SessionStatus) tacklr.Child {
	c := tacklr.Child{ID: string(st.ID), Specialist: st.Specialist, State: durable.ChildState(st.State), Result: st.Result}
	if st.State == durable.SessionFailed && c.Result == "" && st.Err != nil {
		c.Result = st.Err.Error()
	}
	return c
}

func (r *Runtime) waitChild(ctx context.Context, p *sessionProc, child durable.SessionID, callID string) (tacklr.Child, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return tacklr.Child{}, false, err
		}
		st, err := r.Status(ctx, child)
		if err != nil {
			return tacklr.Child{}, false, err
		}
		switch st.State {
		case durable.SessionComplete, durable.SessionFailed:
			_ = r.Close(ctx, child)
			r.dropChild(p, child)
			c := childFromStatus(st)
			if st.State == durable.SessionFailed {
				err = st.Err
				if err == nil {
					err = fmt.Errorf("failed: %w", tacklr.ErrFailed)
				}
				return c, false, err
			}
			return c, false, nil
		}
		if st.Waiting {
			p.mu.Lock()
			p.childParks[callID] = child
			p.mu.Unlock()
			return tacklr.Child{}, true, nil
		}
		cp, err := r.get(child)
		if err != nil {
			return tacklr.Child{}, false, err
		}
		cp.mu.Lock()
		done := cp.turnDone
		cp.mu.Unlock()
		if done == nil {
			<-ctx.Done()
			return tacklr.Child{}, false, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return tacklr.Child{}, false, ctx.Err()
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
