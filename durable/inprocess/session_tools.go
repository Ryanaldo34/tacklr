package inprocess

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
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

func (r *Runtime) harvestJobs(p *sessionProc) int {
	rows := r.childStatuses(p)
	var steers []*tacklr.Message
	var drop []durable.SessionID
	live := 0
	for _, st := range rows {
		if st.State != durable.SessionComplete && st.State != durable.SessionFailed {
			live++
			continue
		}
		steers = append(steers, adapter.ChildJobMessage(st))
		drop = append(drop, st.ID)
	}
	p.mu.Lock()
	if len(drop) > 0 {
		p.children = slices.DeleteFunc(p.children, func(c durable.SessionID) bool {
			return slices.Contains(drop, c)
		})
	}
	if len(steers) > 0 {
		p.inbox.Push(steers...)
	}
	p.mu.Unlock()
	return live
}

func (r *Runtime) drainInbox(ctx context.Context, p *sessionProc, eng tacklr.Engine, out chan tacklr.StreamEvent) (int, int, error) {
	live := r.harvestJobs(p)
	msgs := p.inbox.Take()
	n, err := adapter.AbsorbAll(ctx, eng.AbsorbUser, msgs, out)
	return n, live, err
}

func (r *Runtime) waitJobs(ctx context.Context, p *sessionProc) error {
	p.mu.Lock()
	wake := p.wake
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	}
}

// sessionJobs is the JobHost for one parent session.
type sessionJobs struct {
	r *Runtime
	p *sessionProc
}

func (s sessionJobs) Schedule(ctx context.Context, job tacklr.JobRequest, callID string) (tacklr.Job, error) {
	name, task, err := adapter.NormalizeSpawn(job.Name, job.Task)
	if err != nil {
		return tacklr.Job{}, err
	}
	s.p.mu.Lock()
	agentID := s.p.agentID
	s.p.mu.Unlock()
	if adapter.HasSpecialist(s.r.catalog, agentID, name) {
		id, err := s.spawnSpecialist(ctx, name, task, callID)
		if err != nil {
			return tacklr.Job{}, err
		}
		return tacklr.Job{ID: id, Name: name, State: tacklr.JobRunning}, nil
	}
	if _, ok := s.r.jobs[name]; !ok {
		return tacklr.Job{}, fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
	}
	return s.startWorker(ctx, name, task, callID)
}

func (s sessionJobs) spawnSpecialist(ctx context.Context, specialist, task, callID string) (string, error) {
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

func (s sessionJobs) startWorker(ctx context.Context, name, task, callID string) (tacklr.Job, error) {
	childID := durable.JobID(s.p.id, name, callID)
	if s.r.ownsChild(s.p, childID) {
		return tacklr.Job{ID: string(childID), Name: name, State: tacklr.JobRunning}, nil
	}
	if task == "" {
		return tacklr.Job{}, fmt.Errorf("task is required: %w", tacklr.ErrInvalid)
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
		Worker:     name,
		MCPServers: mcp,
		Mounts:     mounts,
	})
	if err != nil {
		return tacklr.Job{}, err
	}
	if err := ctx.Err(); err != nil {
		_ = s.r.Close(context.WithoutCancel(ctx), id)
		s.r.dropChild(s.p, id)
		return tacklr.Job{}, err
	}
	if err := s.r.Prompt(s.p.childCtx(), id, durable.Prompt{Text: task, Auth: auth}); err != nil {
		return tacklr.Job{}, err
	}
	return tacklr.Job{ID: string(id), Name: name, State: tacklr.JobRunning}, nil
}

func (s sessionJobs) Jobs() []tacklr.Job {
	rows := s.r.childStatuses(s.p)
	out := make([]tacklr.Job, 0, len(rows))
	for _, st := range rows {
		out = append(out, jobFromStatus(st))
	}
	return out
}

func (s sessionJobs) CancelJob(ctx context.Context, id string) error {
	sid := durable.SessionID(id)
	if !s.r.ownsChild(s.p, sid) {
		return adapter.UnknownChild(id)
	}
	_ = s.r.Close(ctx, sid)
	s.r.dropChild(s.p, sid)
	return nil
}

func (s sessionJobs) RunSpecialist(ctx context.Context, name, task, callID string) (string, error) {
	s.p.mu.Lock()
	agentID := s.p.agentID
	s.p.mu.Unlock()
	if !adapter.HasSpecialist(s.r.catalog, agentID, name) {
		return "", fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
	}
	id, err := s.spawnSpecialist(ctx, name, task, callID)
	if err != nil {
		return "", err
	}
	sid := durable.SessionID(id)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		st, err := s.r.Status(ctx, sid)
		if err != nil {
			return "", err
		}
		if st.State == durable.SessionComplete || st.State == durable.SessionFailed {
			s.r.dropChild(s.p, sid)
			if st.State == durable.SessionFailed {
				err = st.Err
				if err == nil {
					err = fmt.Errorf("failed: %w", tacklr.ErrFailed)
				}
				if st.Result != "" {
					return st.Result, err
				}
				return "", err
			}
			return st.Result, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-s.p.wake:
		}
	}
}

func jobFromStatus(st durable.SessionStatus) tacklr.Job {
	j := tacklr.Job{ID: string(st.ID), Name: st.Specialist, State: adapter.ChildState(st.State), Result: st.Result}
	if st.State == durable.SessionFailed && j.Result == "" && st.Err != nil {
		j.Result = st.Err.Error()
	}
	return j
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
