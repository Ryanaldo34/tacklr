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

type runnerJob struct {
	id     string
	name   string
	cancel context.CancelFunc
	result string
	err    error
	term   bool
}

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
	keep := p.runners[:0]
	for _, j := range p.runners {
		if !j.term {
			keep = append(keep, j)
			live++
			continue
		}
		body := j.result
		failed := j.err != nil
		if failed && body == "" {
			body = j.err.Error()
		}
		steers = append(steers, adapter.JobSteer(j.id, j.name, body, failed))
	}
	p.runners = keep
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
	return s.startRunner(name, task, callID)
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

func (s sessionJobs) startRunner(name, task, callID string) (tacklr.Job, error) {
	id := string(durable.JobID(s.p.id, name, callID))
	s.p.mu.Lock()
	for _, j := range s.p.runners {
		if j.id == id {
			s.p.mu.Unlock()
			return tacklr.Job{ID: id, Name: name, State: tacklr.JobRunning, Result: j.result}, nil
		}
	}
	s.p.mu.Unlock()
	if task == "" {
		return tacklr.Job{}, fmt.Errorf("task is required: %w", tacklr.ErrInvalid)
	}
	fn := s.r.jobs[name]
	ctx, cancel := context.WithCancel(s.p.childCtx())
	j := &runnerJob{id: id, name: name, cancel: cancel}
	s.p.mu.Lock()
	s.p.runners = append(s.p.runners, j)
	s.p.mu.Unlock()
	go func() {
		res, err := fn(ctx, task)
		s.p.mu.Lock()
		j.result, j.err, j.term = res, err, true
		s.p.mu.Unlock()
		s.p.ping()
	}()
	return tacklr.Job{ID: id, Name: name, State: tacklr.JobRunning}, nil
}

func (s sessionJobs) Jobs() []tacklr.Job {
	rows := s.r.childStatuses(s.p)
	out := make([]tacklr.Job, 0, len(rows)+4)
	for _, st := range rows {
		out = append(out, jobFromStatus(st))
	}
	s.p.mu.Lock()
	for _, j := range s.p.runners {
		st := tacklr.JobRunning
		res := j.result
		if j.term {
			if j.err != nil {
				st = tacklr.JobFailed
				if res == "" {
					res = j.err.Error()
				}
			} else {
				st = tacklr.JobCompleted
			}
		}
		out = append(out, tacklr.Job{ID: j.id, Name: j.name, State: st, Result: res})
	}
	s.p.mu.Unlock()
	return out
}

func (s sessionJobs) CancelJob(ctx context.Context, id string) error {
	sid := durable.SessionID(id)
	if s.r.ownsChild(s.p, sid) {
		_ = s.r.Close(ctx, sid)
		s.r.dropChild(s.p, sid)
		return nil
	}
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	for i, j := range s.p.runners {
		if j.id != id {
			continue
		}
		if j.cancel != nil {
			j.cancel()
		}
		s.p.runners = slices.Delete(s.p.runners, i, i+1)
		return nil
	}
	return adapter.UnknownChild(id)
}

func (s sessionJobs) WaitJob(ctx context.Context, id string) (tacklr.Job, error) {
	sid := durable.SessionID(id)
	for {
		if err := ctx.Err(); err != nil {
			return tacklr.Job{}, err
		}
		if s.r.ownsChild(s.p, sid) {
			st, err := s.r.Status(ctx, sid)
			if err != nil {
				return tacklr.Job{}, err
			}
			if st.State == durable.SessionComplete || st.State == durable.SessionFailed {
				s.r.dropChild(s.p, sid)
				j := jobFromStatus(st)
				if st.State == durable.SessionFailed {
					err = st.Err
					if err == nil {
						err = fmt.Errorf("failed: %w", tacklr.ErrFailed)
					}
					return j, err
				}
				return j, nil
			}
		} else {
			s.p.mu.Lock()
			var found *runnerJob
			for _, j := range s.p.runners {
				if j.id == id {
					found = j
					break
				}
			}
			if found == nil {
				s.p.mu.Unlock()
				return tacklr.Job{}, adapter.UnknownChild(id)
			}
			if found.term {
				res, err, name := found.result, found.err, found.name
				s.p.runners = slices.DeleteFunc(s.p.runners, func(j *runnerJob) bool { return j.id == id })
				s.p.mu.Unlock()
				st := tacklr.JobCompleted
				if err != nil {
					st = tacklr.JobFailed
					if res == "" {
						res = err.Error()
					}
				}
				return tacklr.Job{ID: id, Name: name, State: st, Result: res}, err
			}
			s.p.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return tacklr.Job{}, ctx.Err()
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
