package inprocess

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

type turnOutcome int

const (
	turnComplete turnOutcome = iota
	turnYield
	turnCancelled
	turnError
)

func (r *Runtime) constructHarness(ctx context.Context, p *sessionProc, bindings []vfs.Binding, state map[string]any) (*tacklr.TurnManager, *vfs.MountSession, *vfs.MountSession, error) {
	if p.agentID == "" {
		return nil, nil, nil, fmt.Errorf("no agent configured for session")
	}
	spec, ok := r.catalog.Lookup(p.agentID)
	if !ok {
		return nil, nil, nil, durable.ErrAgentNotFound
	}
	if p.specialist != "" {
		over, err := durable.OverlaySpecialist(spec, p.specialist)
		if err != nil {
			return nil, nil, nil, err
		}
		spec = over
	}
	threadID := string(p.id)
	ms, skillsMS, err := durable.OpenTurnSessions(ctx, threadID, spec, bindings, r.projection)
	if err != nil {
		return nil, nil, nil, err
	}
	opts := spec.Options
	opts.SessionID = threadID
	opts.MountSession = ms
	opts.SkillsSession = skillsMS
	opts.SkillsRoot = spec.SkillsRoot
	if len(p.mcp) > 0 {
		mcpConfigs := make([]mcp.MCPConfig, 0, len(spec.Options.MCPConfigs)+len(p.mcp))
		mcpConfigs = append(mcpConfigs, spec.Options.MCPConfigs...)
		mcpConfigs = append(mcpConfigs, p.mcp...)
		opts.MCPConfigs = mcpConfigs
	}

	h, err := tacklr.NewTurnManager(ctx, opts)
	if err != nil {
		durable.CloseTurnTrees(ms, skillsMS, threadID, "construct")
		return nil, nil, nil, err
	}
	h.BindChildHost(sessionChildren{r: r, p: p})
	snap, rev, loadErr := r.snapshots.Load(ctx, p.id)
	switch {
	case loadErr == nil:
		p.revision = rev
		if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
			h.Close()
			durable.CloseTurnTrees(ms, skillsMS, threadID, "restore")
			return nil, nil, nil, err
		}
	case errors.Is(loadErr, durable.ErrSessionNotFound):
	default:
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, threadID, "load")
		return nil, nil, nil, loadErr
	}
	if err := h.ApplySessionState(state); err != nil {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, threadID, "state")
		return nil, nil, nil, err
	}
	return h, ms, skillsMS, nil
}

func (r *Runtime) persistHarness(ctx context.Context, p *sessionProc, h *tacklr.TurnManager) error {
	cp, err := h.Checkpoint()
	if err != nil {
		telemetry.RecordCheckpointAttempt(ctx, err)
		return err
	}
	p.mu.Lock()
	children := slices.Clone(p.children)
	parent := p.parent
	specialist := p.specialist
	p.mu.Unlock()
	rev, err := r.snapshots.Save(ctx, p.id, durable.Snapshot{
		AgentID:    p.agentID,
		Specialist: specialist,
		Parent:     parent,
		Children:   children,
		Checkpoint: *cp,
		Mounts:     p.mounts,
	}, p.revision)
	telemetry.RecordCheckpointAttempt(ctx, err)
	if err != nil {
		return err
	}
	p.revision = rev
	p.mu.Lock()
	p.state = nil
	p.mu.Unlock()
	return nil
}

func (r *Runtime) emit(ctx context.Context, p *sessionProc, ev tacklr.StreamEvent) {
	if err := r.events.Append(ctx, p.id, durable.TopicEvents, ev); err != nil {
		slog.Warn("eventlog append failed", "session_id", p.id, "error", err)
	}
}

func (r *Runtime) fail(ctx context.Context, p *sessionProc, err error) turnOutcome {
	p.mu.Lock()
	p.termErr = err
	p.mu.Unlock()
	r.emit(ctx, p, tacklr.StreamEvent{Type: tacklr.StreamEventError, Error: err})
	return turnError
}

func (r *Runtime) runTurn(ctx context.Context, p *sessionProc, user *tacklr.Message, resume map[string][]byte, bindings []vfs.Binding, state map[string]any) turnOutcome {
	h, ms, skillsMS, err := r.constructHarness(ctx, p, bindings, state)
	if err != nil {
		return r.fail(ctx, p, err)
	}
	defer func() {
		h.Close()
		durable.CloseTurnTrees(ms, skillsMS, string(p.id), "turn_end")
	}()

	eng := h.Drive()
	out, stop := tacklr.PipeStreamEvents(func(ev tacklr.StreamEvent) {
		if ev.Type == tacklr.StreamEventComplete {
			p.mu.Lock()
			n := len(p.children)
			p.mu.Unlock()
			if n > 0 {
				return
			}
		}
		r.emit(ctx, p, ev)
	})
	defer stop()

	cancelled := func() turnOutcome {
		persist := context.WithoutCancel(ctx)
		if err := r.persistHarness(persist, p, h); err != nil {
			r.emit(persist, p, tacklr.StreamEvent{Type: tacklr.StreamEventError, Error: err})
		}
		err := ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		r.emit(persist, p, tacklr.StreamEvent{Type: tacklr.StreamEventError, Error: err})
		return turnCancelled
	}

	if len(resume) > 0 {
		if err := eng.ApplyResume(resume); err != nil {
			return r.fail(ctx, p, err)
		}
	} else if user != nil {
		if err := eng.AbsorbUser(ctx, user, out); err != nil {
			if ctx.Err() != nil {
				return cancelled()
			}
			return r.fail(ctx, p, err)
		}
	}

	st := &tacklr.TurnState{}
	toolCalls := eng.PendingToolCalls()
	parked := false
	inferComplete := false
	for {
		if ctx.Err() != nil {
			return cancelled()
		}
		nudge := ""
		if len(toolCalls) == 0 && !parked && inferComplete {
			nudge = durable.ChildrenNudge(r.childStatuses(p))
		}
		switch tacklr.Next(len(toolCalls), parked, inferComplete, nudge != "") {
		case tacklr.ActionInfer:
			step, infErr := eng.RunInference(ctx, st, out)
			if ctx.Err() != nil {
				return cancelled()
			}
			if infErr != nil {
				p.mu.Lock()
				p.termErr = infErr
				p.mu.Unlock()
				if err := r.persistHarness(ctx, p, h); err != nil {
					r.emit(ctx, p, tacklr.StreamEvent{Type: tacklr.StreamEventError, Error: err})
				}
				return turnError
			}
			if step.Complete {
				inferComplete = true
				toolCalls = nil
				continue
			}
			toolCalls = step.ToolCalls
		case tacklr.ActionRunTools:
			st.HadToolRound = true
			var hit atomic.Bool
			var wg sync.WaitGroup
			for _, tc := range toolCalls {
				wg.Add(1)
				go func(tc tacklr.ToolCall) {
					defer wg.Done()
					if ctx.Err() != nil {
						return
					}
					step, _ := eng.RunToolCall(ctx, tc, out)
					if step.Interrupted {
						hit.Store(true)
					}
				}(tc)
			}
			wg.Wait()
			if ctx.Err() != nil {
				return cancelled()
			}
			if err := r.persistHarness(ctx, p, h); err != nil {
				return r.fail(ctx, p, err)
			}
			parked = hit.Load()
			toolCalls = nil
		case tacklr.ActionYield:
			return turnYield
		case tacklr.ActionComplete:
			r.captureResult(p, h)
			if err := r.persistHarness(ctx, p, h); err != nil {
				return r.fail(ctx, p, err)
			}
			return turnComplete
		case tacklr.ActionNudge:
			if err := eng.AbsorbUser(ctx, &tacklr.Message{Role: tacklr.RoleUser, Content: nudge}, out); err != nil {
				return r.fail(ctx, p, err)
			}
			st.HadToolRound = true
			inferComplete = false
		}
	}
}

func (r *Runtime) captureResult(p *sessionProc, h *tacklr.TurnManager) {
	var result string
	for _, m := range h.Drive().Messages() {
		if m != nil && m.Role == tacklr.RoleAssistant && m.Content != "" {
			result = m.Content
		}
	}
	if result == "" {
		return
	}
	p.mu.Lock()
	p.result = result
	p.mu.Unlock()
}

func (r *Runtime) recordTurn(ctx context.Context, agentID, threadID, kind string, promptLen, resumeCount int) (context.Context, func(turnOutcome)) {
	ctx = telemetry.BindTurnContext(ctx, agentID, threadID)
	ctx, span := telemetry.StartTurnSpan(ctx, telemetry.TurnAttrs{
		AgentID:     agentID,
		ThreadID:    threadID,
		SessionID:   threadID,
		Kind:        kind,
		Runtime:     telemetry.RuntimeInProcess,
		LoadSession: kind == telemetry.TurnKindResume,
	})
	telemetry.EmitTurnReceived(ctx, kind, promptLen, resumeCount)
	return ctx, func(o turnOutcome) {
		switch o {
		case turnCancelled:
			span.End(telemetry.OutcomeCancelled)
		case turnError:
			span.End(telemetry.OutcomeError)
		case turnYield:
			span.End(telemetry.OutcomeYield)
		default:
			span.End(telemetry.OutcomeOK)
		}
	}
}
