package inprocess

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/streaming"
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

func (r *Runtime) constructHarness(ctx context.Context, p *sessionProc, load bool, bindings []vfs.Binding) (*tacklr.AgentHarness, *vfs.MountSession, error) {
	if p.agentID == "" {
		return nil, nil, fmt.Errorf("no agent configured for session")
	}
	spec, ok := r.catalog.Lookup(p.agentID)
	if !ok {
		return nil, nil, durable.ErrAgentNotFound
	}
	threadID := string(p.id)
	ms, err := durable.OpenTurnVFS(ctx, threadID, spec, bindings, r.projection)
	if err != nil {
		return nil, nil, err
	}
	mcpConfigs := make([]mcp.MCPConfig, 0, len(spec.Options.MCPConfigs)+len(p.mcp))
	mcpConfigs = append(mcpConfigs, spec.Options.MCPConfigs...)
	mcpConfigs = append(mcpConfigs, p.mcp...)
	opts := spec.Options
	opts.SessionID = threadID
	opts.MCPConfigs = mcpConfigs
	opts.MountSession = ms

	h, err := tacklr.NewAgent(ctx, opts)
	if err != nil {
		durable.CloseTurnVFS(ms, threadID, "construct")
		return nil, nil, err
	}
	if load {
		snap, etag, loadErr := r.snapshots.Load(ctx, p.id)
		switch {
		case loadErr == nil:
			p.etag = etag
			if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
				h.Close()
				durable.CloseTurnVFS(ms, threadID, "restore")
				return nil, nil, err
			}
		case errors.Is(loadErr, durable.ErrSessionNotFound):
			// first turn
		default:
			h.Close()
			durable.CloseTurnVFS(ms, threadID, "load")
			return nil, nil, loadErr
		}
	}
	return h, ms, nil
}

func (r *Runtime) persistHarness(ctx context.Context, p *sessionProc, h *tacklr.AgentHarness) error {
	cp, err := h.Checkpoint()
	if err != nil {
		telemetry.RecordCheckpointAttempt(ctx, err)
		return err
	}
	etag, err := r.snapshots.Save(ctx, p.id, durable.Snapshot{AgentID: p.agentID, Checkpoint: *cp, Mounts: p.mounts}, p.etag)
	telemetry.RecordCheckpointAttempt(ctx, err)
	if err != nil {
		return err
	}
	p.etag = etag
	return nil
}

func (r *Runtime) emit(ctx context.Context, p *sessionProc, ev streaming.StreamEvent) {
	if err := r.events.Append(ctx, p.id, durable.TopicEvents, ev); err != nil {
		slog.Warn("eventlog append failed", "session_id", p.id, "error", err)
	}
}

func (r *Runtime) fail(ctx context.Context, p *sessionProc, err error) turnOutcome {
	r.emit(ctx, p, streaming.StreamEvent{Type: streaming.StreamEventError, Error: err})
	return turnError
}

func (r *Runtime) driveTurn(ctx context.Context, p *sessionProc, user *tacklr.Message, resume map[string][]byte, bindings []vfs.Binding) turnOutcome {
	load := resume != nil || p.etag != ""
	h, ms, err := r.constructHarness(ctx, p, load, bindings)
	if err != nil {
		return r.fail(ctx, p, err)
	}
	defer func() {
		h.Close()
		durable.CloseTurnVFS(ms, string(p.id), "turn_end")
	}()

	out, stop := tacklr.PipeStreamEvents(func(ev streaming.StreamEvent) { r.emit(ctx, p, ev) })
	defer stop()

	cancelled := func() turnOutcome {
		if err := r.persistHarness(context.Background(), p, h); err != nil {
			r.emit(context.Background(), p, streaming.StreamEvent{Type: streaming.StreamEventError, Error: err})
		}
		r.emit(context.Background(), p, streaming.StreamEvent{Type: streaming.StreamEventError, Error: context.Canceled})
		return turnCancelled
	}

	if len(resume) > 0 {
		if err := h.ApplyResume(resume); err != nil {
			return r.fail(ctx, p, err)
		}
	} else if user != nil {
		if err := h.AbsorbUser(ctx, user, out); err != nil {
			if ctx.Err() != nil {
				return cancelled()
			}
			return r.fail(ctx, p, err)
		}
	}

	st := &tacklr.TurnState{}
	toolCalls := h.PendingToolCalls()
	for {
		if ctx.Err() != nil {
			return cancelled()
		}
		if len(toolCalls) == 0 {
			step, infErr := h.RunInference(ctx, st, out)
			if ctx.Err() != nil {
				return cancelled()
			}
			if infErr != nil {
				if err := r.persistHarness(ctx, p, h); err != nil {
					r.emit(ctx, p, streaming.StreamEvent{Type: streaming.StreamEventError, Error: err})
				}
				return turnError
			}
			if step.Complete {
				if err := r.persistHarness(ctx, p, h); err != nil {
					return r.fail(ctx, p, err)
				}
				return turnComplete
			}
			toolCalls = step.ToolCalls
			continue
		}
		st.HadToolRound = true
		interrupted := false
		for _, tc := range toolCalls {
			if ctx.Err() != nil {
				return cancelled()
			}
			step, toolErr := h.RunToolCall(ctx, tc, out)
			if ctx.Err() != nil {
				return cancelled()
			}
			if step.Interrupted {
				if err := r.persistHarness(ctx, p, h); err != nil {
					return r.fail(ctx, p, fmt.Errorf("persist interrupt: %w", err))
				}
				interrupted = true
				break
			}
			if toolErr != nil {
				if err := r.persistHarness(ctx, p, h); err != nil {
					return r.fail(ctx, p, err)
				}
			}
		}
		if interrupted {
			return turnYield
		}
		if err := r.persistHarness(ctx, p, h); err != nil {
			return r.fail(ctx, p, err)
		}
		toolCalls = nil
	}
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
