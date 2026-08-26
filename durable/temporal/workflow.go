package temporal

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const spawnSpecialistName = tacklr.SpawnSpecialistName

// SessionWorkflow is the harness wait loop: one Temporal workflow per agent session.
// It is the primary OpenTelemetry instrumentor: one tacklr.turn span per prompt or
// resume, with Inference/Tool activities as children via OTEL v2 propagation.
func SessionWorkflow(ctx workflow.Context, in WorkflowInput) error {
	logger := workflow.GetLogger(ctx)
	stream, err := workflowstreams.NewWorkflowStream(ctx, nil)
	if err != nil {
		logger.Error("workflow stream", "error", err)
	}

	type spawnedChild struct {
		id  durable.SessionID
		fut workflow.ChildWorkflowFuture
	}
	var (
		etag     string
		closed   bool
		agentID  = in.AgentID
		mcp      = in.MCPServers
		mounts   = durable.ApplyAuth(in.Mounts, in.Auth)
		lastAuth = in.Auth
		spawned  []spawnedChild
		yielded  bool
		promptCh = workflow.GetSignalChannel(ctx, signalPrompt)
		resumeCh = workflow.GetSignalChannel(ctx, signalResume)
		cancelCh = workflow.GetSignalChannel(ctx, signalCancel)
		closeCh  = workflow.GetSignalChannel(ctx, signalClose)
	)
	_ = workflow.SetQueryHandler(ctx, queryStatus, func() (durable.SessionStatus, error) {
		st := durable.SessionStatus{
			ID:         in.SessionID,
			Parent:     in.Parent,
			Specialist: in.Specialist,
			Kind:       "",
			State:      durable.SessionRunning,
			Waiting:    yielded,
		}
		if in.Specialist != "" {
			st.Kind = durable.SessionKindSpecialist
		}
		if closed {
			st.State = durable.SessionComplete
			st.Waiting = false
		}
		return st, nil
	})
	_ = workflow.SetQueryHandler(ctx, queryChildren, func() ([]durable.SessionID, error) {
		out := make([]durable.SessionID, len(spawned))
		for i, c := range spawned {
			out[i] = c.id
		}
		return out, nil
	})
	cancelSpawned := func() {
		for _, c := range spawned {
			var exec workflow.Execution
			if err := c.fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
				continue
			}
			_ = workflow.RequestCancelExternalWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, nil)
		}
		spawned = nil
	}
	drainCancels := func() {
		var ignored any
		for cancelCh.ReceiveAsync(&ignored) {
		}
	}
	emitStreamErr := func(msg string) {
		if stream == nil || msg == "" {
			return
		}
		_ = stream.Topic(durable.TopicEvents).Publish(streaming.StreamEvent{
			Type:    streaming.StreamEventError,
			Fail:    msg,
			Content: msg,
		})
	}
	emitCancel := func() {
		emitStreamErr(context.Canceled.Error())
	}

	selectorWait := func() waitSignal {
		var out waitSignal
		s := workflow.NewSelector(ctx)
		s.AddReceive(promptCh, func(c workflow.ReceiveChannel, more bool) {
			var p promptSignal
			c.Receive(ctx, &p)
			out.kind, out.prompt = signalPrompt, p
		})
		s.AddReceive(resumeCh, func(c workflow.ReceiveChannel, more bool) {
			var p resumeSignal
			c.Receive(ctx, &p)
			out.kind, out.resume = signalResume, p
		})
		s.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.kind = signalCancel
		})
		s.AddReceive(closeCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.kind = signalClose
		})
		s.Select(ctx)
		return out
	}

	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}

	applyAuth := func(auth durable.AuthContext) {
		mounts = durable.ApplyAuth(mounts, auth)
		if len(auth.Bindings) > 0 {
			lastAuth = auth
		}
	}

	runSlice := func(user *streaming.Message, resume map[string][]byte, auth durable.AuthContext, kind string) {
		applyAuth(auth)
		sessionCtx, hasSession := openTurnLocality(ctx, in.TurnLocalityTimeout, 2*time.Second)

		var endTurn func(string, error)
		openTurn := func(k string) {
			_, endTurn = startTurn(sessionCtx, agentID, in.SessionID, k)
		}
		closeTurn := func(outcome string, err error) {
			if endTurn == nil {
				return
			}
			endTurn(outcome, err)
			endTurn = nil
		}

		promptBytes := 0
		if user != nil {
			promptBytes = len(user.Content)
		}
		logInfo(ctx, "turn start",
			"kind", kind, "agent_id", agentID, "session_id", in.SessionID,
			"prompt_len", promptBytes, "resume_count", len(resume),
		)
		openTurn(kind)
		outcome := telemetry.OutcomeOK
		var turnErr error
		defer func() {
			closeTurn(outcome, turnErr)
			if hasSession {
				workflow.CompleteSession(sessionCtx)
				hasSession = false
			}
		}()

		actCtx := workflow.WithActivityOptions(sessionCtx, activityOpts)
		waitAct := func(name string, arg any, result any) error {
			cctx, cancelAct := workflow.WithCancel(actCtx)
			fut := workflow.ExecuteActivity(cctx, name, arg)
			var err error
			s := workflow.NewSelector(ctx)
			s.AddFuture(fut, func(f workflow.Future) { err = f.Get(ctx, result) })
			abort := func() {
				cancelAct()
				err = fut.Get(ctx, result)
			}
			s.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
				cancelSpawned()
				abort()
			})
			s.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
				abort()
			})
			s.Select(ctx)
			drainCancels()
			return err
		}
		onActErr := func(err error) bool {
			if err == nil {
				return false
			}
			if turnCanceled(ctx, err) {
				outcome = telemetry.OutcomeCancelled
				emitCancel()
				return true
			}
			outcome = telemetry.OutcomeError
			turnErr = err
			emitStreamErr(err.Error())
			return true
		}
		hadTools := false
		reqs := 0
		pending := []streaming.ToolCall(nil)
		for {
			if len(pending) == 0 {
				var out InferenceOutput
				err := waitAct("Inference", InferenceInput{
					SessionID:     in.SessionID,
					AgentID:       agentID,
					MCPServers:    mcp,
					Etag:          etag,
					User:          user,
					HadToolRound:  hadTools,
					ModelRequests: reqs,
					Resume:        resume,
					Auth:          lastAuth,
					Mounts:        mounts,
					Specialist:    in.Specialist,
				}, &out)
				user = nil
				resume = nil
				if onActErr(err) {
					break
				}
				etag = out.Etag
				reqs++
				if out.Complete {
					break
				}
				pending = out.ToolCalls
				continue
			}
			hadTools = true
			next := pending
			pending = nil
			stopSlice := false
			// Sequential Tool activities share SnapshotStore etag. Leftover
			// calls after HITL stay in this workflow variable (history), not
			// the snapshot. In-process runs the batch in parallel instead.
			for i, tc := range next {
				if tc.Name == spawnSpecialistName {
					call, parseErr := durable.ParseSpawnCall(tc.Arguments)
					if parseErr != nil {
						stopSlice = onActErr(parseErr)
						break
					}
					childID := durable.ChildSessionID(in.SessionID, call.Specialist, tc.Key())
					cwo := workflow.ChildWorkflowOptions{
						WorkflowID: string(childID),
					}
					logInfo(sessionCtx, "child workflow",
						"workflow_id", cwo.WorkflowID, "agent_id", agentID, "worker", call.Specialist,
					)
					cctx := workflow.WithChildOptions(sessionCtx, cwo)
					fut := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, WorkflowInput{
						SessionID:           childID,
						AgentID:             agentID,
						Parent:              in.SessionID,
						Specialist:          call.Specialist,
						Prompt:              call.Task,
						Auth:                lastAuth,
						Mounts:              mounts,
						TurnLocalityTimeout: in.TurnLocalityTimeout,
					})
					spawned = append(spawned, spawnedChild{id: childID, fut: fut})
					output := durable.ScheduledChildMessage(childID, call.Specialist)
					if call.Block {
						var waitErr error
						ws := workflow.NewSelector(ctx)
						ws.AddFuture(fut, func(f workflow.Future) { waitErr = f.Get(ctx, nil) })
						ws.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
							c.Receive(ctx, nil)
							cancelSpawned()
							waitErr = context.Canceled
						})
						ws.Select(ctx)
						if waitErr != nil {
							workflow.GetLogger(ctx).Error("child workflow", "error", waitErr)
							stopSlice = onActErr(waitErr)
							break
						}
						output = "ok"
					}
					var cout ToolOutput
					err := waitAct("CommitToolOutput", CommitToolInput{
						SessionID:  in.SessionID,
						AgentID:    agentID,
						MCPServers: mcp,
						Etag:       etag,
						Call:       tc,
						Output:     output,
						Auth:       lastAuth,
						Mounts:     mounts,
						Specialist: in.Specialist,
					}, &cout)
					if onActErr(err) {
						stopSlice = true
						break
					}
					etag = cout.Etag
					continue
				}
				var tout ToolOutput
				err := waitAct("Tool", ToolInput{
					SessionID:  in.SessionID,
					AgentID:    agentID,
					MCPServers: mcp,
					Etag:       etag,
					Call:       tc,
					Auth:       lastAuth,
					Mounts:     mounts,
					Specialist: in.Specialist,
				}, &tout)
				if onActErr(err) {
					stopSlice = true
					break
				}
				etag = tout.Etag
				if !tout.Interrupted {
					continue
				}
				rest := append([]streaming.ToolCall(nil), next[i+1:]...)
				if hasSession {
					workflow.CompleteSession(sessionCtx)
					hasSession = false
					sessionCtx = ctx
				}
				yielded = true
				logInfo(ctx, "turn yielded", "agent_id", agentID, "session_id", in.SessionID, "interrupt_id", tout.InterruptID)
				closeTurn(telemetry.OutcomeYield, nil)
				parked := true
				for parked {
					ev := selectorWait()
					switch ev.kind {
					case signalResume:
						parked = false
						applyAuth(ev.resume.Auth)
						sessionCtx, hasSession = openTurnLocality(ctx, in.TurnLocalityTimeout, time.Minute)
						logInfo(ctx, "turn start",
							"kind", telemetry.TurnKindResume, "agent_id", agentID,
							"session_id", in.SessionID, "resume_count", len(ev.resume.Responses),
						)
						openTurn(telemetry.TurnKindResume)
						outcome, turnErr = telemetry.OutcomeOK, nil
						actCtx = workflow.WithActivityOptions(sessionCtx, activityOpts)
						var iout InferenceOutput
						err := waitAct("Inference", InferenceInput{
							SessionID:  in.SessionID,
							AgentID:    agentID,
							MCPServers: mcp,
							Etag:       etag,
							Resume:     ev.resume.Responses,
							Auth:       lastAuth,
							Mounts:     mounts,
						}, &iout)
						if onActErr(err) {
							stopSlice = true
							break
						}
						etag = iout.Etag
						pending = append(append([]streaming.ToolCall(nil), iout.ToolCalls...), rest...)
					case signalClose:
						closed = true
						outcome = telemetry.OutcomeOK
						stopSlice = true
						parked = false
					case signalCancel:
						cancelSpawned()
						emitCancel()
						outcome = telemetry.OutcomeCancelled
						stopSlice = true
						parked = false
					default:
						// prompt while parked: ignore
					}
				}
				break
			}
			if stopSlice {
				break
			}
		}
	}

	if in.Prompt != "" {
		runSlice(&streaming.Message{Role: streaming.RoleUser, Content: in.Prompt}, nil, in.Auth, telemetry.TurnKindPrompt)
		return nil
	}

	for !closed {
		ev := selectorWait()
		switch ev.kind {
		case signalClose:
			closed = true
		case signalCancel:
			// Idle cancel must still stop child sessions. Do not emit a stream
			// error: that poisons the next prompt's Subscribe(after Head).
			cancelSpawned()
			continue
		case signalPrompt:
			if ev.prompt.AgentID != "" {
				agentID = ev.prompt.AgentID
			}
			if ev.prompt.MCPServers != nil {
				mcp = ev.prompt.MCPServers
			}
			user := ev.prompt.UserMessage
			if user == nil && ev.prompt.Text != "" {
				user = &streaming.Message{Role: streaming.RoleUser, Content: ev.prompt.Text}
			}
			runSlice(user, nil, ev.prompt.Auth, telemetry.TurnKindPrompt)
		case signalResume:
			yielded = false
			runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume)
		}
	}
	return nil
}

func turnCanceled(ctx workflow.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	return temporal.IsCanceledError(err) || errors.Is(err, workflow.ErrCanceled) || errors.Is(err, workflow.ErrSessionFailed)
}

// openTurnLocality pins activities to one worker when the host set a timeout.
// Timeout <= 0 skips CreateSession (no hidden default).
func openTurnLocality(ctx workflow.Context, timeout, creation time.Duration) (workflow.Context, bool) {
	if timeout <= 0 {
		return ctx, false
	}
	if creation <= 0 {
		creation = 2 * time.Second
	}
	sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
		CreationTimeout:  creation,
		ExecutionTimeout: timeout,
	})
	if err != nil {
		workflow.GetLogger(ctx).Error("worker session", "error", err)
		return ctx, false
	}
	return sctx, true
}
