package temporal

import (
	"context"
	"errors"
	"slices"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/internal/drive"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// SessionWorkflow is the harness wait loop: one Temporal workflow per agent session.
// It is the primary OpenTelemetry instrumentor: one tacklr.turn span per prompt or
// resume, with Inference/Tool activities as children via OTEL v2 propagation.
func SessionWorkflow(ctx workflow.Context, in WorkflowInput) (string, error) {
	logger := workflow.GetLogger(ctx)
	stream, err := workflowstreams.NewWorkflowStream(ctx, nil)
	if err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var (
		closed     bool
		agentID    = in.AgentID
		mcp        = in.MCPServers
		mounts     = durable.ApplyAuth(in.Mounts, in.Auth)
		lastAuth   = in.Auth
		spawned    []childRun
		yielded    bool
		result     string
		terminal   durable.SessionState
		childParks map[string]durable.SessionID
		seed       = in.State
		promptCh   = workflow.GetSignalChannel(ctx, signalPrompt)
		resumeCh   = workflow.GetSignalChannel(ctx, signalResume)
		cancelCh   = workflow.GetSignalChannel(ctx, signalCancel)
		closeCh    = workflow.GetSignalChannel(ctx, signalClose)
	)
	_ = workflow.SetQueryHandler(ctx, queryStatus, func() (durable.SessionStatus, error) {
		st := durable.SessionStatus{
			ID:         in.SessionID,
			Parent:     in.Parent,
			Specialist: in.Specialist,
			Kind:       "",
			State:      durable.SessionRunning,
			Waiting:    yielded,
			Result:     result,
		}
		if in.Specialist != "" {
			st.Kind = durable.SessionKindSpecialist
		}
		if terminal != "" {
			st.State = terminal
			st.Waiting = false
		} else if closed {
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
		s.AddReceive(workflow.GetSignalChannel(ctx, signalChildWaiting), func(c workflow.ReceiveChannel, more bool) {
			var id durable.SessionID
			c.Receive(ctx, &id)
			if i := findChild(spawned, id); i >= 0 {
				spawned[i].waiting = true
			}
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

	runSlice := func(user *streaming.Message, resume map[string][]byte, auth durable.AuthContext, kind string, extra map[string]any) {
		turnState := durable.MergeUserState(seed, extra)
		seed = nil
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
				terminal = durable.SessionFailed
				emitCancel()
				return true
			}
			outcome = telemetry.OutcomeError
			terminal = durable.SessionFailed
			turnErr = err
			emitStreamErr(err.Error())
			return true
		}
		hadTools := false
		reqs := 0
		toolCalls := []streaming.ToolCall(nil)
		leftover := []streaming.ToolCall(nil)
		parked := false
		inferComplete := false
		interruptID := ""
		stopSlice := false
		for !stopSlice {
			switch drive.Next(len(toolCalls), parked, inferComplete, len(spawned) > 0) {
			case drive.ActionInfer:
				var out InferenceOutput
				err := waitAct("Inference", InferenceInput{
					SessionID:     in.SessionID,
					AgentID:       agentID,
					MCPServers:    mcp,
					User:          user,
					HadToolRound:  hadTools,
					ModelRequests: reqs,
					Resume:        resume,
					Auth:          lastAuth,
					Mounts:        mounts,
					Specialist:    in.Specialist,
					State:         turnState,
				}, &out)
				user = nil
				resume = nil
				if onActErr(err) {
					stopSlice = true
					break
				}
				reqs++
				if out.Complete {
					inferComplete = true
					result = out.Result
					toolCalls = nil
					continue
				}
				inferComplete = false
				toolCalls = out.ToolCalls
			case drive.ActionRunTools:
				hadTools = true
				tc := toolCalls[0]
				rest := toolCalls[1:]
				var tout ToolOutput
				err := waitAct("Tool", ToolInput{
					SessionID:  in.SessionID,
					AgentID:    agentID,
					MCPServers: mcp,
					Call:       tc,
					Auth:       lastAuth,
					Mounts:     mounts,
					Specialist: in.Specialist,
					Known:      spawnedIDs(spawned),
					State:      turnState,
				}, &tout)
				if onActErr(err) {
					stopSlice = true
					break
				}
				if herr := applyChildIntent(ctx, sessionCtx, &spawned, tout, in, agentID, lastAuth, mounts); herr != nil {
					stopSlice = onActErr(herr)
					break
				}
				if tout.AwaitID != "" {
					output, parkID, werr := waitChildTool(ctx, &spawned, &childParks, tc.Key(), tout.AwaitID, cancelCh, cancelSpawned)
					if onActErr(werr) {
						stopSlice = true
						break
					}
					if parkID == "" {
						var cout ToolOutput
						cerr := waitAct("CommitToolOutput", CommitToolInput{
							SessionID:  in.SessionID,
							AgentID:    agentID,
							MCPServers: mcp,
							Call:       tc,
							Output:     output,
							Auth:       lastAuth,
							Mounts:     mounts,
							Specialist: in.Specialist,
							State:      turnState,
						}, &cout)
						if onActErr(cerr) {
							stopSlice = true
							break
						}
						tout.Interrupted = false
					} else {
						tout.Interrupted = true
						if tout.InterruptID == "" {
							tout.InterruptID = parkID
						}
					}
				}
				if tout.Interrupted {
					leftover = rest
					interruptID = tout.InterruptID
					parked = true
					toolCalls = nil
					continue
				}
				toolCalls = rest
			case drive.ActionYield:
				if hasSession {
					workflow.CompleteSession(sessionCtx)
					hasSession = false
					sessionCtx = ctx
				}
				yielded = true
				if in.Parent != "" {
					_ = workflow.SignalExternalWorkflow(ctx, string(in.Parent), "", signalChildWaiting, in.SessionID).Get(ctx, nil)
				}
				logInfo(ctx, "turn yielded", "agent_id", agentID, "session_id", in.SessionID, "interrupt_id", interruptID)
				closeTurn(telemetry.OutcomeYield, nil)
				waiting := true
				for waiting {
					ev := selectorWait()
					switch ev.kind {
					case signalResume:
						waiting = false
						parked = false
						yielded = false
						applyAuth(ev.resume.Auth)
						turnState = durable.MergeUserState(turnState, ev.resume.State)
						if cid, ok := childParks[interruptID]; ok {
							_ = workflow.SignalExternalWorkflow(ctx, string(cid), "", signalResume, resumeSignal{Responses: ev.resume.Responses, Auth: ev.resume.Auth, State: ev.resume.State}).Get(ctx, nil)
							delete(childParks, interruptID)
						}
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
							Resume:     ev.resume.Responses,
							Auth:       lastAuth,
							Mounts:     mounts,
							State:      turnState,
						}, &iout)
						if onActErr(err) {
							stopSlice = true
							break
						}
						toolCalls = slices.Concat(iout.ToolCalls, leftover)
						leftover = nil
						inferComplete = false
					case signalClose:
						closed = true
						outcome = telemetry.OutcomeOK
						stopSlice = true
						waiting = false
					case signalCancel:
						cancelSpawned()
						emitCancel()
						outcome = telemetry.OutcomeCancelled
						stopSlice = true
						waiting = false
					}
				}
			case drive.ActionComplete:
				terminal = durable.SessionComplete
				stopSlice = true
			case drive.ActionNudge:
				nudgeMsg := &streaming.Message{Role: tacklr.RoleUser, Content: spawnedNudge(spawned)}
				var out InferenceOutput
				err := waitAct("Inference", InferenceInput{
					SessionID:     in.SessionID,
					AgentID:       agentID,
					MCPServers:    mcp,
					User:          nudgeMsg,
					HadToolRound:  true,
					ModelRequests: reqs,
					Auth:          lastAuth,
					Mounts:        mounts,
					Specialist:    in.Specialist,
					State:         turnState,
				}, &out)
				if onActErr(err) {
					stopSlice = true
					break
				}
				reqs++
				hadTools = true
				inferComplete = false
				if out.Complete {
					result = out.Result
					inferComplete = true
					toolCalls = nil
					continue
				}
				toolCalls = out.ToolCalls
			}
		}
	}

	if in.Prompt != "" {
		runSlice(&streaming.Message{Role: streaming.RoleUser, Content: in.Prompt}, nil, in.Auth, telemetry.TurnKindPrompt, nil)
		return result, nil
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
			runSlice(user, nil, ev.prompt.Auth, telemetry.TurnKindPrompt, ev.prompt.State)
		case signalResume:
			yielded = false
			runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume, ev.resume.State)
		}
	}
	return result, nil
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
