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
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// SessionWorkflow is the Temporal type name for the session wait loop.
// NewWorker registers it. Hosts call durable.Runtime, not this function.
func SessionWorkflow(ctx workflow.Context, in workflowInput) (string, error) {
	logger := workflow.GetLogger(ctx)
	if _, err := workflowstreams.NewWorkflowStream(ctx, nil); err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var (
		closed      bool
		agentID     = in.AgentID
		mcpServers  = in.MCPServers
		mounts      = adapter.ApplyAuth(in.Mounts, durable.AuthContext{})
		spawned     []childRun
		inbox       []*tacklr.Message
		nextAgentID string
		nextMCP     []mcp.MCPConfig
		yielded     bool
		result      string
		terminal    durable.SessionState
		childParks  map[string]durable.SessionID
		seed        = in.State
		promptCh    = workflow.GetSignalChannel(ctx, signalPrompt)
		resumeCh    = workflow.GetSignalChannel(ctx, signalResume)
		cancelCh    = workflow.GetSignalChannel(ctx, signalCancel)
		closeCh     = workflow.GetSignalChannel(ctx, signalClose)
		childWaitCh = workflow.GetSignalChannel(ctx, signalChildWaiting)
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
		return spawnedIDs(spawned), nil
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
		inbox = nil
		nextAgentID = ""
		nextMCP = nil
	}
	drainCancels := func() {
		var ignored any
		for cancelCh.ReceiveAsync(&ignored) {
		}
	}
	wait := func() waitSignal {
		return waitSession(ctx, promptCh, resumeCh, cancelCh, closeCh, childWaitCh, spawned)
	}

	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: resolveActivityTimeout(in.ActivityTimeout),
		HeartbeatTimeout:    resolveHeartbeatTimeout(in.HeartbeatTimeout),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: resolveActivityAttempts(in.ActivityAttempts),
		},
	}

	rec := func() durable.Snapshot {
		return durable.Snapshot{
			AgentID:    agentID,
			Specialist: in.Specialist,
			Parent:     in.Parent,
			Children:   spawnedIDs(spawned),
			Mounts:     mounts,
		}
	}

	runSlice := func(user *tacklr.Message, resume map[string][]byte, auth durable.AuthContext, kind string, extra map[string]any) {
		turnState := adapter.MergeUserState(seed, extra)
		seed = nil
		mounts = adapter.ApplyAuth(mounts, auth)
		terminal = ""
		yielded = false
		result = ""
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
		// Cancel is a separate mailbox so this wait can abort without
		// receiving Prompt (Prompt stays queued until InboxSafe drain).
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
		emitEvent := func(ev tacklr.StreamEvent) {
			_ = waitAct("EmitEvent", emitEventInput{SessionID: in.SessionID, Event: ev}, nil)
		}
		drainBufferedPrompts := func() {
			var p promptSignal
			for promptCh.ReceiveAsync(&p) {
				enqueueSteer(p, &inbox, &mounts, &turnState, &nextAgentID, &nextMCP)
			}
		}
		onActErr := func(err error) bool {
			if err == nil {
				return false
			}
			msg := err.Error()
			if turnCanceled(ctx, err) {
				outcome = telemetry.OutcomeCancelled
				terminal = durable.SessionFailed
				msg = context.Canceled.Error()
				emitEvent(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
				return true
			}
			outcome = telemetry.OutcomeError
			terminal = durable.SessionFailed
			turnErr = err
			emitEvent(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
			return true
		}
		hadTools := false
		reqs := 0
		toolCalls := []tacklr.ToolCall(nil)
		leftover := []tacklr.ToolCall(nil)
		parked := false
		inferComplete := false
		interruptID := ""
		interruptData := []byte(nil)
		stopSlice := false
		extraUsers := []*tacklr.Message(nil)
		for !stopSlice {
			if adapter.InboxSafe(len(toolCalls)+len(leftover), parked) {
				harvestReadyChildren(ctx, &spawned, &inbox)
				if user == nil {
					drainBufferedPrompts()
				}
				if extra := adapter.TakeMessages(&inbox); len(extra) > 0 {
					extraUsers = append(extraUsers, extra...)
					inferComplete = false
				}
			}
			switch tacklr.Next(len(toolCalls), parked, inferComplete, len(spawned) > 0) {
			case tacklr.ActionInfer:
				var out inferenceOutput
				err := waitAct("Inference", inferenceInput{
					SessionID:     in.SessionID,
					Rec:           rec(),
					MCPServers:    mcpServers,
					User:          user,
					Extra:         extraUsers,
					HadToolRound:  hadTools,
					ModelRequests: reqs,
					Resume:        resume,
					State:         turnState,
				}, &out)
				user = nil
				resume = nil
				extraUsers = nil
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
			case tacklr.ActionRunTools:
				hadTools = true
				tc := toolCalls[0]
				rest := toolCalls[1:]
				var tout toolOutput
				err := waitAct("Tool", toolInput{
					SessionID:  in.SessionID,
					Rec:        rec(),
					MCPServers: mcpServers,
					Call:       tc,
					State:      turnState,
				}, &tout)
				if onActErr(err) {
					stopSlice = true
					break
				}
				if herr := applyChildIntent(ctx, sessionCtx, &spawned, tout, in, agentID, mounts); herr != nil {
					stopSlice = onActErr(herr)
					break
				}
				if tout.AwaitID != "" {
					output, parkID, werr := waitChildTool(ctx, &spawned, &childParks, tc.Key(), tout.AwaitID, cancelCh, childWaitCh, cancelSpawned)
					if onActErr(werr) {
						stopSlice = true
						break
					}
					if parkID == "" {
						var cout toolOutput
						cerr := waitAct("CommitToolOutput", commitToolInput{
							SessionID:  in.SessionID,
							Rec:        rec(),
							MCPServers: mcpServers,
							Call:       tc,
							Output:     output,
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
					interruptData = tout.InterruptData
					parked = true
					toolCalls = nil
					continue
				}
				toolCalls = rest
			case tacklr.ActionYield:
				yielded = true
				emitEvent(tacklr.StreamEvent{
					Type:      tacklr.StreamEventInterrupt,
					MessageID: interruptID,
					Data:      interruptData,
				})
				if hasSession {
					workflow.CompleteSession(sessionCtx)
					hasSession = false
					sessionCtx = ctx
					actCtx = workflow.WithActivityOptions(sessionCtx, activityOpts)
				}
				if in.Parent != "" {
					_ = workflow.SignalExternalWorkflow(ctx, string(in.Parent), "", signalChildWaiting, in.SessionID).Get(ctx, nil)
				}
				logInfo(ctx, "turn yielded", "agent_id", agentID, "session_id", in.SessionID, "interrupt_id", interruptID)
				closeTurn(telemetry.OutcomeYield, nil)
				waiting := true
				for waiting {
					ev := wait()
					switch ev.kind {
					case signalPrompt:
						enqueueSteer(ev.prompt, &inbox, &mounts, &turnState, &nextAgentID, &nextMCP)
					case signalResume:
						waiting = false
						parked = false
						yielded = false
						mounts = adapter.ApplyAuth(mounts, ev.resume.Auth)
						turnState = adapter.MergeUserState(turnState, ev.resume.State)
						if cid, ok := childParks[interruptID]; ok {
							_ = workflow.SignalExternalWorkflow(ctx, string(cid), "", signalResume, resumeSignal{Responses: ev.resume.Responses, Auth: ev.resume.Auth.WithoutSecrets(), State: ev.resume.State}).Get(ctx, nil)
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
						var iout inferenceOutput
						err := waitAct("Inference", inferenceInput{
							SessionID:  in.SessionID,
							Rec:        rec(),
							MCPServers: mcpServers,
							Resume:     ev.resume.Responses,
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
						terminal = durable.SessionFailed
						yielded = false
						msg := context.Canceled.Error()
						emitEvent(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
						outcome = telemetry.OutcomeCancelled
						stopSlice = true
						waiting = false
					}
				}
			case tacklr.ActionComplete:
				terminal = durable.SessionComplete
				emitEvent(tacklr.StreamEvent{Type: tacklr.StreamEventComplete})
				stopSlice = true
			case tacklr.ActionNudge:
				nudgeMsg := &tacklr.Message{Role: tacklr.RoleUser, Content: spawnedNudge(spawned)}
				var out inferenceOutput
				err := waitAct("Inference", inferenceInput{
					SessionID:     in.SessionID,
					Rec:           rec(),
					MCPServers:    mcpServers,
					User:          nudgeMsg,
					Extra:         extraUsers,
					HadToolRound:  true,
					ModelRequests: reqs,
					State:         turnState,
				}, &out)
				extraUsers = nil
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
		runSlice(&tacklr.Message{Role: tacklr.RoleUser, Content: in.Prompt}, nil, durable.AuthContext{}, telemetry.TurnKindPrompt, nil)
		if terminal == durable.SessionFailed {
			if result != "" {
				return result, errors.New(result)
			}
			return "", errors.New("child failed")
		}
		return result, nil
	}

	for !closed {
		ev := wait()
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
			} else if nextAgentID != "" {
				agentID = nextAgentID
			}
			nextAgentID = ""
			if ev.prompt.MCPServers != nil {
				mcpServers = ev.prompt.MCPServers
			} else if nextMCP != nil {
				mcpServers = nextMCP
			}
			nextMCP = nil
			user := adapter.UserFromPrompt(ev.prompt.Text, ev.prompt.UserMessage)
			runSlice(user, nil, ev.prompt.Auth, telemetry.TurnKindPrompt, ev.prompt.State)
		case signalResume:
			yielded = false
			runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume, ev.resume.State)
		}
	}
	return result, nil
}

func enqueueSteer(
	p promptSignal,
	inbox *[]*tacklr.Message,
	mounts *[]durable.MountRecipe,
	turnState *map[string]any,
	nextAgentID *string,
	nextMCP *[]mcp.MCPConfig,
) {
	*inbox = adapter.AppendMessages(*inbox, adapter.UserFromPrompt(p.Text, p.UserMessage))
	*mounts = adapter.ApplyAuth(*mounts, p.Auth)
	*turnState = adapter.MergeUserState(*turnState, p.State)
	if p.AgentID != "" {
		*nextAgentID = p.AgentID
	}
	if p.MCPServers != nil {
		*nextMCP = p.MCPServers
	}
}

// waitSession is the idle/park demux. Temporal signal channels are named
// mailboxes (not opened/closed). ChildWaiting only updates spawned and loops.
func waitSession(
	ctx workflow.Context,
	promptCh, resumeCh, cancelCh, closeCh, childWaitCh workflow.ReceiveChannel,
	spawned []childRun,
) waitSignal {
	for {
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
		s.AddReceive(childWaitCh, func(c workflow.ReceiveChannel, more bool) {
			var id durable.SessionID
			c.Receive(ctx, &id)
			if i := findChild(spawned, id); i >= 0 {
				spawned[i].waiting = true
			}
		})
		s.Select(ctx)
		if out.kind != "" {
			return out
		}
	}
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
