package temporal

import (
	"context"
	"encoding/json"
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

const spawnWorkerName = tacklr.SpawnWorkerName

// SessionWorkflow is the harness wait loop: one Temporal workflow per agent session.
// It is the primary OpenTelemetry instrumentor: one tacklr.turn span per prompt or
// resume, with Inference/Tool activities as children via OTEL v2 propagation.
func SessionWorkflow(ctx workflow.Context, in WorkflowInput) error {
	logger := workflow.GetLogger(ctx)
	stream, err := workflowstreams.NewWorkflowStream(ctx, nil)
	if err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var (
		etag     string
		closed   bool
		agentID  = in.AgentID
		mcp      = in.MCPServers
		mounts   = durable.ApplyAuth(in.Mounts, in.Auth)
		lastAuth = in.Auth
		promptCh = workflow.GetSignalChannel(ctx, signalPrompt)
		resumeCh = workflow.GetSignalChannel(ctx, signalResume)
		cancelCh = workflow.GetSignalChannel(ctx, signalCancel)
		closeCh  = workflow.GetSignalChannel(ctx, signalClose)
	)
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
		sessionCtx, hasSession := openWorkerSession(ctx, in.WorkerSessionTimeout, 2*time.Second)

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
				if tc.Name == spawnWorkerName {
					var args struct {
						Task string `json:"task_description_and_context"`
					}
					_ = json.Unmarshal([]byte(tc.Arguments), &args)
					cwo := workflow.ChildWorkflowOptions{
						WorkflowID: string(in.SessionID) + "/worker/" + tc.Key(),
					}
					logInfo(sessionCtx, "child workflow",
						"workflow_id", cwo.WorkflowID, "agent_id", agentID,
					)
					cctx := workflow.WithChildOptions(sessionCtx, cwo)
					if err := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, WorkflowInput{
						SessionID:            durable.SessionID(cwo.WorkflowID),
						AgentID:              agentID,
						Prompt:               args.Task,
						Auth:                 lastAuth,
						Mounts:               mounts,
						WorkerSessionTimeout: in.WorkerSessionTimeout,
					}).Get(ctx, nil); err != nil {
						workflow.GetLogger(ctx).Error("child workflow", "error", err)
						stopSlice = onActErr(err)
						break
					}
					var cout ToolOutput
					err := waitAct("CommitToolOutput", CommitToolInput{
						SessionID:  in.SessionID,
						AgentID:    agentID,
						MCPServers: mcp,
						Etag:       etag,
						Call:       tc,
						Output:     "ok",
						Auth:       lastAuth,
						Mounts:     mounts,
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
				logInfo(ctx, "turn yielded", "agent_id", agentID, "session_id", in.SessionID, "interrupt_id", tout.InterruptID)
				closeTurn(telemetry.OutcomeYield, nil)
				parked := true
				for parked {
					ev := selectorWait()
					switch ev.kind {
					case signalResume:
						parked = false
						applyAuth(ev.resume.Auth)
						sessionCtx, hasSession = openWorkerSession(ctx, in.WorkerSessionTimeout, time.Minute)
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
			// Idle cancel is a no-op. Emitting here poisons the next prompt's
			// Subscribe(after Head) when Cancel raced with a just-finished turn.
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
			runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume)
		}
	}
	return nil
}

func turnCanceled(ctx workflow.Context, err error) bool {
	return err != nil && (ctx.Err() != nil || temporal.IsCanceledError(err) || errors.Is(err, workflow.ErrSessionFailed) || errors.Is(err, workflow.ErrCanceled))
}

// openWorkerSession pins activities to one worker when the host set a timeout.
// Timeout <= 0 skips CreateSession (no hidden default).
func openWorkerSession(ctx workflow.Context, timeout, creation time.Duration) (workflow.Context, bool) {
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
