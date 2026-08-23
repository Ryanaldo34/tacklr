package temporal

import (
	"context"
	"encoding/json"
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
	emitCancel := func() {
		if stream == nil {
			return
		}
		_ = stream.Topic(durable.TopicEvents).Publish(streaming.StreamEvent{
			Type:    streaming.StreamEventError,
			Fail:    context.Canceled.Error(),
			Content: context.Canceled.Error(),
		})
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
		sessionCtx := ctx
		if sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
			CreationTimeout:  2 * time.Second,
			ExecutionTimeout: 10 * time.Minute,
		}); err != nil {
			logError(ctx, "worker session", "error", err)
		} else {
			sessionCtx = sctx
		}

		var endTurn func(string, error)
		openTurn := func(k string) {
			var spanCtx workflow.Context
			spanCtx, endTurn = startTurn(sessionCtx, agentID, in.SessionID, k)
			sessionCtx = spanCtx
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
			if sessionCtx != ctx {
				workflow.CompleteSession(sessionCtx)
			}
		}()

		actCtx := workflow.WithActivityOptions(sessionCtx, activityOpts)
		waitAct := func(name string, arg any, result any) error {
			cctx, cancelAct := workflow.WithCancel(actCtx)
			fut := workflow.ExecuteActivity(cctx, name, arg)
			var err error
			s := workflow.NewSelector(ctx)
			s.AddFuture(fut, func(f workflow.Future) { err = f.Get(ctx, result) })
			s.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
				cancelAct()
				err = fut.Get(ctx, result)
			})
			s.Select(ctx)
			drainCancels()
			return err
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
				if err != nil {
					if turnCanceled(ctx, err) {
						outcome = telemetry.OutcomeCancelled
					} else {
						outcome = telemetry.OutcomeError
						turnErr = err
					}
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
			for _, tc := range next {
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
						SessionID: durable.SessionID(cwo.WorkflowID),
						AgentID:   agentID,
						Prompt:    args.Task,
						Auth:      lastAuth,
						Mounts:    mounts,
					}).Get(ctx, nil); err != nil {
						logError(ctx, "child workflow", "error", err)
						outcome, turnErr, stopSlice = telemetry.OutcomeError, err, true
						break
					}
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
				if err != nil {
					if turnCanceled(ctx, err) {
						outcome = telemetry.OutcomeCancelled
					} else {
						outcome, turnErr = telemetry.OutcomeError, err
					}
					stopSlice = true
					break
				}
				etag = tout.Etag
				if !tout.Interrupted {
					continue
				}
				if sessionCtx != ctx {
					workflow.CompleteSession(sessionCtx)
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
						sessionCtx, err = workflow.CreateSession(ctx, &workflow.SessionOptions{
							CreationTimeout:  time.Minute,
							ExecutionTimeout: 10 * time.Minute,
						})
						if err != nil {
							sessionCtx = ctx
						}
						logInfo(ctx, "turn start",
							"kind", telemetry.TurnKindResume, "agent_id", agentID,
							"session_id", in.SessionID, "resume_count", len(ev.resume.Responses),
						)
						openTurn(telemetry.TurnKindResume)
						outcome, turnErr = telemetry.OutcomeOK, nil
						actCtx = workflow.WithActivityOptions(sessionCtx, activityOpts)
						var iout InferenceOutput
						err = workflow.ExecuteActivity(actCtx, "Inference", InferenceInput{
							SessionID:  in.SessionID,
							AgentID:    agentID,
							MCPServers: mcp,
							Etag:       etag,
							Resume:     ev.resume.Responses,
							Auth:       lastAuth,
							Mounts:     mounts,
						}).Get(ctx, &iout)
						if err != nil {
							if turnCanceled(ctx, err) {
								outcome = telemetry.OutcomeCancelled
							} else {
								outcome, turnErr = telemetry.OutcomeError, err
							}
							stopSlice = true
							break
						}
						etag = iout.Etag
						pending = iout.ToolCalls
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
	return err != nil && (ctx.Err() != nil || temporal.IsCanceledError(err))
}
