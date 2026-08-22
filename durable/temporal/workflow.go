package temporal

import (
	"encoding/json"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
)

const spawnWorkerName = tacklr.SpawnWorkerName

// SessionWorkflow is the harness wait loop: one Temporal workflow per agent session.
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
	_ = stream

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

	runSlice := func(user *streaming.Message, resume map[string][]byte, auth durable.AuthContext) {
		applyAuth(auth)
		sessionCtx := ctx
		if sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
			CreationTimeout:  2 * time.Second,
			ExecutionTimeout: 10 * time.Minute,
		}); err != nil {
			logger.Error("worker session", "error", err)
		} else {
			sessionCtx = sctx
		}
		actCtx := workflow.WithActivityOptions(sessionCtx, activityOpts)
		hadTools := false
		reqs := 0
		pending := []streaming.ToolCall(nil)
		for {
			if len(pending) == 0 {
				var out InferenceOutput
				err := workflow.ExecuteActivity(actCtx, "Inference", InferenceInput{
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
				}).Get(ctx, &out)
				user = nil
				resume = nil
				if err != nil {
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
					cctx := workflow.WithChildOptions(ctx, cwo)
					if err := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, WorkflowInput{
						SessionID: durable.SessionID(cwo.WorkflowID),
						AgentID:   agentID,
						Prompt:    args.Task,
						Auth:      lastAuth,
						Mounts:    mounts,
					}).Get(ctx, nil); err != nil {
						logger.Error("child workflow", "error", err)
						stopSlice = true
						break
					}
					continue
				}
				var tout ToolOutput
				err := workflow.ExecuteActivity(actCtx, "Tool", ToolInput{
					SessionID:  in.SessionID,
					AgentID:    agentID,
					MCPServers: mcp,
					Etag:       etag,
					Call:       tc,
					Auth:       lastAuth,
					Mounts:     mounts,
				}).Get(ctx, &tout)
				if err != nil {
					stopSlice = true
					break
				}
				etag = tout.Etag
				if !tout.Interrupted {
					continue
				}
				if sessionCtx != ctx {
					workflow.CompleteSession(sessionCtx)
				}
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
							stopSlice = true
							break
						}
						etag = iout.Etag
						pending = iout.ToolCalls
					case signalClose:
						closed = true
						stopSlice = true
						parked = false
					case signalCancel:
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
		if sessionCtx != ctx {
			workflow.CompleteSession(sessionCtx)
		}
	}

	if in.Prompt != "" {
		runSlice(&streaming.Message{Role: streaming.RoleUser, Content: in.Prompt}, nil, in.Auth)
		return nil
	}

	for !closed {
		ev := selectorWait()
		switch ev.kind {
		case signalClose:
			closed = true
		case signalCancel:
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
			runSlice(user, nil, ev.prompt.Auth)
		case signalResume:
			runSlice(nil, ev.resume.Responses, ev.resume.Auth)
		}
	}
	return nil
}
