package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
)

type SubAgent struct {
	Tools        []*Tool
	Instructions string
	Model        InferenceStrategy
	WorkerName   string
	Description  string
}

func (h *AgentHarness) initSubAgentWorkers(specs []*SubAgent) {
	if h.subagents == nil {
		h.subagents = make(map[string]*AgentHarness)
	}
	if h.subagentDescriptions == nil {
		h.subagentDescriptions = make(map[string]string)
	}
	for _, spec := range specs {
		if spec == nil || spec.WorkerName == "" {
			continue
		}
		h.subagentDescriptions[spec.WorkerName] = spec.Description
		h.subagents[spec.WorkerName] = &AgentHarness{
			Model:        spec.Model,
			Instructions: spec.Instructions,
			MCPConfigs:   h.MCPConfigs,
			Tools:        spec.Tools,
			Store:        h.Store,
			Runtime:      h.Runtime,
		}
	}
}

type spawnWorkerArgs struct {
	TaskDescriptionAndContext string `json:"task_description_and_context"`
	WorkerName                string `json:"worker_name"`
}

func (a *AgentHarness) spawnTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "spawn_worker",
		DisplayName: "Spawn Worker",
		Description: "Use to spawn a sub-agent or \"worker\" to help parallelize a task or to-do smaller subtasks and assist with research. Ensure the task is clearly outlined with a clear goal, acceptance criteria, and helpful context applied",
		Category:    streaming.ToolCategoryExecute,
		Handler: func(ctx context.Context, args spawnWorkerArgs, runtime HarnessRuntime) (string, error) {
			spec, ok := a.subagents[args.WorkerName]
			if !ok {
				return "", fmt.Errorf("worker %q not found", args.WorkerName)
			}

			slog.Info("spawning worker", "worker", args.WorkerName)

			workerCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			worker := NewAgent(workerCtx, AgentOptions{
				Config: Config{
					MaxWindowSize: a.MaxWindowSize,
					SystemPrompt:  spec.Instructions,
				},
				Model:      spec.Model,
				Tools:      spec.Tools,
				MCPConfigs: a.MCPConfigs,
			})
			defer worker.Close()

			events, err := worker.Run(workerCtx, args.TaskDescriptionAndContext)
			if err != nil {
				return "", fmt.Errorf("starting worker %q: %w", args.WorkerName, err)
			}
			runtime.EmitUpdate(fmt.Sprintf("Worker %q started", args.WorkerName))

			start := time.Now()
			var result string

			func() {
				for ev := range events {
					select {
					case <-ctx.Done():
						result = ""
						return
					default:
					}

					switch ev.Type {
					case StreamEventError:
						slog.Warn("worker failed", "worker", args.WorkerName, "error", ev.Error)
						result = ""
						return
					case StreamEventMessage:
						if ev.Content != "" {
							runtime.EmitUpdate(fmt.Sprintf("[%s] %s", args.WorkerName, ev.Content))
						}
					case StreamEventReasoning:
						if ev.Content != "" {
							runtime.EmitUpdate(fmt.Sprintf("[%s thinking] %s", args.WorkerName, ev.Content))
						}
					case StreamEventFunctionCall:
						for _, tc := range ev.ToolCalls {
							runtime.EmitUpdate(fmt.Sprintf("[%s] tool call: %s", args.WorkerName, tc.Name))
						}
					}
				}
			}()

			if ctx.Err() != nil {
				return "", ctx.Err()
			}

			for i := len(worker.ContextWindow) - 1; i >= 0; i-- {
				msg := worker.ContextWindow[i]
				if (msg.Role == RoleAssistant || msg.Role == RoleReasoning) && msg.Content != "" {
					result = msg.Content
					break
				}
			}

			elapsed := time.Since(start).Round(time.Millisecond)
			slog.Info("worker completed", "worker", args.WorkerName, "elapsed", elapsed, "output_length", len(result))

			if result == "" {
				return "", fmt.Errorf("worker %q finished but produced no output", args.WorkerName)
			}
			return result, nil
		},
	})
}
