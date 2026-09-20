package tacklr

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Specialist describes a nested session a harness can spawn via spawn_specialist.
// Specs may nest via Specialists. Child sessions inherit the parent world through
// AgentOptions.WithSpecialist (VFS, brain, MCP, interceptors). Spec fields replace
// model, instructions, tools, and nested Specialists. They skip planningWriteLock.
type Specialist struct {
	Tools        []*Tool
	Instructions string
	Model        InferenceStrategy
	Name         string
	Description  string
	// Specialists are nested workers available to this worker when it runs.
	Specialists []*Specialist
}

// initSpecialists registers worker specs. Invalid or duplicate specs are
// constructor errors: a misconfigured host must not start a harness that
// silently drops workers.
func (h *TurnManager) initSpecialists(specs []*Specialist) error {
	for _, spec := range specs {
		if spec == nil {
			return fmt.Errorf("tacklr: Specialist must not be nil")
		}
		if spec.Name == "" {
			return fmt.Errorf("tacklr: Specialist.Name is required")
		}
		if spec.Model == nil {
			return fmt.Errorf("tacklr: Specialist.Model is required")
		}
		if _, exists := h.specialists[spec.Name]; exists {
			return fmt.Errorf("tacklr: duplicate Specialist name %s", spec.Name)
		}
		cp := *spec
		h.specialists[spec.Name] = &cp
	}
	return nil
}

// formatSpecialistPromptList builds the deterministic AVAILABLE SPECIALISTS list.
func (a *TurnManager) formatSpecialistPromptList() string {
	names := slices.Sorted(maps.Keys(a.specialists))
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	for _, name := range names {
		spec := a.specialists[name]
		if spec.Description != "" {
			fmt.Fprintf(&b, " - %s: %s\n", name, spec.Description)
		} else {
			fmt.Fprintf(&b, " - %s\n", name)
		}
	}
	return b.String()
}

type spawnSpecialistArgs struct {
	TaskDescriptionAndContext string `json:"task_description_and_context" desc:"Clear task goal, acceptance criteria, and helpful context for the worker"`
	Specialist                string `json:"specialist" desc:"Name of a registered specialist to spawn"`
	Block                     *bool  `json:"block" desc:"Wait for the worker and return its result. Defaults to true. Set false to start a job and continue the turn; the result arrives as a later message."`
}

func (a *TurnManager) spawnTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "spawn_specialist",
		DisplayName: "Spawn {specialist}",
		Description: "Run a named specialist on a focused subtask when work can proceed in parallel or needs substantial research or analysis and you only need the final output. When block is true (the default), waits and returns the specialist's result. When block is false, schedules a job and returns a job id; the result arrives later as a message and the current turn can continue. Fails if the specialist is unknown or the task is empty.",
		Category:    ToolCategoryExecute,
		Handler: func(ctx context.Context, args spawnSpecialistArgs, runtime HarnessRuntime) (string, error) {
			out, err := spawnSpecialist(ctx, args, runtime)
			if err == nil {
				a.retainCollapse(ctx, collapseEvent{Trigger: triggerSpecialist, Body: out})
			}
			return out, err
		},
	})
}

type listChildrenArgs struct{}

type cancelChildArgs struct {
	ChildID string `json:"child_id" desc:"Job id returned by spawn_specialist when block is false"`
}

func (a *TurnManager) listChildrenTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "list_children",
		DisplayName: "List jobs",
		Description: "Return the current jobs (id, name, status) without waiting. Status stays running while a specialist is waiting for user input. Returns \"No jobs.\" when none exist, otherwise a list. Finished job results arrive as later messages; this call does not return those bodies.",
		Category:    ToolCategoryExecute,
		Handler:     listChildren,
	})
}

func (a *TurnManager) cancelChildTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "cancel_child",
		DisplayName: "Cancel job {child_id}",
		Description: "Cancel a job and remove it. Call when that work is no longer needed. Returns that the job was cancelled and removed. Completed and failed jobs are discarded without returning their result. Fails if child_id is empty or unknown.",
		Category:    ToolCategoryExecute,
		Handler:     cancelChild,
	})
}

// FindSpecialist returns the named worker from specs, including nested Specialists.
func FindSpecialist(specs []*Specialist, name string) *Specialist {
	name = strings.TrimSpace(name)
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		if spec.Name == name {
			return spec
		}
		if found := FindSpecialist(spec.Specialists, name); found != nil {
			return found
		}
	}
	return nil
}

// WithSpecialist overlays a worker spec onto the parent session world. The child
// keeps parent MCP, brain, interceptors, and skills (SkillsSession / SkillsLoader).
// Model, tools, nested workers, and instructions come from spec. Planning write
// lock is off. MountSession, SkillsSession, and SessionID stay as the caller
// set them (Runtime injects a child tree).
func (o AgentOptions) WithSpecialist(spec *Specialist) AgentOptions {
	out := o
	out.Config.SystemPrompt = spec.Instructions
	if spec.Model != nil {
		out.Model = spec.Model
	}
	out.Tools = slices.Clone(spec.Tools)
	out.Specialists = spec.Specialists
	out.skipPlanningLock = true
	return out
}
