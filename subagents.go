package tacklr

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ryanaldo34/tacklr/streaming"
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
	Block                     *bool  `json:"block" desc:"Wait for the worker and return its result. Defaults to true. Set false to start a child session and continue the turn."`
}

func (a *TurnManager) spawnTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "spawn_specialist",
		DisplayName: "Spawn {specialist}",
		Description: "Spawn a specialist as a child session. block defaults to true and returns the worker result before continuing. Set block=false to start the child and continue other work, then use list_children, get_child, or cancel_child.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     spawnSpecialist,
	})
}

type listChildrenArgs struct{}

type getChildArgs struct {
	ChildID string `json:"child_id" desc:"Child session id returned by spawn_specialist when block is false"`
	Block   bool   `json:"block" desc:"Wait for a running child before returning its result. Defaults to false."`
}

type cancelChildArgs struct {
	ChildID string `json:"child_id" desc:"Child session id returned by spawn_specialist when block is false"`
}

func (a *TurnManager) listChildrenTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "list_children",
		DisplayName: "List children",
		Description: "Non-blocking overview of child sessions (running, completed, failed). Status stays running while a child waits for user input. Use get_child to collect a result or wait, and cancel_child to stop work that is no longer needed.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     listChildren,
	})
}

func (a *TurnManager) getChildTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "get_child",
		DisplayName: "Get child {child_id}",
		Description: "Get one child session. By default this is non-blocking: a running child returns its current status (including while waiting for user input), while a terminal child returns and consumes its result. Set block=true to wait until it finishes, or to park this call if the child needs user input.",
		Category:    streaming.ToolCategoryExecute,
		Handler:     getChild,
	})
}

func (a *TurnManager) cancelChildTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "cancel_child",
		DisplayName: "Cancel child {child_id}",
		Description: "Cancel and remove a child session that is no longer needed. Completed and failed children are discarded without returning their result.",
		Category:    streaming.ToolCategoryExecute,
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
	out.disablePlanningLock = true
	return out
}
