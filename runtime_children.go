package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
)

// ChildHost is the session-side implementation of HarnessRuntime child methods.
// Durable runtimes bind nested sessions; nil host means children are unavailable.
type ChildHost interface {
	SpawnChild(ctx context.Context, specialist, task, callID string) (string, error)
	Children() []Child
	CancelChild(ctx context.Context, id string) error
	// AwaitChild waits or collects. A *interrupt.ChildWaiting error means the
	// child needs input: the wrapper Parks it. Other errors pass through.
	AwaitChild(ctx context.Context, id, callID string) (child Child, err error)
}

// toolRuntime is the HarnessRuntime passed to tools: session emit/state/interrupt
// plus child operations. Durable drivers replace the host; nil host errors on spawn.
type toolRuntime struct {
	session.Runtime
	host ChildHost
}

func newToolRuntime(ch chan StreamEvent, sm *session.SessionManager, host ChildHost) toolRuntime {
	return toolRuntime{Runtime: session.NewRuntime(ch, sm), host: host}
}

func (t toolRuntime) WithToolCallID(id string) toolRuntime {
	t.Runtime = t.Runtime.WithToolCallID(id)
	return t
}

func (t toolRuntime) requireHost() (ChildHost, error) {
	if t.host == nil {
		return nil, fmt.Errorf("child sessions are not available: %w", ErrFailed)
	}
	return t.host, nil
}

func (t toolRuntime) SpawnChild(ctx context.Context, specialist, task string) (string, error) {
	host, err := t.requireHost()
	if err != nil {
		return "", err
	}
	return host.SpawnChild(ctx, specialist, task, t.CurrentToolCallID())
}

func (t toolRuntime) Children() []Child {
	if t.host == nil {
		return nil
	}
	return t.host.Children()
}

func (t toolRuntime) CancelChild(ctx context.Context, id string) error {
	host, err := t.requireHost()
	if err != nil {
		return err
	}
	return host.CancelChild(ctx, id)
}

func (t toolRuntime) AwaitChild(ctx context.Context, id string) (Child, error) {
	host, err := t.requireHost()
	if err != nil {
		return Child{}, err
	}
	child, err := host.AwaitChild(ctx, id, t.CurrentToolCallID())
	if err != nil {
		var waiting *interrupt.ChildWaiting
		if errors.As(err, &waiting) {
			_, err := t.Park(interrupt.TypeChildWaiting, []byte(`{}`))
			return Child{}, err
		}
		return child, err
	}
	return child, nil
}

func formatChildren(rows []Child) string {
	if len(rows) == 0 {
		return "No child sessions."
	}
	var b strings.Builder
	b.Grow(80 + 64*len(rows))
	b.WriteString("Child sessions:\n")
	for _, c := range rows {
		fmt.Fprintf(&b, "- id=%s specialist=%s status=%s\n", c.ID, c.Specialist, c.State)
	}
	b.WriteString("Use get_child to collect a result (block=true to wait), or cancel_child to stop a child.")
	return b.String()
}

func formatChild(c Child) string {
	return fmt.Sprintf("id=%s specialist=%s status=%s\nStill running. Call get_child again later, or set block=true to wait until finished.",
		c.ID, c.Specialist, c.State)
}

func unknownChildErr(name string, err error) error {
	if errors.Is(err, ErrNotFound) {
		return Correctionf(ErrNotFound, "%s: that child_id is unknown. Call list_children, then get_child or cancel_child with an id from that list", name)
	}
	return err
}

func scheduledChildMessage(id, specialist string) string {
	return fmt.Sprintf("Child %s scheduled (specialist=%s). Use list_children to poll, get_child to collect its result (block=true to wait), or cancel_child to stop it.", id, specialist)
}

func spawnSpecialist(ctx context.Context, args spawnSpecialistArgs, runtime HarnessRuntime) (string, error) {
	spec := strings.TrimSpace(args.Specialist)
	task := strings.TrimSpace(args.TaskDescriptionAndContext)
	if task == "" {
		return "", Correctionf(ErrInvalid, "spawn_specialist: task_description_and_context is required. Describe the worker's goal and constraints")
	}
	block := args.Block == nil || *args.Block
	id, err := runtime.SpawnChild(ctx, spec, task)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", Correctionf(ErrNotFound, "spawn_specialist: that specialist is not registered. Pass a name from the available specialists")
		}
		if errors.Is(err, ErrInvalid) {
			return "", Correctionf(ErrInvalid, "spawn_specialist: specialist is required. Pass a name from the available specialists")
		}
		return "", err
	}
	if !block {
		return scheduledChildMessage(id, spec), nil
	}
	child, err := runtime.AwaitChild(ctx, id)
	if err != nil {
		return "", unknownChildErr("spawn_specialist", err)
	}
	return child.Result, nil
}

func listChildren(_ context.Context, _ listChildrenArgs, runtime HarnessRuntime) (string, error) {
	return formatChildren(runtime.Children()), nil
}

func getChild(ctx context.Context, args getChildArgs, runtime HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", Correctionf(ErrInvalid, "get_child: child_id is required. Call list_children and pass a child_id from that list")
	}
	if !args.Block {
		for _, c := range runtime.Children() {
			if c.ID == id {
				if c.State == ChildRunning {
					return formatChild(c), nil
				}
				break
			}
		}
	}
	child, err := runtime.AwaitChild(ctx, id)
	if err != nil {
		return "", unknownChildErr("get_child", err)
	}
	return child.Result, nil
}

func cancelChild(ctx context.Context, args cancelChildArgs, runtime HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", Correctionf(ErrInvalid, "cancel_child: child_id is required. Call list_children and pass a child_id from that list")
	}
	if err := runtime.CancelChild(ctx, id); err != nil {
		return "", unknownChildErr("cancel_child", err)
	}
	return fmt.Sprintf("Child %s cancelled and removed.", id), nil
}
