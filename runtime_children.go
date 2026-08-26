package tacklr

import (
	"context"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
)

// childHost is the session-side implementation of HarnessRuntime child methods.
// Durable runtimes bind nested sessions; nil host means children are unavailable.
// Method names are exported so other packages can implement this by duck typing.
type childHost interface {
	SpawnChild(ctx context.Context, specialist, task, callID string) (string, error)
	Children() []Child
	CancelChild(ctx context.Context, id string) error
	// AwaitChild waits or collects. park=true means the child needs input:
	// the tool Parks child_waiting. An interrupt error parks as-is.
	AwaitChild(ctx context.Context, id, callID string) (child Child, park bool, err error)
}

// toolRuntime is the HarnessRuntime passed to tools: session emit/state/interrupt
// plus child operations. Durable drivers replace the host; nil host errors on spawn.
type toolRuntime struct {
	session.Runtime
	host childHost
}

func newToolRuntime(ch chan StreamEvent, sm *session.SessionManager, host childHost) toolRuntime {
	return toolRuntime{Runtime: session.NewRuntime(ch, sm), host: host}
}

func (t toolRuntime) WithToolCallID(id string) toolRuntime {
	t.Runtime = t.Runtime.WithToolCallID(id)
	return t
}

func (t toolRuntime) SpawnChild(ctx context.Context, specialist, task string) (string, error) {
	if t.host == nil {
		return "", fmt.Errorf("child sessions are not available: %w", ErrFailed)
	}
	return t.host.SpawnChild(ctx, specialist, task, t.CurrentToolCallID())
}

func (t toolRuntime) Children() []Child {
	if t.host == nil {
		return nil
	}
	return t.host.Children()
}

func (t toolRuntime) CancelChild(ctx context.Context, id string) error {
	if t.host == nil {
		return fmt.Errorf("child sessions are not available: %w", ErrFailed)
	}
	return t.host.CancelChild(ctx, id)
}

func (t toolRuntime) AwaitChild(ctx context.Context, id string) (Child, error) {
	if t.host == nil {
		return Child{}, fmt.Errorf("child sessions are not available: %w", ErrFailed)
	}
	child, park, err := t.host.AwaitChild(ctx, id, t.CurrentToolCallID())
	if err != nil {
		return child, err
	}
	if park {
		_, err := t.Park(interrupt.TypeChildWaiting, []byte(`{}`))
		return Child{}, err
	}
	return child, nil
}

func formatChildren(rows []Child) string {
	if len(rows) == 0 {
		return "No child sessions."
	}
	var b strings.Builder
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

func scheduledChildMessage(id, specialist string) string {
	return fmt.Sprintf("Child %s scheduled (specialist=%s). Use list_children to poll, get_child to collect its result (block=true to wait), or cancel_child to stop it.", id, specialist)
}

func spawnSpecialist(ctx context.Context, args spawnSpecialistArgs, runtime HarnessRuntime) (string, error) {
	spec := strings.TrimSpace(args.Specialist)
	task := strings.TrimSpace(args.TaskDescriptionAndContext)
	block := args.Block == nil || *args.Block
	id, err := runtime.SpawnChild(ctx, spec, task)
	if err != nil {
		return "", err
	}
	if !block {
		return scheduledChildMessage(id, spec), nil
	}
	return collectChild(runtime.AwaitChild(ctx, id))
}

func listChildren(_ context.Context, _ listJobsArgs, runtime HarnessRuntime) (string, error) {
	return formatChildren(runtime.Children()), nil
}

func getChild(ctx context.Context, args getJobArgs, runtime HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", fmt.Errorf("child_id is required; call list_children and pass a child_id from that list: %w", ErrInvalid)
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
	return collectChild(runtime.AwaitChild(ctx, id))
}

func cancelChild(ctx context.Context, args cancelJobArgs, runtime HarnessRuntime) (string, error) {
	id := strings.TrimSpace(args.ChildID)
	if id == "" {
		return "", fmt.Errorf("child_id is required; call list_children and pass a child_id from that list: %w", ErrInvalid)
	}
	if err := runtime.CancelChild(ctx, id); err != nil {
		return "", err
	}
	return fmt.Sprintf("Child %s cancelled and removed.", id), nil
}

func collectChild(child Child, err error) (string, error) {
	if err != nil {
		if child.Result != "" {
			return child.Result, err
		}
		return "", err
	}
	return child.Result, nil
}
