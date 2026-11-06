package orchestrator

import (
	"maps"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	adapter "github.com/ryanaldo34/tacklr/session/internal"
)

// ToolTimeoutMargin is what a runtime's Tool step adds to its tool's own
// timeout: the step also builds or reads the harness around the call.
const ToolTimeoutMargin = time.Minute

// Carry is a session's state across a suspend: everything the State holds
// that is not in the snapshot. A runtime keeps it when Run returns
// ErrSuspended (on Temporal, in the continue-as-new input) and starts the
// next Run from it with NewState. The session goes on with no step running:
// idle, or in a turn that waits on jobs (Suspended). Jobs carry by id and,
// when one already ended, its result.
type Carry struct {
	Mounts    []session.MountRecipe `json:"mounts,omitempty"`
	MCP       []session.MCPServer   `json:"mcp,omitempty"`
	Inbox     []*message.Message    `json:"inbox,omitempty"`
	NextMCP   []session.MCPServer   `json:"nextMcp,omitempty"`
	State     map[string]any        `json:"state,omitempty"`
	Round     *tacklr.Round         `json:"round,omitempty"`
	Rev       session.Revision      `json:"rev,omitempty"`
	Result    string                `json:"result,omitempty"`
	Terminal  session.State         `json:"terminal,omitempty"`
	Yielded   bool                  `json:"yielded,omitempty"`
	Finished  bool                  `json:"finished,omitempty"`
	Suspended bool                  `json:"suspended,omitempty"`
	Jobs      []Job                 `json:"jobs,omitempty"`
	Held      []session.Wake        `json:"held,omitempty"`
}

// Carry is t's state for the next Run.
func (t *State) Carry() Carry {
	return Carry{
		Mounts: t.Mounts, MCP: t.MCP, Inbox: t.Inbox, NextMCP: t.NextMCP, State: t.State,
		Round: t.Round, Rev: t.Rev, Result: t.Result, Terminal: t.Terminal, Yielded: t.Yielded,
		Finished: t.Finished, Suspended: t.Suspended, Jobs: t.Jobs, Held: t.held,
	}
}

// NewState is the State a session's Run starts from: its identity, the MCP
// servers and host state it was created with, and, when an earlier Run
// suspended, that Run's carry. Its mounts hold no credentials; each turn
// applies its own.
func NewState(id session.ID, ident Identity, mcp []session.MCPServer, state map[string]any, c *Carry) *State {
	ident.Mounts = adapter.ApplyAuth(ident.Mounts, session.AuthContext{})
	t := &State{SessionID: id, Identity: ident, MCP: mcp, State: maps.Clone(state)}
	if c != nil {
		t.Mounts, t.MCP, t.Inbox, t.NextMCP, t.State = c.Mounts, c.MCP, c.Inbox, c.NextMCP, c.State
		t.Round, t.Rev, t.Result, t.Terminal, t.Yielded = c.Round, c.Rev, c.Result, c.Terminal, c.Yielded
		t.Finished, t.Suspended, t.Jobs, t.held = c.Finished, c.Suspended, c.Jobs, c.Held
	}
	return t
}

// JobEnd is how a task session that Run ended tells its parent: its
// result, or why it failed.
func JobEnd(id session.ID, result string, err error) session.JobEnd {
	if err != nil {
		return session.JobEnd{ID: id, Result: session.JobResult{Output: err.Error(), Failed: true}}
	}
	return session.JobEnd{ID: id, Result: session.JobResult{Output: result}}
}

// TakeCancel takes the first cancel in queue, with the prompts queued
// before it: they steered the turn the cancel ends. It returns the wakes
// that stay, in order, the positions of those it took, and whether it found
// a cancel. A runtime calls it while a step runs, so the cancel
// stops the step.
func TakeCancel(queue []session.Wake) (kept []session.Wake, taken []int, ok bool) {
	i := -1
	for j, w := range queue {
		if w.Kind == session.WakeCancel {
			i = j
			break
		}
	}
	if i < 0 {
		return queue, nil, false
	}
	kept = make([]session.Wake, 0, len(queue)-1)
	for j, w := range queue {
		if j > i || (j < i && w.Kind != session.WakePrompt) {
			kept = append(kept, w)
			continue
		}
		taken = append(taken, j)
	}
	return kept, taken, true
}
