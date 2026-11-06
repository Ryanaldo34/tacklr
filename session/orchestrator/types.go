package orchestrator

import (
	"github.com/ryanaldo34/tacklr/session"
)

// Identity is who a session is and what it mounts. The session's runtime
// holds it and gives it to each step, which builds the turn's harness from
// it; the snapshot store does not keep it.
type Identity struct {
	Specialist string
	// Worker, when set, means this session is a named JobHandler child.
	Worker string
	Parent session.ID
	Mounts []session.MountRecipe
	// Task says the session runs its first prompt, its task, then ends: a
	// child a tool started. Its runtime puts the task first in its mailbox.
	Task bool `json:",omitempty"`
}
