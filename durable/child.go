package durable

import (
	"context"
	"fmt"
	"strings"
)

// JobHandler runs a named background job. Register on inprocess/temporal Config.Jobs.
type JobHandler func(ctx context.Context, task string) (string, error)

// ChildSessionID is the stable id for a spawn_specialist child session.
func ChildSessionID(parent SessionID, specialist, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/w/%s/%s", parent, strings.TrimSpace(specialist), strings.TrimSpace(callID)))
}

// JobID is the stable id for a named background job (not a child session).
func JobID(parent SessionID, name, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/j/%s/%s", parent, strings.TrimSpace(name), strings.TrimSpace(callID)))
}
