package durable

import (
	"fmt"
	"strings"
)

// ChildSessionID is the stable id for a spawn_specialist child session.
func ChildSessionID(parent SessionID, specialist, callID string) SessionID {
	return SessionID(fmt.Sprintf("%s/w/%s/%s", parent, strings.TrimSpace(specialist), strings.TrimSpace(callID)))
}
