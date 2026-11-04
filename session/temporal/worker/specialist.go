package worker

import (
	"fmt"

	"github.com/ryanaldo34/tacklr"
)

// overlaySpecialist copies the named specialist onto the parent agent.
func overlaySpecialist(parent tacklr.AgentOptions, specialist string) (tacklr.AgentOptions, error) {
	spec := tacklr.FindSpecialist(parent.Specialists, specialist)
	if spec == nil {
		return tacklr.AgentOptions{}, fmt.Errorf("%w: specialist %q", tacklr.ErrNotFound, specialist)
	}
	out := parent.WithSpecialist(spec)
	return tacklr.BindTurn(out, "", nil), nil
}
