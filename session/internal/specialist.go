package adapter

import (
	"fmt"

	"github.com/ryanaldo34/tacklr"
)

// OverlaySpecialist is parent with the named specialist on it: the agent a
// specialist session runs.
func OverlaySpecialist(parent tacklr.AgentOptions, specialist string) (tacklr.AgentOptions, error) {
	spec := tacklr.FindSpecialist(parent.Specialists, specialist)
	if spec == nil {
		return tacklr.AgentOptions{}, fmt.Errorf("%w: specialist %q", tacklr.ErrNotFound, specialist)
	}
	out := parent.WithSpecialist(spec)
	return tacklr.BindTurn(out, "", nil), nil
}
