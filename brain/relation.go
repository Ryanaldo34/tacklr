package brain

import (
	"fmt"
	"slices"
	"strings"
)

// RelationSpec is one relation an agent may link records with: its label,
// what it means, and the kinds each end may be. An empty From or To takes
// any kind.
type RelationSpec struct {
	Relation    string
	Description string
	From, To    []string
}

// Ends checks that a link of s may go from a record of kind from to one of
// kind to.
func (s RelationSpec) Ends(from, to string) error {
	if len(s.From) > 0 && !slices.Contains(s.From, from) {
		return fmt.Errorf("%w: %s links from %s, not %s", ErrInvalid, s.Relation, strings.Join(s.From, " or "), from)
	}
	if len(s.To) > 0 && !slices.Contains(s.To, to) {
		return fmt.Errorf("%w: %s links to %s, not %s", ErrInvalid, s.Relation, strings.Join(s.To, " or "), to)
	}
	return nil
}
