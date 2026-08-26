package drive

// Action is the wait-loop leftover/HITL decision. In-process and Temporal
// adapters interpret this; they do not fork leftover-tool rules.
type Action int

const (
	ActionInfer Action = iota
	ActionRunTools
	ActionYield
	ActionComplete
	ActionNudge
)

// Next chooses the next wait-loop step from leftover tools, park, inference
// completion, and remaining children. A later Restate/DBOS adapter must use
// this same decision so HITL and leftovers stay consistent.
func Next(runnable int, parked bool, inferComplete bool, childrenRemain bool) Action {
	if runnable > 0 {
		return ActionRunTools
	}
	if parked {
		return ActionYield
	}
	if inferComplete {
		if childrenRemain {
			return ActionNudge
		}
		return ActionComplete
	}
	return ActionInfer
}
