package tacklr

// Action is the wait-loop leftover/HITL decision. In-process and Temporal
// adapters interpret this; they do not fork leftover-tool rules.
type Action int

const (
	ActionInfer Action = iota
	ActionRunTools
	ActionYield
	ActionComplete
	ActionWait
)

// Next chooses the next wait-loop step from leftover tools, park, inference
// completion, and remaining jobs. A later Restate/DBOS adapter must use
// this same decision so HITL and leftovers stay consistent.
func Next(runnable int, parked bool, inferComplete bool, jobsRemain bool) Action {
	if runnable > 0 {
		return ActionRunTools
	}
	if parked {
		return ActionYield
	}
	if inferComplete {
		if jobsRemain {
			return ActionWait
		}
		return ActionComplete
	}
	return ActionInfer
}
