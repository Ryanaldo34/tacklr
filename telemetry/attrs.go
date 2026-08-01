package telemetry

// Span names for the agent-turn lifecycle. Prefer these over free-form method
// strings as attributes — backends index span name for search/filter.
//
// Intended trace shape (noise-free):
//
//	tacklr.turn
//	  event: prompt.received | resume.received
//	  tacklr.tool            (create_plan, work tools, complete_todo, …)
//	  tacklr.plan.install    (plan document placed in context)
//	  tacklr.context.handoff (after todo complete / plan revise)
//	  event: turn.ended
//
// Do not add spans for streaming messages, absorb, or compress — those are
// plumbing, not agent milestones.
const (
	SpanTurn           = "tacklr.turn"
	SpanTool           = "tacklr.tool"
	SpanPlanInstall    = "tacklr.plan.install"
	SpanContextHandoff = "tacklr.context.handoff"
)

// Span attribute keys: low-cardinality, searchable dimensions.
const (
	AttrArea        = "tacklr.area"
	AttrSessionID   = "tacklr.session_id"
	AttrAgentID     = "tacklr.agent_id"
	AttrThreadID    = "tacklr.thread_id"
	AttrTurnKind    = "tacklr.turn.kind" // prompt | resume
	AttrLoadSession = "tacklr.load_session"
	AttrToolName    = "tacklr.tool.name"
	AttrToolNS      = "tacklr.tool.namespace"
	AttrToolStatus  = "tacklr.tool.status" // success | error | interrupt | …
	AttrOpenTodos   = "tacklr.open_todos"  // remaining todos at handoff
	AttrOutcome     = "tacklr.outcome"     // ok | error | cancelled
)

// Span event names: lifecycle milestones with optional dynamic payload.
// Avoid start/finish event pairs that only restate the span itself.
const (
	EventPromptReceived = "prompt.received"
	EventResumeReceived = "resume.received"
	EventTurnEnded      = "turn.ended"
)

// Event attribute keys (only on AddEvent payloads).
const (
	EventAttrPromptLen            = "prompt_len"
	EventAttrResumeInterruptCount = "resume_interrupt_count"
	EventAttrOutcome              = "outcome" // ok | error | cancelled
)

// Area values for AttrArea.
const (
	AreaRegistry   = "registry"
	AreaHarness    = "harness"
	AreaModelTasks = "model_tasks"
	AreaContext    = "context"
)

// Outcome values for AttrOutcome / EventAttrOutcome.
const (
	OutcomeOK        = "ok"
	OutcomeError     = "error"
	OutcomeCancelled = "cancelled"
)
