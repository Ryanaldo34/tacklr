package telemetry

// Span names for the agent-turn lifecycle. Prefer these over free-form method
// strings as attributes — backends index span name for search/filter.
//
// Intended trace shape (noise-free):
//
//	tacklr.turn
//	  log-event: prompt.received | resume.received   (OTel Logs API, span-correlated)
//	  tacklr.model           (turn | handoff | compress Invoke)
//	  tacklr.tool            (create_plan, work tools, complete_todo, …)
//	  tacklr.plan.install    (plan document placed in context)
//	  tacklr.context.handoff (after todo complete / plan revise)
//	  log-event: turn.ended
//
// Span attribute keys are package constants (static). Prefer setting known
// dimensions at span start (WithAttributes). Outcome/error enums at end only.
// High-cardinality values (prompts, call_ids, free-text errors) must not be
// metric labels; prefer log events for free-text detail.
//
// Prefer OTel Logs API records with SetEventName for lifecycle milestones
// (not span.AddEvent).
const (
	SpanTurn           = "tacklr.turn"
	SpanTool           = "tacklr.tool"
	SpanPlanInstall    = "tacklr.plan.install"
	SpanContextHandoff = "tacklr.context.handoff"
	SpanModel          = "tacklr.model"
)

// Span / log attribute keys (static identifiers only).
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
	AttrOutcome     = "tacklr.outcome"     // ok | error | cancelled | fallback

	// Model invoke (tacklr.model) — start attrs where possible.
	AttrModelPhase       = "tacklr.model.phase" // turn | handoff | compress
	AttrModelSeq         = "tacklr.model.seq"
	AttrContextMsgs      = "tacklr.context.messages"
	AttrContextToolPairs = "tacklr.context.tool_pairs"
	AttrHTTPStatus       = "tacklr.http.status"
	AttrErrorCode        = "tacklr.error.code"
	AttrErrorClass       = "tacklr.error.class" // bucketed enum
	AttrAfterTools       = "tacklr.model.after_tools"

	// OpenTelemetry GenAI semantic conventions (stable keys).
	// https://opentelemetry.io/docs/specs/semconv/gen-ai/
	AttrGenAIOperationName = "gen_ai.operation.name"
	AttrGenAIProviderName  = "gen_ai.provider.name"
	AttrGenAIRequestModel  = "gen_ai.request.model"
	AttrGenAIInputTokens   = "gen_ai.usage.input_tokens"
	AttrGenAIOutputTokens  = "gen_ai.usage.output_tokens"
)

// Model phase values for AttrModelPhase (closed enum).
const (
	ModelPhaseTurn     = "turn"
	ModelPhaseHandoff  = "handoff"
	ModelPhaseCompress = "compress"
)

// GenAI operation / provider values (closed enums for low cardinality).
const (
	GenAIOperationChat   = "chat"
	GenAIProviderAzure   = "azure.openai"
	GenAIProviderOpenAI  = "openai"
	GenAIProviderUnknown = "unknown"
)

// Error class buckets for metrics/span attrs (closed enum).
const (
	ErrorClassOK          = "ok"
	ErrorClassProvider4xx = "provider_4xx"
	ErrorClassProvider5xx = "provider_5xx"
	ErrorClassMaxTokens   = "max_tokens"
	ErrorClassCancelled   = "cancelled"
	ErrorClassTimeout     = "timeout"
	ErrorClassOther       = "other"
)

// Handoff outcome values.
const (
	HandoffOutcomeOK       = "ok"
	HandoffOutcomeFallback = "fallback"
	HandoffOutcomeError    = "error"
)

// Log event names (OTel Logs API Record.SetEventName).
const (
	EventPromptReceived  = "prompt.received"
	EventResumeReceived  = "resume.received"
	EventTurnEnded       = "turn.ended"
	EventProviderFailed  = "provider.failed"
	EventModelAfterTools = "model.after_tools"
)

// Event attribute keys on log-based events.
const (
	EventAttrPromptLen            = "prompt_len"
	EventAttrResumeInterruptCount = "resume_interrupt_count"
	EventAttrOutcome              = "outcome"
	EventAttrBodySnip             = "body_snip"
	EventAttrInputItems           = "input_items"
)

// Area values for AttrArea.
const (
	AreaRegistry   = "registry"
	AreaHarness    = "harness"
	AreaModelTasks = "model_tasks"
	AreaContext    = "context"
	AreaInference  = "inference"
)

// Outcome values for AttrOutcome / EventAttrOutcome.
const (
	OutcomeOK        = "ok"
	OutcomeError     = "error"
	OutcomeCancelled = "cancelled"
)
