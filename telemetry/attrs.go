package telemetry

// Span names (use these; backends index span name).
//
// Trace shape:
//
//	tacklr.turn
//	  log: prompt.received | resume.received
//	  tacklr.model | tacklr.tool | tacklr.plan.install | tacklr.context.handoff
//	    tacklr.brain   (under tool when knowledge builtins run)
//	  log: turn.ended
//
// Set static attributes at span start. Outcome and error enums at end only.
// Do not use high-cardinality values as metric labels; use log events for free text.
// Lifecycle milestones use OTel Logs SetEventName, not span.AddEvent.
const (
	SpanTurn           = "tacklr.turn"
	SpanTool           = "tacklr.tool"
	SpanPlanInstall    = "tacklr.plan.install"
	SpanContextHandoff = "tacklr.context.handoff"
	SpanModel          = "tacklr.model"
	SpanBrain          = "tacklr.brain"
)

// Span and log attribute keys (static identifiers only).
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

	// Brain retrieval (tacklr.brain) — span attrs for trace debug only.
	// Metrics use LabelBrainOp / LabelDegrade / LabelEmpty (see RecordBrain).
	AttrBrainOp      = "tacklr.brain.op"      // see BrainOp* closed enum
	AttrBrainDegrade = "tacklr.brain.degrade" // none | lexical_only | containment_only
	AttrBrainHits    = "tacklr.brain.hits"    // page size; not a metric label (cardinality)

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

// Brain op values for AttrBrainOp / LabelBrainOp (closed enum).
// Keep in sync with brain.Op constants.
const (
	BrainOpSearch      = "search"
	BrainOpFindExact   = "find_exact"
	BrainOpFindObjects = "find_objects"
	BrainOpFindLinks   = "find_links"
	BrainOpContinue    = "continue"
	BrainOpExpand      = "expand"
	BrainOpExpandMany  = "expand_many"
)

// Brain degrade modes (closed enum).
const (
	BrainDegradeNone            = "none"
	BrainDegradeLexicalOnly     = "lexical_only"
	BrainDegradeContainmentOnly = "containment_only"
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
	EventFuseMount       = "vfs.fuse.mount"
	EventFuseMountError  = "vfs.fuse.mount_error"
	EventFuseUnmount     = "vfs.fuse.unmount"
	EventFuseUnavailable = "vfs.fuse.unavailable"
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
	AreaRuntime    = "runtime"
	AreaHarness    = "harness"
	AreaModelTasks = "model_tasks"
	AreaContext    = "context"
	AreaInference  = "inference"
	AreaBrain      = "brain"
)

// Outcome values for AttrOutcome / EventAttrOutcome.
const (
	OutcomeOK        = "ok"
	OutcomeError     = "error"
	OutcomeCancelled = "cancelled"
)
