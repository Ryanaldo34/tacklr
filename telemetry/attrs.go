package telemetry

// Span names. A turn is one trace, from the caller's request to the
// answer, across every process and runtime it touches:
//
//	invoke_agent {agent}        INTERNAL the turn (gen_ai.* invoke_agent)
//	  chat {model}              CLIENT   one model request (gen_ai.* chat)
//	  execute_tool {tool}       INTERNAL one tool call (gen_ai.* execute_tool)
//	  tacklr.context.handoff    INTERNAL a plan handoff, with its model request
//	  invoke_agent {specialist} INTERNAL a child session's turn
//
// The span names of the turn, the model request and the tool call are the
// OpenTelemetry GenAI semantic conventions'. Each name is the operation
// and its static target, so names stay few.
//
// Attributes on spans are static and searchable: identifiers, closed enums,
// and the outcome and error.type set at the end. Values that change within
// an operation or are free text (lengths, counts, error messages, answers)
// go on span-correlated logs (EmitEvent, slog with the context), never on
// spans. Metric labels are closed enums and configuration ids only: never
// a session id.
const (
	SpanHandoff = "tacklr.context.handoff"
)

// GenAI operation names (gen_ai.operation.name), which name their spans.
const (
	OperationInvokeAgent = "invoke_agent"
	OperationChat        = "chat"
	OperationExecuteTool = "execute_tool"
)

// Attribute keys of the OpenTelemetry semantic conventions.
// https://opentelemetry.io/docs/specs/semconv/gen-ai/
const (
	AttrGenAIOperationName    = "gen_ai.operation.name"
	AttrGenAIProviderName     = "gen_ai.provider.name"
	AttrGenAIRequestModel     = "gen_ai.request.model"
	AttrGenAIAgentName        = "gen_ai.agent.name"
	AttrGenAIConversationID   = "gen_ai.conversation.id"
	AttrGenAIToolName         = "gen_ai.tool.name"
	AttrGenAIToolCallID       = "gen_ai.tool.call.id"
	AttrGenAIToolType         = "gen_ai.tool.type"
	AttrGenAITokenType        = "gen_ai.token.type"
	AttrGenAIInputTokens      = "gen_ai.usage.input_tokens"
	AttrGenAIOutputTokens     = "gen_ai.usage.output_tokens"
	AttrGenAICachedTokens     = "gen_ai.usage.cached_tokens"
	AttrGenAICacheWriteTokens = "gen_ai.usage.cache_write_tokens"
	AttrErrorType             = "error.type"
	AttrHTTPStatus            = "http.response.status_code"
)

// Tacklr's own attribute keys (static identifiers and closed enums).
const (
	AttrTurnKind      = "tacklr.turn.kind" // prompt | resume | continue
	AttrRuntime       = "tacklr.runtime"   // temporal | durableobject
	AttrParentSession = "tacklr.session.parent"
	AttrToolNS        = "tacklr.tool.namespace"
	AttrModelPhase    = "tacklr.model.phase" // turn | handoff | compress
	AttrOutcome       = "tacklr.outcome"
	AttrErrorCode     = "tacklr.error.code" // the provider's error code
)

// Runtime values for AttrRuntime (closed enum).
const (
	RuntimeTemporal      = "temporal"
	RuntimeDurableObject = "durableobject"
)

// Model phase values for AttrModelPhase (closed enum).
const (
	ModelPhaseTurn     = "turn"
	ModelPhaseHandoff  = "handoff"
	ModelPhaseCompress = "compress"
)

// GenAI provider values (closed enum).
const (
	GenAIProviderAnthropic = "anthropic"
	GenAIProviderAzure     = "azure.openai"
	GenAIProviderOpenAI    = "openai"
	GenAIProviderUnknown   = "unknown"
)

// GenAI tool types (gen_ai.tool.type): a host's function, or a tool an MCP
// server provides.
const (
	ToolTypeFunction  = "function"
	ToolTypeExtension = "extension"
)

// Error class buckets for error.type (closed enum).
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

// Log event names (OTel Logs Record.SetEventName): what happened within an
// operation, with its dynamic values as attributes.
const (
	EventModelRequest   = "gen_ai.request"
	EventProviderFailed = "provider.failed"
	EventToolFailed     = "tool.failed"
	EventPlanInstalled  = "plan.installed"
	EventTurnStarted    = "turn.started"
	EventTurnEnded      = "turn.ended"
)

// Attribute keys on log events (dynamic values).
const (
	EventAttrOutcome      = "outcome"
	EventAttrInputItems   = "input_items"
	EventAttrMessages     = "context.messages"
	EventAttrToolPairs    = "context.tool_pairs"
	EventAttrAfterTools   = "after_tools"
	EventAttrSeq          = "seq"
	EventAttrPromptLength = "prompt.length"
	EventAttrResumes      = "resumes"
	EventAttrTodos        = "todos"
	EventAttrOpenTodos    = "todos.open"
	EventAttrError        = "error"
)

// Area values for the "area" field of Tacklr's slog records: which part of
// Tacklr logged it.
const (
	AreaRuntime    = "runtime"
	AreaHarness    = "harness"
	AreaModelTasks = "model_tasks"
	AreaContext    = "context"
)

// Outcome values for AttrOutcome / EventAttrOutcome.
const (
	OutcomeOK        = "ok"
	OutcomeError     = "error"
	OutcomeCancelled = "cancelled"
	OutcomeYield     = "yield"
)
