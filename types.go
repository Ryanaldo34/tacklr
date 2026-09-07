package tacklr

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// Shared errors, inference contract, and interrupt re-exports for harness hosts.
// Message, StreamEvent, Todo, and related conversation types live in this package.

// Coarse categories for errors.Is. Wrap a specific message at the call site
// (fmt.Errorf("tool %q: %w", name, ErrNotFound)) instead of a sentinel per
// situation. Named sentinels below are distinct handling branches, not children
// of these categories.
//
// ErrCorrection is a model-facing tool failure: Error() is the correction the model
// should follow. Construct with Correction(cause, msg). Distinct from ErrFailed
// (harness/runtime). errors.Is matches both ErrCorrection and cause.
var (
	ErrNotFound    = errors.New("not found")
	ErrInvalid     = errors.New("invalid")
	ErrFailed      = errors.New("failed")
	ErrCorrection  = errors.New("correction")
	ErrAuthExpired = errors.New("auth expired")
)

var (
	ErrModelRefused         = errors.New("model refused")
	ErrMaxTokens            = errors.New("max tokens reached")
	ErrMaxTurnRequests      = errors.New("max turn model requests exceeded")
	ErrModelAfterTools      = errors.New("model request failed after tools completed")
	ErrApiKeyNotSet         = errors.New("api key not set")
	ErrModelNotSet          = errors.New("model not set")
	ErrUnknownModel         = errors.New("unknown model")
	ErrToolTimeout          = errors.New("tool timed out")
	ErrToolPermissionDenied = errors.New("tool permission denied")
)

// ProviderStatus supplies HTTP status and error code from a provider error.
// Optional on InferenceStrategy errors for model-span attributes.
type ProviderStatus interface {
	ProviderHTTPStatus() int
	ProviderErrorCode() string
}

// InferenceStrategy is the model provider interface used by the harness.
// Fluent With* builders and SetSystemPrompt live on concrete providers
// (for example *builtins.OpenAIInferenceStrategy), not this interface.
type InferenceStrategy interface {
	Invoke(ctx context.Context, messages []*Message, tools []*Tool, systemPrompt string) (chan LLMResponseChunk, error)
	CountTokens(context.Context, []*Message, []*Tool) (int, error)
	MaxContextWindow() (int, error)
	// SupportsMIME reports whether the currently selected model accepts the
	// given MIME type as user input. Empty and text/* are always true.
	// Probe representatives for ads (e.g. image/png); do not enumerate all types.
	SupportsMIME(mimeType string) bool
}

// UnsupportedMIMEs returns mimes for which s.SupportsMIME is false (first-seen order).
func UnsupportedMIMEs(s InferenceStrategy, mimes []string) []string {
	if s == nil || len(mimes) == 0 {
		return nil
	}
	var bad []string
	seen := make(map[string]struct{}, len(mimes))
	for _, m := range mimes {
		m = NormalizeMIME(m)
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		if !s.SupportsMIME(m) {
			bad = append(bad, m)
		}
	}
	return bad
}

// AgentWatchDog records assistant output and tool results for a turn.
// Nil on AgentOptions means no watchdog.
type AgentWatchDog interface {
	RecordOutput(*Message) error
	RecordToolResult(*Message) error
}

// Job is one background job of the current session as tools may see it.
// State is running, completed, or failed. A specialist child waiting for
// input stays running. Name is the specialist or registered worker.
type Job struct {
	ID     string
	Name   string
	State  string
	Result string
}

// JobRequest is Schedule input. Name is a specialist or a Runtime job worker.
type JobRequest struct {
	Name string
	Task string
}

// Parent-facing job states. A specialist child waiting for input stays running.
const (
	JobRunning     = "running"
	JobCompleted   = "completed"
	JobFailed      = "failed"
	ChildRunning   = JobRunning
	ChildCompleted = JobCompleted
	ChildFailed    = JobFailed
)

// HarnessRuntime is the tool-facing hook for one harness turn.
// Tools emit progress, read/write user session state, Park, and
// run specialists or schedule jobs of this session. Session modules (plan,
// permissions, on-call) are not on this interface.
//
// Built-in spawn_specialist / list_children / cancel_child call these.
// Host tools may call them too. The loop never matches those tool names.
type HarnessRuntime interface {
	EmitUpdate(message string)
	StateGet(key string) (any, bool)
	StateSet(key string, value any) error
	StateDelete(key string)
	// Park writes pending for this tool call and returns the interrupt as
	// error. After Resume it returns the resolved interrupt and a nil error.
	Park(kind string, payload []byte) (Interrupt, error)
	CurrentToolCallID() string

	// RunSpecialist starts a nested specialist session and returns its result.
	// The tool call stays open until the child completes. This is not a job:
	// no inbox message, and the child is dropped when this returns.
	RunSpecialist(ctx context.Context, name, task string) (string, error)
	// Schedule starts a job of this session. It does not wait.
	// Name is a registered specialist or Runtime job worker. The result
	// arrives later as an inbox message. Pass the id to Jobs or CancelJob.
	Schedule(ctx context.Context, job JobRequest) (Job, error)
	// Jobs lists this session's jobs. Waiting specialist children appear as running.
	Jobs() []Job
	// CancelJob stops one job of this session and drops it from Jobs.
	CancelJob(ctx context.Context, id string) error
}

// JobWaitError is returned by the Temporal JobHost so the Tool activity can
// return without writing a tool result. The workflow waits on the child, then
// RecordToolResult. In-process RunSpecialist blocks and never returns this.
type JobWaitError struct{ ID string }

func (e *JobWaitError) Error() string {
	if e == nil || e.ID == "" {
		return "wait for child"
	}
	return "wait for child " + e.ID
}

// Interrupt types re-exported for tool authors.
type (
	Interrupt               = interrupt.Interrupt
	PayloadValidator        = interrupt.PayloadValidator
	UserChoice              = interrupt.UserChoice
	UserSelectionInterrupt  = interrupt.UserSelectionInterrupt
	ToolPermissionInterrupt = interrupt.ToolPermissionInterrupt
	PermissionOption        = interrupt.PermissionOption
	AuthExpired             = interrupt.AuthExpired
)

var (
	ErrInterruptNotFound     = interrupt.ErrInterruptNotFound
	ErrInvalidPayload        = interrupt.ErrInvalidPayload
	DefaultPermissionOptions = interrupt.DefaultPermissionOptions
)

const (
	PermissionAllowOnce    = interrupt.PermissionAllowOnce
	PermissionAllowAlways  = interrupt.PermissionAllowAlways
	PermissionRejectOnce   = interrupt.PermissionRejectOnce
	PermissionRejectAlways = interrupt.PermissionRejectAlways
	TypeAuthExpired        = interrupt.TypeAuthExpired
)

// RegisterInterrupt registers a custom interrupt factory for session rehydrate.
func RegisterInterrupt(factory func() Interrupt) {
	interrupt.Register(factory)
}
