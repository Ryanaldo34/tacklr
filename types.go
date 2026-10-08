package tacklr

import (
	"context"
	"errors"
)

// Shared errors and the inference contract for harness hosts.
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

// ErrNetwork is a transport failure: the dial, the read, a timeout, or an
// upstream 408, 429, or 5xx. Wrap it at the call that hit the network.
// The durable runtime retries a wrapped network error. Any other error
// stops on the first attempt.
var ErrNetwork = errors.New("network")

type networkError struct{ error }

func (e networkError) Unwrap() error { return e.error }

func (e networkError) Is(target error) bool { return target == ErrNetwork }

// Network marks err as a transport failure. The text stays err.Error().
func Network(err error) error {
	if err == nil || errors.Is(err, ErrNetwork) {
		return err
	}
	return networkError{err}
}

// ProviderStatus supplies HTTP status and error code from a provider error.
// Optional on InferenceStrategy errors for model-span attributes.
type ProviderStatus interface {
	ProviderHTTPStatus() int
	ProviderErrorCode() string
}

// InferenceStrategy is the model provider interface used by the harness.
// Fluent With* builders and SetSystemPrompt live on concrete providers
// (for example *openai.OpenAIInferenceStrategy), not this interface.
type InferenceStrategy interface {
	Invoke(ctx context.Context, messages []*Message, tools []*Tool, systemPrompt string) (chan LLMResponseChunk, error)
	CountTokens(context.Context, []*Message, []*Tool) (int, error)
	MaxContextWindow() (int, error)
	// SupportsMIME reports whether the currently selected model accepts the
	// given MIME type as user input. Empty and text/* are always true.
	// Probe representatives for ads (e.g. image/png); do not enumerate all types.
	SupportsMIME(mimeType string) bool
}

type toolChoiceNoneKey struct{}

// ContextWithToolChoiceNone asks the provider to keep tool definitions on the
// wire but not call them (Responses tool_choice=none). Handoff and compress
// use this so the cached tools prefix matches a normal turn.
func ContextWithToolChoiceNone(ctx context.Context) context.Context {
	return context.WithValue(ctx, toolChoiceNoneKey{}, true)
}

// ToolChoiceNone reports whether ctx was wrapped with ContextWithToolChoiceNone.
func ToolChoiceNone(ctx context.Context) bool {
	v, _ := ctx.Value(toolChoiceNoneKey{}).(bool)
	return v
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

	// RunSpecialist queues a blocking specialist session.
	// WaitFor is the child session the caller must wait on. Output is the
	// child's text when the host already finished that session. A host sets
	// one of them. This is not a background job: no inbox message.
	RunSpecialist(ctx context.Context, name, task string) (SpecialistResult, error)
	// Schedule starts a job of this session. It does not wait.
	// Name is a registered specialist or Runtime job worker. The result
	// arrives later as an inbox message. Pass the id to Jobs or CancelJob.
	Schedule(ctx context.Context, job JobRequest) (Job, error)
	// Jobs lists this session's jobs. Waiting specialist children appear as running.
	Jobs() []Job
	// CancelJob stops one job of this session and drops it from Jobs.
	CancelJob(ctx context.Context, id string) error
}

// SpecialistResult is the outcome of a blocking specialist request.
// WaitFor means the child session is not finished. The session loop waits
// for that id, then records the tool result. Output is set when the child
// already finished inside the call.
type SpecialistResult struct {
	Output  string
	WaitFor string
}

// RegisterInterrupt registers a custom interrupt factory for session rehydrate.
func RegisterInterrupt(factory func() Interrupt) {
	registerInterrupt(factory)
}
