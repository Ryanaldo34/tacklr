package tacklr

import (
	"context"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// Shared errors and the inference contract (Model) for harness hosts.

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
	ErrModelAfterTools = errors.New("model request failed after tools completed")
	ErrApiKeyNotSet    = errors.New("api key not set")
	ErrModelNotSet     = errors.New("model not set")
	ErrUnknownModel    = errors.New("unknown model")
	// ErrCannotCount is a CountTokens error: the provider has no way to
	// count a request's tokens. The harness then sizes requests from the
	// input tokens the provider reports for each response.
	ErrCannotCount          = errors.New("the model cannot count tokens")
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

// ModelRequest is one model request. A strategy holds no per-request
// state: everything a request needs is here, so one strategy is safe to
// share across sessions and calls.
type ModelRequest struct {
	Messages     []*message.Message
	Tools        []*Tool
	SystemPrompt string
	// CacheKey is the session id. A strategy may send it so the provider
	// keeps the session's prompt prefix cached.
	CacheKey string
}

// Model is the model provider interface used by the harness.
// Model and endpoint settings (the fluent With* builders) live on concrete
// providers such as *openai.Model; everything per request
// is in ModelRequest.
type Model interface {
	Invoke(ctx context.Context, req ModelRequest) (chan LLMResponseChunk, error)
	// CountTokens counts what Invoke would send for req, with the provider.
	// A provider that cannot count returns an error that wraps
	// ErrCannotCount. Invoke reports a request the provider refused as over
	// the context window with session.ErrContextLength.
	CountTokens(ctx context.Context, req ModelRequest) (int, error)
	MaxContextWindow() (int, error)
	// ModelTelemetryIdentity names the provider and model for model spans.
	ModelTelemetryIdentity() telemetry.ModelIdentity
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
func UnsupportedMIMEs(s Model, mimes []string) []string {
	if s == nil || len(mimes) == 0 {
		return nil
	}
	var bad []string
	seen := make(map[string]struct{}, len(mimes))
	for _, m := range mimes {
		m = message.NormalizeMIME(m)
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

// Job is one background job of the current session as tools may see it.
// State is running, completed, or failed. A specialist child waiting for
// input stays running. Name is the specialist or registered worker.
type Job struct {
	ID     string
	Name   string
	State  string
	Result string
}

// JobRequest is Schedule input. Name is a Runtime job worker.
type JobRequest struct {
	Name string
	Task string
	// Wait makes the call wait for the job: the job's output is the call's
	// result. Without it the call goes on, and the output arrives later as
	// an inbox message.
	Wait bool
}

// JobRunning is a listed job's state. Jobs lists only running jobs: a
// finished one leaves the list when its result arrives. A specialist child
// waiting for input stays running.
const JobRunning = "running"

// RegisterInterrupt registers a custom interrupt factory for session rehydrate.
func RegisterInterrupt(factory func() Interrupt) {
	name := factory().TypeName()
	interrupts.Lock()
	defer interrupts.Unlock()
	if _, ok := interruptFactories[name]; ok {
		panic(fmt.Sprintf("interrupt type %q registered twice", name))
	}
	interruptFactories[name] = factory
}
