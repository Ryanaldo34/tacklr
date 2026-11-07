package anthropic

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/provider"
	"github.com/ryanaldo34/tacklr/session"
)

// apiError is the error object of an error answer or an error event.
type apiError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// classifyStatus is the error of an HTTP error answer.
func classifyStatus(status int, body []byte) error {
	var answer struct {
		Error *apiError `json:"error"`
	}
	if json.Unmarshal(body, &answer) != nil || answer.Error == nil {
		answer.Error = &apiError{Message: string(body)}
	}
	return classifyError(status, answer.Error)
}

// classifyError is the error of a failed request. A request over the
// context window is session.ErrContextLength, and an overload, a rate
// limit or a server failure is tacklr.ErrNetwork, which the runtime
// retries.
func classifyError(status int, e *apiError) error {
	if e == nil {
		e = &apiError{}
	}
	failed := &APIStatusError{Status: status, Body: e.Message, Code: e.Type}
	msg := strings.ToLower(e.Message)
	switch {
	case status == http.StatusRequestEntityTooLarge || e.Type == "request_too_large",
		strings.Contains(msg, "prompt is too long"),
		strings.Contains(msg, "exceed context limit"):
		return fmt.Errorf("%w: %w", session.ErrContextLength, failed)
	case provider.Retryable(status), e.Type == "overloaded_error", e.Type == "api_error", e.Type == "rate_limit_error":
		return tacklr.Network(failed)
	}
	return failed
}
