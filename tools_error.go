package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

// agentError is a model-facing tool failure. Error() is what the model sees
// (what failed and how to correct it). Unwrap is the harness cause.
type agentError struct {
	msg   string
	cause error
}

func (e *agentError) Error() string {
	if e == nil || e.msg == "" {
		return ErrAgent.Error()
	}
	return e.msg
}

func (e *agentError) Unwrap() error { return e.cause }

func (e *agentError) Is(target error) bool { return target == ErrAgent }

// AgentError wraps cause with model-facing correction text. msg is Error();
// errors.Is matches ErrAgent and cause. A nil/empty msg uses cause.Error().
func AgentError(cause error, msg string) error {
	msg = strings.TrimSpace(msg)
	if msg == "" && cause != nil {
		msg = strings.TrimPrefix(cause.Error(), "vfs: ")
	}
	if msg == "" {
		return ErrAgent
	}
	var a *agentError
	if errors.As(cause, &a) {
		return cause
	}
	return &agentError{msg: msg, cause: cause}
}

// AgentErrorf is AgentError with fmt.Sprintf.
func AgentErrorf(cause error, format string, args ...any) error {
	return AgentError(cause, fmt.Sprintf(format, args...))
}

// presentToolError rewrites every tool failure the model sees: what happened,
// and how to fix it when that is known. Interrupts, caller cancel, and
// ErrFailed (harness/runtime) pass through; everything else is ErrAgent.
func presentToolError(name string, err error) error {
	if err == nil {
		return nil
	}
	var intr interrupt.Interrupt
	if errors.As(err, &intr) {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(err, ErrFailed) {
		return err
	}
	if errors.Is(err, ErrAgent) {
		return err
	}
	if e := presentExaError(name, err); e != nil {
		return e
	}
	switch {
	case errors.Is(err, vfs.ErrInvalidWrite), errors.Is(err, vfs.ErrConflict):
		return AgentErrorf(vfs.ErrInvalidWrite, "%s: the document was not saved. Some of the content may already be in the file. Read the file, then write the full HTML again", name)
	case errors.Is(err, vfs.ErrStaleContent):
		return AgentErrorf(vfs.ErrStaleContent, "%s: the file changed since that rev. Omit rev, or read the file again before writing", name)
	case errors.Is(err, vfs.ErrUseHTML):
		return AgentErrorf(vfs.ErrUseHTML, "%s: this is a document. Pass content as HTML, for example <h1>Title</h1> and <p>paragraphs</p>", name)
	case errors.Is(err, vfs.ErrEmptyReplace):
		return AgentErrorf(vfs.ErrEmptyReplace, "%s: %s", name, vfs.ErrEmptyReplace.Error())
	case errors.Is(err, vfs.ErrTabIDRequired):
		return AgentErrorf(vfs.ErrTabIDRequired, "%s: %s", name, vfs.ErrTabIDRequired.Error())
	case errors.Is(err, vfs.ErrProjected):
		return AgentErrorf(vfs.ErrProjected, "%s: that write is not supported on this file type. For a document, write HTML content or a line range. For a sheet, write one cell as block_id Sheet!A1", name)
	case errors.Is(err, vfs.ErrNotExist):
		return AgentErrorf(vfs.ErrNotExist, "%s: that path does not exist. List the parent with run_command (ls) or read a known path, then retry", name)
	case errors.Is(err, vfs.ErrInvalidPath):
		return AgentErrorf(vfs.ErrInvalidPath, "%s: that path is not a valid virtual path. Use an absolute path under a mount (for example /workspace/documents/Name)", name)
	case errors.Is(err, vfs.ErrIsDir):
		return AgentErrorf(vfs.ErrIsDir, "%s: that path is a directory. Read a file inside it, or list it with run_command ls", name)
	case errors.Is(err, vfs.ErrNotDir):
		return AgentErrorf(vfs.ErrNotDir, "%s: that path is a file, not a directory", name)
	case errors.Is(err, vfs.ErrAuthExpired):
		return AgentErrorf(vfs.ErrAuthExpired, "%s: cloud credentials expired. The host must refresh the token; then retry the same call", name)
	case errors.Is(err, vfs.ErrPermission):
		return AgentErrorf(vfs.ErrPermission, "%s: permission denied on that path. Use a path the session is allowed to access", name)
	case errors.Is(err, vfs.ErrReadOnly):
		return AgentErrorf(vfs.ErrReadOnly, "%s: that mount is read-only. Read the file, or write under a writable mount", name)
	case errors.Is(err, vfs.ErrLineOutOfRange):
		return AgentErrorf(vfs.ErrLineOutOfRange, "%s: that line range is outside the file. Read the path without start/end to see line_count, then request a window that fits", name)
	case errors.Is(err, vfs.ErrInvalidLine):
		return AgentErrorf(vfs.ErrInvalidLine, "%s: a replacement line contained a newline. Pass each line as its own array entry, with no embedded \\n", name)
	case errors.Is(err, vfs.ErrTooLarge):
		return AgentErrorf(vfs.ErrTooLarge, "%s: that payload is too large. Write a smaller window or split the document", name)
	case errors.Is(err, vfs.ErrFuseNotMounted):
		return AgentErrorf(vfs.ErrFuseNotMounted, "%s: the host shell is not mounted. Use read/write on virtual paths instead of run_command", name)
	case errors.Is(err, vfs.ErrNotTextual):
		return AgentErrorf(vfs.ErrNotTextual, "%s: that file is not text. Use a different path, or write a text/HTML document", name)
	case errors.Is(err, ErrToolTimeout):
		return AgentErrorf(ErrToolTimeout, "%s timed out. Retry with a smaller request, fewer URLs, or a narrower search", name)
	case errors.Is(err, ErrToolPermissionDenied):
		return AgentErrorf(ErrToolPermissionDenied, "%s was rejected by the user. Do not retry this tool unless the task can proceed another way", name)
	case errors.Is(err, ErrNotFound):
		return presentNotFound(name, err)
	case errors.Is(err, ErrInvalid):
		return presentInvalid(name, err)
	default:
		msg := strings.TrimPrefix(err.Error(), "vfs: ")
		if name != "" && !strings.HasPrefix(msg, name) {
			msg = name + ": " + msg
		}
		return AgentError(err, msg)
	}
}

func presentNotFound(name string, err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "worker"):
		return AgentErrorf(err, "%s: that worker is not registered. Pass a worker_name from the available sub-agents", name)
	case strings.Contains(msg, "job"):
		return AgentErrorf(err, "%s: that job_id is unknown. Call list_jobs, then get_job or cancel_job with an id from that list", name)
	default:
		return AgentErrorf(err, "%s: not found. Check the id or path and try again", name)
	}
}

func presentInvalid(name string, err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "job_id"):
		return AgentErrorf(err, "%s: job_id is required. Call list_jobs and pass a job_id from that list", name)
	case strings.Contains(msg, "command is required"):
		return AgentErrorf(err, "%s: command is required. Pass a shell command string, for example ls work", name)
	case strings.Contains(msg, "empty task"):
		return AgentErrorf(err, "%s: task_description_and_context is required. Describe the worker's goal and constraints", name)
	default:
		if strings.Contains(msg, "required") || strings.Contains(msg, "use ") || strings.Contains(msg, "Pass ") {
			return AgentErrorf(err, "%s: %s", name, strings.TrimPrefix(msg, name+": "))
		}
		return AgentErrorf(err, "%s: invalid arguments. %s", name, msg)
	}
}

func presentExaError(name string, err error) error {
	msg := err.Error()
	if !strings.Contains(msg, "exa ") && !strings.Contains(msg, "UNSUPPORTED_PUBLICATION") {
		return nil
	}
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(msg, "UNSUPPORTED_PUBLICATION") || strings.Contains(low, "not supported for category=publication"):
		return AgentError(err, name+": category=publication cannot filter by domain. Omit category, or drop include_domains/exclude_domains, then search again")
	case strings.Contains(msg, "status 401"), strings.Contains(msg, "status 403"):
		return AgentError(err, name+": the search provider rejected the API key. The host must fix Exa credentials, then retry")
	case strings.Contains(msg, "status 429"):
		return AgentError(err, name+": the search provider rate-limited the request. Wait and retry, or drop extra filters")
	case strings.Contains(msg, "status 4"):
		return AgentError(err, name+": the search provider rejected that request. Simplify filters (omit category or domain lists) and retry")
	default:
		return AgentError(err, name+": the search provider failed. Retry with a simpler query, or omit category and domain filters")
	}
}
