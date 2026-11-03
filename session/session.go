package session

// Wake kinds from the Driver mailbox. A prompt or resume is also the turn-span kind.
const (
	WakePrompt = "prompt"
	WakeResume = "resume"
	WakeCancel = "cancel"
	WakeClose  = "close"
	// WakeJob is a job's end: its result.
	WakeJob = "job"
	// WakeSuspend stops a parent session so the runtime can restart it with
	// the same state (on Temporal, continue-as-new to keep history small):
	// when idle, or during a turn that waits on jobs. The session's state is
	// plain data; the runtime carries it to the next run.
	WakeSuspend = "suspend"
)

// Wake is one mailbox event. Job is set when Kind is WakeJob. A zero Wake
// (empty Kind) is Next's answer when nothing is queued and it must not
// block.
type Wake struct {
	Kind   string
	Prompt Prompt
	Resume Resume
	Job    JobEnd
}

// JobEnd is how a job ended: its result, from the job itself.
type JobEnd struct {
	ID     ID
	Result JobResult
}
