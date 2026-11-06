// Package orchestrator decides each step of a session, the same way on
// every runtime.
//
// Run drives one session from its first wake to its close. Given the
// session's State and each wake (a prompt, a resume, a cancel, a job's
// end), it decides the next step: commit the prompt into the window, ask
// the model, run the reply's calls and start the jobs they ask for, park
// on an interrupt, commit the round, and end the turn. It makes no network
// call and touches no store: it only calls its Runtime.
//
// Runtime is the port to a durable runtime (Temporal, Durable Objects): it
// runs each step once (Infer, Tools, Commit), reads the session's next
// wake (Next), publishes events (Emit), and runs jobs. A runtime records
// each step's result, so a replay of Run gets the recorded result instead
// of running the step again. The work of a step is the harness's
// (tacklr.Harness); the runtime only carries it out.
//
// State is the session's state across its turns: the open round, the
// inbox, the jobs, and the last commit's revision. Carry is the part of it
// that a runtime keeps across a suspend or a continue-as-new.
package orchestrator
