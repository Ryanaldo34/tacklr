# Concepts

This page explains the words the rest of the documentation uses.

## Session

A **session** is one long-lived conversation with an agent. You create it with `session.Client.CreateSession`, send it prompts, and close it with `Close`. Between prompts it waits and uses no compute.

A session is durable. Its runtime records the result of each step, so after a crash or a deploy the session continues from the last finished step on any machine. A step that finished never runs again.

`session.Client` is the only API a host needs:

| Method | Does |
|---|---|
| `CreateSession` | Starts a session. `State` sets values that tools read with `CallContext.StateGet`. |
| `Prompt` | Sends a user message. A prompt sent while a turn runs waits in a queue for that turn to reach a safe point. |
| `Resume` | Answers the question that a paused turn is waiting for. |
| `Subscribe` | Streams the session's events from a sequence number. |
| `Cancel` | Stops the running turn and the session's children. The session stays open. |
| `Close` | Ends the session and deletes what it saved. |
| `Status`, `Head`, `Jobs` | Report the session's state, its latest event, and its child sessions. |

## Turn

A **turn** is the work that one prompt starts: the agent calls the model, runs the tools the model asks for, and repeats until the model answers without a tool call. A turn ends with `EventComplete` or `EventError`.

A turn can **park**: a tool asks the user something and the turn waits for `Resume`. While it waits, `Status` is `running` and `Waiting` is true. The client gets an `EventInterrupt` event with the question.

## Round

A **round** is a user message, or one model reply together with a result for each tool call in it. The context window holds only complete rounds, so the model never sees a tool call without its result.

The tool calls of one reply run in parallel. The round commits when the last call has its result.

## Steps and the harness

The runtime runs a session as three kinds of step:

| Step | Does |
|---|---|
| `Infer` | Sends one model request on the current context window. |
| `Tool` | Runs one tool call. |
| `Commit` | Saves a complete round to the context window and the session's checkpoint. |

The **harness** (`tacklr.Harness`) does the work of each step. You do not create it: the runtime builds it from your `AgentOptions` and keeps one for each session on the worker that runs it.

`orchestrator.Run` decides which step comes next. It is the same code on every runtime, so an agent behaves the same on Temporal and on Durable Objects.

## Context window

The **context window** is the list of messages the model gets with each request. Tacklr keeps it small:

- **Plans.** The agent writes a plan and a to-do list with `create_plan`. The plan stays at the top of the window.
- **Hand-offs.** When the agent completes a to-do with `complete_todo` and other to-dos are still open, the model writes a short hand-off, and the window becomes the user's goal, the plan, and the hand-off.
- **Collapse.** When the window grows past 85% of its size, the history becomes one summary.
- **Cuts.** When one round is still too large, its largest tool results are cut, and the full text is kept where the agent can read it.

With a knowledge store, the messages that leave the window stay searchable for that session. See [Context window](context.md) for the details.

## Specialists and jobs

A **specialist** is another agent that the main agent can start with the `spawn_specialist` tool. It runs as a **child session** with its own model, tools, and instructions, and returns only its answer.

A **job** is long work that is not an agent, such as a data export. A tool starts it with `CallContext.Schedule`, and it also runs as a child session.

The main agent can wait for a child's result, or continue and get the result later. See [Sessions in production](session.md#specialists-and-jobs).

## Events

`Subscribe` sends two kinds of event:

- **Durable events** are stored in the session's event log: complete messages and reasoning, tool calls, tool results, plan updates, questions (`EventInterrupt`), and turn ends.
- **Live events** are not stored: pieces of a message as the model writes it (`Event.Delta` is true), and tool progress. They go straight to the clients that listen at that moment.

A turn's error is an `EventError` whose `Error` you check with `errors.Is`: `session.ErrModelRefused`, `session.ErrMaxTokens`, `session.ErrContextLength`, or `context.Canceled`.

## Host, worker, and runtime

| Part | Runs | State |
|---|---|---|
| **Host** | Your API or protocol server. It calls `session.Client`. | None |
| **Runtime** | Temporal, or Durable Objects. It keeps each session durable. | The session's history |
| **Worker** | The process that runs the steps (on Temporal). | A cache of the harnesses it holds |
| **Stores** | Postgres, or in-memory stores for tests. | Checkpoints, sealed credentials, events, knowledge |

The host never imports the agent code, and a worker links only the adapters you import.
