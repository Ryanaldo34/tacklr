# Getting started

This guide starts from the [quick start program](../README.md#quick-start) in the README and adds one feature at a time: tool arguments, a question to the user, a permission, a workspace, a knowledge store, a specialist, and a production layout.

## Before you start

You need:

- Linux and Go 1.27 or later.
- A Temporal server. For development, install the [Temporal CLI](https://docs.temporal.io/cli) and run `temporal server start-dev`.
- An API key for Claude (`ANTHROPIC_API_KEY`) or for an OpenAI-compatible endpoint. See [Models](models.md).

## The parts of the first program

The quick start program has three parts:

1. **An agent.** `tacklr.AgentOptions` holds the model, the system prompt, and the tools. Every session runs this agent.
2. **A worker.** `worker.New` runs sessions. Each model request and each tool call is a Temporal activity, so a step that finished never runs again after a crash.
3. **A client.** `temporal.Open` returns a `session.Client`. It creates sessions, sends prompts, and streams their events. The client does not run the agent, so a server that uses it stays stateless.

The worker and the client share the in-memory stores, so they must run in one process. [Go to production](#go-to-production) splits them.

## Describe tool arguments

A tool's handler is a Go function. Its optional parameters are a `context.Context`, an arguments struct, and a `*tacklr.CallContext`, in that order. Tacklr builds the JSON Schema that the model sees from the struct's fields:

```go
type searchArgs struct {
	Query string   `json:"query" desc:"Words to search for"`
	Sort  string   `json:"sort" desc:"Result order" enum:"newest,relevance"`
	Limit *int     `json:"limit,omitempty" desc:"Most results to return"`
	Tags  []string `json:"tags"`
}
```

`desc` describes an argument, `enum` lists its values, and a pointer or `omitempty` makes it optional. [Tools](tools.md#arguments) lists every tag.

A handler that returns an error gives the model the error text, and the model can try again. To tell the model exactly how to correct its call, return `tacklr.Correction(err, "Use a date in YYYY-MM-DD format.")`.

## Ask the user a question

A tool can pause the session and ask the user to choose. `CallContext.Park` returns an error the first time; return that error and the session waits. When the user answers, the tool runs again and `Park` returns the answer:

```go
type reportArgs struct {
	Name string `json:"name" desc:"Report name"`
}

func deleteTool() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "delete_report",
		Description: "Delete a report after the user confirms.",
		Handler: func(ctx context.Context, args reportArgs, cc *tacklr.CallContext) (string, error) {
			answer, err := cc.Park(&tacklr.UserSelectionInterrupt{
				Question: "Delete " + args.Name + "?",
				Options:  []tacklr.UserChoice{{Title: "Delete"}, {Title: "Keep"}},
			})
			if err != nil {
				return "", err // the session waits here
			}
			if answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title != "Delete" {
				return "kept " + args.Name, nil
			}
			return "deleted " + args.Name, nil
		},
	})
}
```

The client gets an `EventInterrupt` event. Answer it with `Resume`, keyed by the event's `MessageID`:

```go
func confirm(ctx context.Context, sessions session.Client, id session.ID, ev session.Event) error {
	return sessions.Resume(ctx, id, session.Resume{
		Responses: map[string][]byte{ev.MessageID: []byte(`{"selectionIdx":0}`)},
	})
}
```

The other tool calls of the same reply do not run again. A session can wait for an answer for as long as you need: on Temporal, the waiting session is a workflow that uses no worker.

## Ask for permission

Tools that change things can ask the user before they run. Give the tool write access and the built-in permission check:

```go
tacklr.NewTool(tacklr.ToolConfig{
	Name:        "send_invoice",
	Description: "Email an invoice to a customer.",
	Access:      tacklr.ToolWriteAccess,
	OnCall:      []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
	Handler:     sendInvoice,
})
```

The user can allow or reject the call, once or always. A tool with write access also runs only after the agent has made a plan with `create_plan`, so the agent decides what to do before it changes anything. To skip the question for a trusted tool, list it in `AgentOptions.AllowTools`. See [Tools](tools.md#permissions).

## Add a workspace

A workspace gives the agent files to read and write under `/workspace`. Set `worker.Config.Workspace`:

```go
func agentWorkspace() workspace.Config {
	return workspace.Config{Open: vfs.Tree(
		vfs.At("work", vfs.SessionLocal("/srv/tacklr/work")),     // /workspace/work, one directory per session
		vfs.At("docs", vfs.Local("/srv/tacklr/docs")).ReadOnly(), // /workspace/docs, shared and read-only
	)}
}
```

The agent gets the `read`, `write`, `write_document`, and `write_spreadsheet` tools. Set `worker.Config.FuseRoot` to also give it `run_command`, a shell that sees only the session's workspace. See [Workspace](workspace.md) for S3, Azure Blob, Google Drive, Microsoft 365, and Git.

## Add a knowledge store

A knowledge store lets the agent save facts and find them later, in this session or another. Set `AgentOptions.Brain`:

```go
func knowledge() (*brain.Engine, brain.Namespace, error) {
	note := brain.KindSpec{Kind: "Note", Description: "A fact worth keeping", IsParent: true}
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(note))
	if err != nil {
		return nil, nil, err
	}
	ns, err := brain.ParseNamespace("org", "acme")
	return eng, ns, err
}
```

Then set `Brain: eng`, `SearchNamespace: ns`, and `BrainWriteKinds: []string{"Note"}` on the agent. The agent gets `search`, `open`, `related`, and `save`, and it can read and write only inside the namespace. In production, use `brain/postgres`. See [Knowledge](knowledge.md).

## Add a specialist

A specialist is a second agent with its own instructions, model, and tools. The main agent gives it a task with the built-in `spawn_specialist` tool, and gets back only the specialist's answer. The specialist's work never enters the main agent's context window.

```go
func withReviewer(agent tacklr.AgentOptions) tacklr.AgentOptions {
	agent.Specialists = []*tacklr.Specialist{{
		Name:         "reviewer",
		Description:  "Reviews a change and reports problems.",
		Instructions: "Review the change. Report only real problems.",
		Model:        agent.Model,
	}}
	return agent
}
```

Each specialist runs as its own child session. See [Sessions in production](session.md#specialists-and-jobs).

## Go to production

In production the client and the worker run in different processes, and they share Postgres:

- **Hosts** call `temporal.Open` and serve your clients, for example with the ACP server in `server/acp`. They keep no session state, so you can run any number behind a load balancer.
- **Workers** call `worker.New` and run the sessions. A session's steps go to the worker that already has the session loaded. If that worker stops, any other worker continues the session from Postgres.

Both use the stores from `session/postgres`:

```go
// sessionpg is github.com/ryanaldo34/tacklr/session/postgres.
func stores(ctx context.Context, pool *pgxpool.Pool, key []byte) (session.Store, session.SecretStorage, error) {
	db, err := sessionpg.New(pool, sessionpg.Options{})
	if err != nil {
		return nil, nil, err
	}
	if err := db.Setup(ctx); err != nil { // creates the tables that do not exist
		return nil, nil, err
	}
	sealer, err := session.NewAESGCM(key) // a 32-byte key, or your own session.Sealer over a KMS
	if err != nil {
		return nil, nil, err
	}
	return db.Store(), db.Secrets(sealer), nil
}
```

[examples/temporal](../examples/temporal) is a complete production layout with an end-to-end test. [Sessions in production](session.md) covers the configuration, and [examples/cloudflare](../examples/cloudflare) runs the same agent on Cloudflare Workers without Temporal.
