# Tool clients

A tool that needs a database, SDK, or other client **closes over it** when you call `NewTool`. That is typed dependency injection. There is no string bag and no lookup on `HarnessRuntime`.

## Host tools

Write a constructor that takes the client and returns `*tacklr.Tool`. The handler captures the client.

```go
func NewSearchRecordsTool(store RecordStore) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "search_records",
		Description: "Search operational records.",
		Handler: func(ctx context.Context, args SearchArgs, rt *tacklr.HarnessRuntime) (string, error) {
			return store.Search(ctx, args.Query, args.Limit)
		},
	})
}
```

Register the result on `AgentOptions.Tools`. The closure is formed when the host builds the agent. Rebuild the `*Tool` if you need a different client.

### Tests

Pass a fake into the same constructor:

```go
opts := tacklr.AgentOptions{
	Model: model,
	Tools: []*tacklr.Tool{NewSearchRecordsTool(fakeStore)},
}
```

Or invoke the tool directly with that constructor. You do not mock `HarnessRuntime` to inject the store: it is a concrete type the harness makes for each call (`*tacklr.HarnessRuntime`), so tests run tools through a real harness.

### What `HarnessRuntime` is for

Progress (`EmitUpdate`), park (`Park` with a typed interrupt such as `&UserSelectionInterrupt{…}`; a custom type is registered with `RegisterInterrupt` so a parked call can be read back), Runtime job workers (`Schedule`, with `Wait` to make the job's output the call's result; `Jobs`; `CancelJob`; specialists are the model's to start, through `spawn_specialist`), and session key-values (`StateGet` / `StateSet` / `StateDelete`). Hosts set those values on `CreateSession.State`, `Prompt.State`, or `Resume.State`. Close over clients in the constructor. Register named workers on `temporal.Config.Jobs`.

### Large results

Each result goes into the context window with its call. When a reply's results do not fit even after the window collapses into a summary, the largest are cut to fit, and the cut result tells the model so. Set `AgentOptions.OverflowPath` to a directory on a writable mount to keep each cut result's full text there; the note names the file for `read`. A `ToolInterceptor` can also replace a result's `Output` to shorten it: it returns the call's `ToolOutcome` and may change its `Output` and `Effect` after `next` returns.

## Optional tools

Packages `web` and `email` export optional tools. Construct them with a closed-over client and put the result on `AgentOptions.Tools`. The harness does not inject these from options fields.

```go
client := web.NewExa(os.Getenv("EXA_API_KEY"))
mail := gmail.New(gmailService)

opts := tacklr.AgentOptions{
    Model: model,
    Tools: []*tacklr.Tool{
        web.WebSearch(client),
        web.WebFetch(client),
        email.ReadInbox(mail),
        email.SendEmail(mail),
    },
}
```

Tests pass a fake into the same constructor. Omit a constructor and that tool is absent. Specialists that need these tools list them on `Specialist.Tools`; they are not inherited.

## Session-world tools

These still inject when the turn’s world is present. They close over per-turn mount and search state.

| You set | Tools that close over it |
|---------|--------------------------|
| `MountSession` | `read`, `write`, `write_document`, `write_spreadsheet`, `run_command` |
| `SkillsPath` (on the workspace when mounted, otherwise a local directory) | `read_skill` |
| `Brain` | `search`, `open`, `related`; `save` with `BrainWriteKinds`; `link` when the engine has a graph |
| Brain + workspace + search namespace | `index_file`, `unindex` |

Write tools need an active plan (`create_plan`). That lock is a product rule: hosts cannot turn it off. Specialists skip it. `write`, the document and spreadsheet writes, and `run_command` park for permission unless the host names them in `AgentOptions.AllowTools` (by the name the model calls them; a user's reject-always still wins).

Plan tools close over the session plan store (harness state, not a host client). Child-session tools inject when `Specialists` is set. MCP tools close over the live MCP connection created at discover time.

## Why not a map of clients

A `map[string]any` (or typed lookup on the runtime) hides what a tool needs, requires a cast or a miss at call time, and invites putting clients in checkpointed state. A constructor argument is the dependency list. The compiler checks it. A test replaces it in one place.
