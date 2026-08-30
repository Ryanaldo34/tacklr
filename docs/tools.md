# Tool clients

A tool that needs a database, SDK, or other client **closes over it** when you call `NewTool`. That is typed dependency injection. There is no string bag and no lookup on `HarnessRuntime`.

## Host tools

Write a constructor that takes the client and returns `*tacklr.Tool`. The handler captures the client.

```go
func NewSearchRecordsTool(store RecordStore) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "search_records",
		Description: "Search operational records.",
		Handler: func(ctx context.Context, args SearchArgs, rt tacklr.HarnessRuntime) (string, error) {
			return store.Search(ctx, args.Query, args.Limit)
		},
	})
}
```

Register the result on `AgentOptions.Tools`. Catalog register time is when the closure is formed. Rebuild the `*Tool` if you need a different client.

### Tests

Pass a fake into the same constructor:

```go
opts := tacklr.AgentOptions{
	Model: model,
	Tools: []*tacklr.Tool{NewSearchRecordsTool(fakeStore)},
}
```

Or invoke the tool directly with that constructor. You do not mock `HarnessRuntime` to inject the store.

### What `HarnessRuntime` is for

Progress (`EmitUpdate`), park (`Park`), children (`SpawnChild` and friends), and session key-values (`StateGet` / `StateSet` / `StateDelete`). Hosts set those values on `CreateSession.State`, `Prompt.State`, or `Resume.State`. Close over clients in the constructor.

## Optional builtins

Package `builtins` exports optional tools. Construct them with a closed-over client and put the result on `AgentOptions.Tools`. The harness does not inject these from options fields.

```go
exa := builtins.NewExa(os.Getenv("EXA_API_KEY"))
mail := builtins.Gmail(gmailService)

opts := tacklr.AgentOptions{
    Model: model,
    Tools: []*tacklr.Tool{
        builtins.WebSearch(exa),
        builtins.WebFetch(exa),
        builtins.ReadInbox(mail),
        builtins.SendEmail(mail),
    },
}
```

Tests pass a fake into the same constructor. Omit a constructor and that tool is absent. Specialists that need these tools list them on `Specialist.Tools`; they are not inherited.

## Session-world tools

These still inject when the turn’s world is present. They close over per-turn mount and search state.

| You set | Tools that close over it |
|---------|--------------------------|
| `MountSession` | `read`, `write`, `write_document`, `write_spreadsheet`, `run_command` |
| `SkillsSession` (`AgentSpec.OpenSkills`) | `read_skill` |
| `Brain` | `search`, `find_exact`, `read_object`, `schema`, `save_*`, `link`, `expand`, … |
| Brain + VFS + namespace (index bridge) | `index_file`, `unindex` |

Write tools need an active plan (`create_plan`). That lock is a product rule: hosts cannot turn it off. Specialists skip it. `write` / `run_command` park for permission unless the host sets `UnattendedWrite` / `UnattendedRunCommand` on `AgentOptions`.

Plan tools close over the session plan store (harness state, not a host client). Child-session tools inject when `Specialists` is set. MCP tools close over the live MCP connection created at discover time.

## Why not a map of clients

A `map[string]any` (or typed lookup on the runtime) hides what a tool needs, requires a cast or a miss at call time, and invites putting clients in checkpointed state. A constructor argument is the dependency list. The compiler checks it. A test replaces it in one place.
