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

Progress (`EmitUpdate`), park (`Park`), children (`SpawnChild` and friends), and **session** state (`StateGet` / `StateSet`). Session state is checkpointed. Clients are not session state. Do not store them there.

## Built-in tools

The harness uses the same constructor-closure pattern. You supply the client on `AgentOptions`; `injectBuiltinTools` closes it into the handler.

| You set | Tools that close over it |
|---------|--------------------------|
| `EmailProvider` | `read_inbox`, `send_email` |
| `ExaAPIKey` or env `EXA_API_KEY` | `web_search`, `web_fetch` (an Exa client) |
| `MountSession` | `read`, `write`, `write_document`, `write_spreadsheet`, `run_command` |
| `Brain` | `search`, `find_exact`, `read_object`, `schema`, `save_*`, `link`, `expand`, … |
| Brain + VFS + namespace (index bridge) | `index_file`, `unindex` |

A missing client means the tools are not registered. Tests inject a fake provider, a memory brain, or a temp mount the same way a host would.

Plan tools close over the session plan store (harness state, not a host client). MCP tools close over the live MCP connection created at discover time.

## Why not a map of clients

A `map[string]any` (or typed lookup on the runtime) hides what a tool needs, requires a cast or a miss at call time, and invites putting clients in checkpointed state. A constructor argument is the dependency list. The compiler checks it. A test replaces it in one place.
