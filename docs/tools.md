# Tools

A tool is a Go function the model can call. This guide covers writing tools, asking the user, the built-in tools, MCP servers, and skills.

## Write a tool

Write a constructor that takes the clients the tool needs and returns a `*tacklr.Tool`. The handler closes over the clients:

```go
func searchRecordsTool(store RecordStore) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "search_records",
		Description: "Search operational records by keyword.",
		Category:    message.ToolCategorySearch,
		Access:      tacklr.ToolReadAccess,
		Handler: func(ctx context.Context, args searchArgs) (string, error) {
			return store.Search(ctx, args.Query)
		},
	})
}
```

Put the tool on `AgentOptions.Tools`. The closure is the tool's client for every session that runs the agent. To change the client, build the tool again.

| `ToolConfig` field | Effect |
|---|---|
| `Name` | The name the model calls. Required. |
| `Description` | What the tool does and when to use it. The model reads it. |
| `DisplayName` | A title for clients. It can use the arguments, such as `Search {query}`. |
| `Namespace` | Groups tools. The model calls a namespaced tool `namespace__name`. |
| `Category` | The kind of action (read, edit, search, fetch, execute, ...), for clients. |
| `Access` | `ToolReadAccess`, `ToolWriteAccess`, `ToolExecuteAccess`, or a mix. A tool with write access runs only after the agent has a plan. |
| `Timeout` | The longest one call may run. The model gets an error and can retry with a smaller request. |
| `OnCall` | Checks that run before the handler and can ask the user. See [Permissions](#permissions). |
| `Handler` | The function. |

The handler's optional parameters are a `context.Context`, an arguments struct, and a `*tacklr.CallContext`, in that order. It returns a string and an error.

## Arguments

Tacklr builds the JSON Schema that the model sees from the fields of the arguments struct:

```go
type searchArgs struct {
	Query string   `json:"query" desc:"Words to search for"`
	Sort  string   `json:"sort" desc:"Result order" enum:"newest,relevance"`
	Limit *int     `json:"limit,omitempty" desc:"Most results to return"`
	Tags  []string `json:"tags"`
}
```

| Tag | Effect |
|---|---|
| `json:"name"` | The argument's name. `json:"-"` or `schema:"-"` leaves the field out. |
| `desc:"..."` | The argument's description. |
| `enum:"a,b"` | The values the argument may take. |
| `omitempty` or a pointer | The argument is optional: the model can send `null` for it. |

Every other field is required. Embedded structs are flattened, and a `time.Time` is a string.

## Results and errors

The string the handler returns is the tool's result, which goes into the context window with the call.

- **An error** gives the model the error's text, and the model can try again.
- **A correction** tells the model how to fix its call: `return "", tacklr.Correction(err, "Use a date in YYYY-MM-DD format.")`.
- **A large result** stays whole when it fits. When a round is too large for the window, its largest results are cut, and the cut result tells the model so. With a workspace, set `workspace.Config.OverflowPath` to keep each full result in a file that the agent can `read`.
- **Interceptors.** `AgentOptions.ToolInterceptors` wrap every tool call. An interceptor can log, time, or shorten a result.

## CallContext

`*tacklr.CallContext` is the call's link to its session:

| Method | Does |
|---|---|
| `EmitUpdate(text)` | Sends progress to the clients that listen. Progress is not stored. |
| `Park(interrupt)` | Pauses the session to ask the user. See [Ask the user](#ask-the-user). |
| `StateGet`, `StateSet`, `StateDelete` | Read and write the session's key-value state. The host sets initial values with `CreateSession.State`, and later with `Prompt.State` or `Resume.State`. |
| `Schedule(ctx, JobRequest)` | Starts a job: a child session that runs a `session.JobHandler` that the worker registered in `Config.Jobs`. With `Wait`, the job's output is the call's result. |
| `Jobs`, `CancelJob` | List and stop the session's running jobs. |
| `CurrentToolCallID` | The call's id. Use it as the idempotency key for a side effect, because a step can run again after a crash. |

## Ask the user

`Park` pauses the session with a question. It returns an error the first time; return that error from the handler. When the user answers, the runtime runs the tool again, and `Park` returns the answer:

```go
answer, err := cc.Park(&tacklr.UserSelectionInterrupt{
	Question: "Which environment?",
	Options:  []tacklr.UserChoice{{Title: "staging"}, {Title: "production"}},
})
if err != nil {
	return "", err
}
env := answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title
```

Code before `Park` runs again when the tool runs again, so do side effects after it. A tool can define its own question type: implement `tacklr.Interrupt`, and register it with `tacklr.RegisterInterrupt` so that a paused session can read it back.

The model can also ask the user itself, with the built-in `ask_user_choice` tool.

## Permissions

`tacklr.ToolPermissionOnCall` asks the user before the tool runs:

```go
OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
```

The user can allow or reject the call, once or always. An "always" answer applies to the tool for the rest of the session.

- `AgentOptions.AllowTools` lists tools that run without the question, by the name the model calls them. A user's "reject always" still wins.
- A tool with write access (`ToolWriteAccess`) runs only after the agent has made a plan with `create_plan`. You cannot turn this off. Specialists skip it.
- The built-in workspace write tools and `run_command` ask for permission in the same way.

## Built-in tools

The harness adds tools for the features you turn on:

| You set | Tools |
|---|---|
| Always | `create_plan`, `edit_plan`, `list_plan`, `complete_todo`, `ask_user_choice` |
| `AgentOptions.Specialists` | `spawn_specialist`, `list_children`, `cancel_child` |
| `AgentOptions.Brain` | `search`, `open`, `related`; `save` with `BrainWriteKinds`; `link` when the store has a graph |
| `worker.Config.Workspace` | `read`, `write`, `write_document`, `write_spreadsheet` |
| `worker.Config.Workspace` and `Brain` | `index_file`, `unindex` |
| `worker.Config.Workspace` and `FuseRoot` | `run_command` |
| `AgentOptions.SkillsLoader`, or a workspace with a `skills` directory | `read_skill` |

## Web tools

Package `web` has `web_search` and `web_fetch` on [Exa](https://exa.ai):

```go
exa := web.NewExa(os.Getenv("EXA_API_KEY"))
agent.Tools = append(agent.Tools, web.WebSearch(exa), web.WebFetch(exa))
```

## MCP servers

Set `AgentOptions.MCP` to `mcp.Client{}` and list servers in `AgentOptions.MCPConfigs`:

```go
agent.MCP = mcp.Client{}
agent.MCPConfigs = []session.MCPServer{
	{Name: "github", Type: session.MCPHTTP, URL: "https://mcp.example.com/github"},
}
```

Each server's tools are namespaced by its name (`github__create_issue`). A server connects when the session's harness is built. Clients can also name servers for a session: over ACP, in `mcpServers` on `session/new` or `session/load`.

Credentials in `Env` and `Headers` are never stored. For a credential the session must keep, set `CredentialRef`, and resolve it with `AgentOptions.MCPCredentialResolver` just before each connection.

## Skills

A skill is a directory with a `SKILL.md` file of instructions for one task. The model sees each skill's name and description, and reads the full instructions with `read_skill` when it needs them.

Set `AgentOptions.SkillsLoader` to `skills.Loader{Root: dir}` to read skills from a local directory. With a workspace, skills load from `/workspace/skills` by default (`workspace.Config.SkillsPath`).

## Test a tool

A tool's constructor takes its clients, so a test passes a test client into the same constructor and runs the tool through a real harness. `CallContext` is a concrete type that the harness makes for each call; you do not mock it.
