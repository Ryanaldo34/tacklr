# Models

An agent's model is a `tacklr.Model`. Tacklr has two:

| Package | Talks to |
|---|---|
| `anthropic` | Claude models on any Anthropic Messages API: the Claude API, Microsoft Foundry, and gateways |
| `openai` | OpenAI, and any endpoint that serves the OpenAI Responses API: Azure OpenAI, OpenRouter, xAI, local servers |

Both are plain `net/http` clients. Each request carries its own prompt, so one model value is safe to share across sessions. A specialist can use a different model from the main agent (`Specialist.Model`).

## Claude

```go
model := anthropic.New(httpClient).
	WithApiKey(os.Getenv("ANTHROPIC_API_KEY")).
	WithModel("claude-opus-5-5").
	WithEffort("high")
```

`New(nil)` uses the settings of `http.DefaultClient`. Every request carries the trace context of its turn.

| Option | Default | Effect |
|---|---|---|
| `WithApiKey` | none (required) | The API key. |
| `WithModel` | none (required) | The model id, such as `claude-opus-5-5`, `claude-sonnet-5-5`, or `claude-haiku-5-5`. |
| `WithEffort` | the model's default | `low`, `medium`, `high`, `xhigh`, or `max`. Higher effort means more thinking and more tokens. The default differs by model: `claude-opus-5-5` defaults to `medium`. |
| `WithMaxTokens` | 64,000 | The limit on each response. |
| `WithURL` | `https://api.anthropic.com/v1` | The base URL of a Messages API: a gateway, or Microsoft Foundry (below). A model id with a gateway prefix (`vendor/claude-opus-5-5`) works. |
| `WithContextWindow` | from the model id | The context window in tokens, for a model id the client does not know, such as a deployment name. |

What the client does for you:

- **Thinking.** Current models think before they answer. The client asks for summarized thinking, which streams to clients as `EventReasoning`, and sends each thinking block back unchanged on later requests. Models older than adaptive thinking (Claude 4.5 and earlier) get no thinking settings.
- **Prompt caching.** The tools and the system prompt, and the plan, are cache breakpoints, and automatic caching covers the rest of the conversation. The next request of a session reads its prefix from the cache.
- **Token counting.** `CountTokens` uses the `count_tokens` endpoint.
- **Input.** Claude models accept text, JPEG, PNG, GIF, and WebP images, and PDF documents.
- **Context window.** 1M tokens for current models, and 200K for Claude 4.5 models and earlier.

### Platforms

| Platform | Use |
|---|---|
| Claude API | The default. |
| Microsoft Foundry | `WithURL("https://<resource>.services.ai.azure.com/anthropic/v1")`, the resource's API key, and the deployment name as the model. Set `WithContextWindow` when the deployment name does not start with `claude-`. Entra ID tokens are not supported. |
| Amazon Bedrock, Google Vertex AI | Not supported. They serve Claude through their own request formats and authentication (AWS SigV4, Google OAuth), and their OpenAI-compatible endpoints do not serve Claude. |

## OpenAI and compatible endpoints

```go
model := openai.New(httpClient).
	WithApiKey(os.Getenv("OPENAI_API_KEY")).
	WithModel("gpt-5-mini")
```

| Option | Default | Effect |
|---|---|---|
| `WithApiKey` | none (required) | The API key, sent as a bearer token. |
| `WithModel` | none (required) | The model or deployment id. |
| `WithURL` | `https://api.openai.com/v1` | The base URL of any Responses API endpoint. |
| `WithReasoningLevel` | none | The reasoning effort (`low`, `medium`, `high`). It also turns on reasoning summaries, so reasoning streams to clients. |
| `WithReasoningSummary` | none | The reasoning summary: `auto`, `concise`, or `detailed`. |

`openai.NewOpenRouter(httpClient)` is the client set up for OpenRouter.

The client speaks the Responses API only (`/responses`). An endpoint that serves only Chat Completions (`/chat/completions`) does not work with it.

- **Token counting.** `CountTokens` uses `POST /responses/input_tokens`. An endpoint without it, such as Azure OpenAI or a local server, makes Tacklr size requests from the usage that each response reports. See [Context window](context.md#size-of-a-request).
- **Prompt caching.** The client sends the session id as the prompt cache key. On xAI it also sends it as the conversation id.
- **Reasoning.** Encrypted reasoning items go back to the provider on later requests, so the client needs no provider-side storage.
- **Input.** The model id decides which images and files the model accepts. An unknown model accepts text only.

## Errors

Both clients map provider failures to the same errors, so the runtime and your clients handle every model the same way:

| Error | Means | What happens |
|---|---|---|
| wraps `tacklr.ErrNetwork` | A transport failure, a timeout, a rate limit, an overload, or a 5xx | The runtime retries the step. |
| `session.ErrContextLength` | The request is larger than the context window | The turn collapses the window and asks again, once. |
| `session.ErrModelRefused` | The model refused to answer | The turn ends with stop reason `refusal`. |
| `session.ErrMaxTokens` | The response reached its token limit | The turn ends with stop reason `max_tokens`. |
| `*anthropic.APIStatusError`, `*openai.APIStatusError` | Any other HTTP error, with its status and error code | The turn fails. |

## Write your own model

Implement `tacklr.Model`:

```go
type Model interface {
	Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error)
	CountTokens(ctx context.Context, req tacklr.ModelRequest) (int, error)
	MaxContextWindow() (int, error)
	ModelTelemetryIdentity() telemetry.ModelIdentity
	SupportsMIME(mimeType string) bool
}
```

`ModelRequest` holds the messages, the tools, the system prompt, and a cache key (the session id). The model holds no state for a request, so it must be safe to share.

`Invoke` sends one streaming request and returns a channel of chunks. It closes the channel when the response ends:

| Chunk | Send |
|---|---|
| Text or reasoning, as it streams | `Type` `EventMessage` or `EventReasoning`, `MessageId`, and the new `Content` |
| A complete text or reasoning item | The same type and `MessageId` with `IsComplete`. Put the provider's replay data for a reasoning item in `EncryptedContent`; you get it back on the message in later requests. |
| A tool call | `EventFunctionCall` with `IsComplete` and the call in `ToolCalls` |
| The end | `EventComplete` with the token usage. `InputTokens` is everything the request sent, cached tokens included. |
| A failure | `EventError` with `Error` and `IsComplete`, as the last chunk |

Send items in the order the model wrote them. The harness keeps that order in the window.

The other methods:

- `CountTokens` counts what `Invoke` would send, with the provider's API. A provider that cannot count returns an error that wraps `tacklr.ErrCannotCount`. Do not count with a local tokenizer.
- `MaxContextWindow` returns the window in tokens, or an error that wraps `tacklr.ErrUnknownModel`.
- `SupportsMIME` reports whether the model accepts a type of user input. Empty and `text/*` types are always true.
- `ModelTelemetryIdentity` names the provider and the model for the model spans.

Map failures to the errors in the table above. Wrap transport failures with `tacklr.Network(err)`.

To test a model, point it at an `httptest` server that speaks the provider's API. The tests of `anthropic` and `openai` show the pattern.
