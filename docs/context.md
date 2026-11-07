# Context window

This guide explains how Tacklr keeps each model request inside the model's context window: how a request is sized, and what happens when the window is full.

## Rules

- The window holds **complete rounds** only. A round is a user message, or a model reply and a result for each of its tool calls.
- The window changes only when a round commits, or when a plan or a hand-off replaces it. A model request does not change it.
- The size of a request comes from the provider. Tacklr does not count tokens with a local tokenizer.

## Size of a request

Before a round is added, the window estimates the next request's size from three sources, in this order:

1. **A bound.** The window keeps the provider's count of the last request. It adds the size in bytes of the messages that came after it. A token is at least one byte, so the bound is never below the real count. When the bound is under the pressure (85% of the window), the round is added and the provider is not asked.
2. **The provider's count.** Over the pressure, the window asks the provider for an exact count (`Model.CountTokens`).
3. **The bound alone.** When the provider cannot count (`CountTokens` returns `tacklr.ErrCannotCount`), the window uses the bound for the rest of the session. The bound is high for text (about four bytes for each token), so such a window collapses earlier.

The window's size is `AgentOptions.MaxWindowSize`. Zero uses the model's `MaxContextWindow`.

## When the window is full

Over the pressure, the window makes room in this order:

1. **Collapse.** Everything after the user's first message and the plan becomes one summary that the model writes. The new round goes after the summary. Reasoning in the messages that stay is dropped, because it was about messages that the summary replaced.
2. **Cut.** When the round alone is still larger than the window, its largest tool results are cut until it fits. Each cut result ends with a note that says how much was kept. With `AgentOptions.Overflow` (the workspace's `OverflowPath`), the full result is kept in a file first, and the note gives the path to `read`.

A round with nothing to cut fails the commit. When any part of a commit fails (a count, the summary, a save), the commit saves nothing, and the window stays as it was.

With a knowledge store, the messages a collapse removes are saved as the session's searchable messages, and the summary tells the agent that `search` finds them.

## Plans and hand-offs

These replace the window, so the round that asks for them goes in without a fit:

- **A plan** (`create_plan`): the window becomes the user's first message and the plan.
- **A hand-off** (`complete_todo` while other to-dos are open): the model writes a hand-off of the window, and the window becomes the first message, the plan, the hand-off, and a note to continue. When the model fails or writes nothing, the hand-off is built from the plan.

## A request the provider refuses

The bound can be too low, for example when a message refers to a file that is larger in tokens than in bytes. When the provider refuses a request as too long (`session.ErrContextLength`):

1. The turn collapses the window. The summary covers the messages that the last counted request carried, which the provider accepted, so the summary request fits.
2. The model is asked again, once.

A second refusal, or a window with nothing to collapse, fails the turn with `session.ErrContextLength`.

## Checkpoints

The window is saved in the session's checkpoint: one row for each message, and one row for the last count. A save writes only the rows that changed, so adding a round writes only its messages. A restored session, such as a Durable Object after eviction, sizes its next request from the saved count.

The system prompt and the tools stay the same for the life of a session's harness, so the provider's prompt cache and the saved count stay valid. A harness is built again only when the session's MCP servers or credentials change.

## What a model must do

A `tacklr.Model` takes part in sizing. It counts with the provider's API, reports the input tokens of each response, and reports a request that is too long as `session.ErrContextLength`. See [Models](models.md#write-your-own-model).
