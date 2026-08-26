# What is Tacklr

Tacklr is an opinionated Go agent harness SDK. It is a framework, not a bag of helpers. It defines how a turn should run and how the harness is structured.

## Goals of the Project

Design decisions should follow these four ideas.

### Structured context

Context is built for the work in front of the agent, not for everything that has ever been said. Planning cycles (Adaptive Case Management) produce a to-do list. Completing a to-do rebuilds a hand-off for the next work. Unused history does not stay in the window. Specialists are the same idea at a larger grain: a nested session that returns only what the parent asked for.

### Cloud-native

Tacklr is meant to run in the cloud: Go, JSON-RPC protocols, optional Temporal for durable sessions. The same harness should embed in-process or sit behind `durable.Runtime`.

### A bounded agent world

The virtual filesystem gives the agent one read/write interface over the mounts the host chose (local disk, object storage, Drive, Graph, knowledge objects). The agent sees virtual paths. It does not see host paths, other sessions, or credentials. Checkpoints store mount recipes, not tokens or file bytes.

### Knowledge base

The brain is a host-owned store the agent can query: hybrid search, an optional graph, and namespaces. Retrieval is on demand so the window is not a dump of stale text.

# Coding Standards

Tacklr is written in golang, so all modern idioms are to be followed. This includes iterators, generics, stdlib packages such as slice, map, sync, and strings, modern error wrapping & unwrapping, and expressions in the `new()` built-in function.

## Refactoring

We encourage refactoring whenever possible. We always strive to keep code clean and maintainable. If you notice obvious smells, inefficiencies, or violations of coding standards, refactor to improve the code. Do this regardless if its code directly related to the feature you are working on or not. Always aim to build the most maintainable solution. Do not build band-aids and hacky patches, always build the best and most maintainable solution. Just fix the underlying architectural issue. Boil the ocean. 

## Dependencies

Go has a rich standard library. Whenever possible, use the standard library over third-party packages. Third-party packages should be used as a last resort.

## Testing

### Philosophy

We do not care about unit tests. We aim for high-value, outcome-oriented integration tests instead. With AI, the overhead of maintaining large integration tests instead of several smaller, more focused unit tests is not a factor anymore. Therefore, unit tests are not necessary in most cases. Prefer unit tests when we are testing significant, encapsulated implementation details which are not easily tested for in integration tests. We should not test implementation details when integration testing, and instead should test greater outcomes being met. We will not test for something *not existing* or *not happening* but should test for something that *should happen* or is *expected to happen* instead. This is much more effective in catching genuine regressions, making tests less brittle when implementation details change that don't change the outcome. We should aim for 100% coverage built primarily on integration tests covering as many branches as possible at once, with each test case testing a specific return path as an outcome. We should not have duplicate return path coverage across test cases. We should aim for brevity over test volume, or as few tests cases to get to 100% coverage as possible.

### Test Style Guide

- Follow AAA pattern (Arrange, Act, Assert)
- Use absolute minimal mocking and opt for using test containers or in-memory implementations whenever possible
- Test names should be self-documenting and be named after the test case they represent

## How to Write Code

- Prefer explicitness over cleverness
- Build small composable components with clear responsibilities rather than large abstractions that hide behavior
- Design around interfaces that represent capabilities rather than implementation details
- APIs consumers shouldn't have access to need to be encapsulated. As this is meant to be a public library, we don't want importers shooting themselves in the foot accessing APIs and data that should be encapsualted and hidden from the user.
- Prefer immutable inputs and explicit outputs whenever practical
- Build systems that can be inspected and tested at every stage
- Before introducing an abstraction, ask:
  - Does this abstraction add value or just obfuscate the code?
  - Does this reduce complexity?
  - Will this still make sense six months from now?
  - Could this be implemented as two smaller pieces instead?
- When multiple solutions exist:
  - Choose the simplest design that satisfies today's requirements
  - Prefer solutions that compose well with the existing architecture
  - Minimize framework-specific behavior
  - Reduce hidden state
  - Prefer deterministic implementations over "magic"

## Planning

When planning, be extremely detailed and verbose, offering several possible solutions we can explore together. Ask any clarifying questions or request any additional context or information for anything that is unclear. Never make any assumptions. Use ASD-STE100 for writing and nothing that falls outside of its general writing specifications, allowed vocabulary, etc.

# Agent Checklist Before Committing

- [ ] Coding standards have been reviewed and followed
- [ ] All tests related to code touched by you are passing
- [ ] All touched files have been formatted and linted `go fmt`, `go vet`, and `golangci-lint`
- [ ] Code coverage of your changes is 100%, with no duplicate return path coverage across tests
- [ ] Testability improvements are made as needed, to reduce mocking and make tests ACTUALLY useful
