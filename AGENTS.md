# What is Tacklr

Tacklr is a publicly distributed, open source agent harness SDK. The SDK is opinionated, meaning it is a framework not a library. Tacklr defines how we think agents *should* be built and how the harness should be structured.

 ## Goals of the Project

 Tacklr was built for a few core reasons and all design decisions should be made with our ethos in mind.

 ### A Better Way of Context Structuring

 Rather than just summarizing the context window generally when it gets close to filling up or naively trimming / summarizing the oldest messages in the context window to make room, Tacklr takes a structured approach to context management. Context is structured & optimized for the current task at hand. Our advanced planning cycles rooted in Adaptive Case Management force the agent to build detail plans for each task in the form of a "to-do list" of subtasks to complete the task or project given. Upon completion of a to-do, the agent marks it complete and the context is updated to produce a "hand-off" of context structured for the next to-do(s) to be completed. This way, it optimizes cost, context structure, and performance as needless bloat & irrelevant context does not need to be kept in the context window. Think of it like subagents being spawned and only returning the necessary information requested by the orchestrator.

 ### Cloud-Native Design

 Tacklr is designed to run natively in the cloud, leveraging cloud-native technologies and services. It is built to be scalable, fault-tolerant, and easy to deploy. This is why we've chosen to build Tacklr using Go and why we support a variety of JSON-RPC based protocols for communication with the agent.

 ### Security

 Security is horrendous when it comes to AI agents. Our state-of-the-art virtual execution environment & filesystem (eventually will be implemented) allow for the agent to only have access to what it should, and not know about anything else outside of its scope. No more `rm -rf /`, unexpected tool/process running, or prompt injection attack oopsies! And thanks to the virtual filesystem, the agent doesn't know where files are coming from because to it, it all looks the same with one single read/write interface across cloud buckets, local filesystems, google drive, sharepoint, dropbox, and more. Think how virtual memory works in operating systems -- the running process has no idea where the memory is coming from or what other processes are doing, it just knows it has access to it.

### The Knowledge Base or "Brain"

The ship of tradiional RAG has sailed long ago. Temporal & graph-based semantic search has proven much more effective in the age of AI. That is why we will be offering these search utilities natively in Tacklr. The agent will have access to a self-maintained knowledge base that can be queried for information and memory efficiently and accurately. Never let out-of-date information pollute your agent's context.

# Coding Standards

Tacklr is written in golang, so all modern idioms are to be followed. This includes iterators, generics, stdlib packages such as slice, map, sync, and strings, modern error wrapping & unwrapping, and expressions in the `new()` built-in function.

## Refactoring

We encourage refactoring whenever possible. We always strive to keep code clean and maintainable. If you notice obvious smells, inefficiencies, or violations of coding standards, refactor to improve the code. Do this regardless if its code directly related to the feature you are working on or not. Always aim to build the most maintainable solution. Do not build band-aids and hacky patches, always build the best and most maintainable solution. Just fix the underlying architectural issue. Boil the ocean. 

## Dependencies

Go has a rich standard library. Whenever possible, use the standard library over third-party packages. Third-party packages should be used as a last resort.

## Testing

### Philosophy

We do not care about unit tests. We aim for high-value, outcome-oriented integration tests instead. With AI, the overhead of maintaining large integration tests instead of several smaller, more focused unit tests is not a factor anymore. Therefore, unit tests are not necessary. We should not test implementation details, and instead should test greater outcomes being met. We will not test for something *not existing* or *not happening* but should test for something that *should happen* or is *expected to happen* instead. This is much more effective in catching genuine regressions, making tests less brittle when implementation details change that don't change the outcome. We should aim for 100% coverage built on integration tests, with each test case testing a specific return path as an outcome. We should not have duplicate return path coverage across test cases. We should aim for brevity over test volume, or as few tests cases to get to 100% coverage as possible.

### Test Style Guide

- Follow AAA pattern (Arrange, Act, Assert)
- Use absolute minimal mocking and opt for using test containers or in-memory implementations whenever possible
- Test names should be self-documenting and be named after the test case they represent

## How to Write Code

- Prefer explicitness over cleverness
- Build small composable components with clear responsibilities rather than large abstractions that hide behavior
- Design around interfaces that represent capabilities rather than implementation details
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
