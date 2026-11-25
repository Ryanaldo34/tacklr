# Agent Expectations

- Untrusted, agent generated bash or code scripts run only in Tacklr's sandbox workers: Firecracker microVMs on a sandbox host, never on the session worker, with no network and no credentials inside the VM
- Credentials and the session's data sources stay with the supervisor on the sandbox host; the guest reaches files only through the supervisor's 9P2000.L server over vsock, and only output goes back to the session runtime
- Everything we write is Go (the core, the supervisor, and the guest's worker) and follows all modern Go idioms
- You will never update this file or skills without explicit permission. You will never add documentation without explicit permission
- You will never update the README.md or CONTRIBUTE.md without explicit permission
- Do not run test suites locally that aren't relevant to your changes, aim for 100% coverage in your tests and run other basic CI checks locally before committing changes
- Do not commit and push without explicit permission per change
- Define modular components that can be tested in isolation without mocking code we explcitly own
- Add integration tests only for code we can't cover with unit tests with absolute minimal mocking. We want fast, outcome-oriented tests.
- Use realism in integration tests with local test containers
- When writing commit, PR, or changelog descriptions - keep it brief and TL;DR with bullet points so its quick and easy to scan and read. Nobody is reading paragraphs of information and "summaries". Mention breaking changes, new feature additions, and dependencies added
- You will not add OS-related workarounds for non linux operating systems. We only support linux for both development and production use and issues with other systems is not our problem
- Tacklr is a large SDK, so packages and modules will remain lightweight and decoupled from one another, allowing for small binary size and not bundling code and components which aren't used
- Untangle components and refactor as needed. Make the real fix and not temporary workarounds and bandaids
- Keep comments out of code and only write docstrings. No comments documenting workarounds or making strangely verbose observations and callouts

## Test Guide

- Follow AAA pattern (Arrange, Act, Assert)
- Use absolute minimal mocking and opt for using test containers or in-memory implementations whenever possible in integration tests
- Test names should be self-documenting and be named after the test case they represent
- Do not hack away at writing tests for goofy edge cases. If you need several tests for an edge case, its probably not an actual edge case or the implementation needs to be cleaner to avoid such edge cases
- Aim for 100% coverage, write unit tests without mocking code and boundaries we own. Mock only the lowest level components such as networks. Use integration tests to test contracts between modular components and code that isn't coverable in unit tests
- Each component & module should individually testable without heavy mocking of other components in unit tests
- Test observable outcomes, not heavy internal behavior that could change at anytime and therefore make tests rigid
- Avoid singletons and complex fixtures as those will lead to flaky tests that don't catch actual regressions

## Code Guide

- Modular design: each component or piece of the puzzle should be individually testable in isolation. Decouple heavily coupled components
- Procedural Style: we don't want endless interfaces, fake encapsulation, and pointless getters & setters. Avoid enterprise java-like code. Do not make interfaces for the sack of writing mocks easily, this is bad. Interfaces should only be used polymorphism is actually needed outside of testing mocks. Getters/setters should only be used when locking is needed, everything else should just be public. Favor actual encapsulation at the package/module level by not exporting things that are internal details a programmer should have access to. Keep items private that would expose heavy internal details to a consumer unnecessarily. Everything else should be public.
- Keep call stack flat and minimal: we don't want endless helper function wrappers so the implementation should be readable without having to click through several pointless wrappers which don't add any testability or reusability benefits
- All possible errors must be handled at the boundary and explcitly documented. Errors that can be handled internally should be handled internally, only surface errors the caller or consumer needs to know about.
- Keep error wrapping absolutely minimal and only wrap an error when there is a benefit to conditionally implementing specific error handling logic. Keep error descriptions minimal and descriptive to what went wrong and reconcillation steps. Also include any relevant data that can be used to handle the error or log an unrecoverable crash
- Telemetry spans should have static attributes, the same trace should go through each system so the trace has the full picture across all systems it touches. Logs within spans should contain the dynamic information. Keep spans minimal and create a span for grouped work that is undeterministic such as anything related to I/O. Anything worth exposing in telemetry to give engineers the full picture of performance and behavior should be logged and exposed. End-to-end observability is a hard requirement

# What is Tacklr

Tacklr is a linux-based cloud-native agent operating & security system. It is a framework for defining & building tools, processing input & output, wiring telemetry & logging into arbitrary observability stacks, securely running untrusted llm-generated code in microVM sandboxes, and handling complex parallel agent swarms & workflows.
- A core that handles tool calls, inference/model API quirks, async subagents and long-running commands, parallelism, and agent swarm coordination; written in go and can compile to lightweight WASM to run in JS runtimes
- A supervisor on each sandbox host that boots, watches and kills Firecracker microVMs for the session runtime over mTLS, holds the user's delegated credentials, and serves each VM's /workspace from the session's tree
- A Go worker that is the init of each microVM: it mounts /workspace and runs the supervisor's scripts with native bash and tools
- Cloud-nativeness built to run on durable runtimes such as Azure Durable Functions, Lambda Durable Functions, Temporal, Durable Objects, and more. We do not care about local.
- Worker runtime monitoring and configurable security policies; risky calls reviewable by security teams for approval/denial

# Agent Checklist Before Committing

- [ ] Coding standards have been reviewed and followed
- [ ] All tests related to code touched by you are passing
- [ ] All touched files have been formatted and linted `go fmt`, `go vet`, and `golangci-lint`
- [ ] Code coverage of your changes is 100%, with no duplicate return path coverage across tests
- [ ] Testability improvements are made as needed, to reduce mocking and make tests ACTUALLY useful
