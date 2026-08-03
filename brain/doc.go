// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain; harness builtins call into it.
// This package does not import the harness or session packages. Scope is passed
// in by the caller (the harness derives it from the host-set session namespace).
// Knowledge storage (Store) is separate from stores.BaseStore session checkpoints.
package brain
