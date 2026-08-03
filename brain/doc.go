// Package brain is Tacklr's knowledge-base retrieval engine.
//
// Hosts attach an Engine via AgentOptions.Brain; harness builtins call into it.
// This package does not import the harness or session packages. Scope is passed
// in by the caller (the harness derives it from the host-set session namespace).
// Knowledge storage (Store) is separate from stores.BaseStore session checkpoints.
//
// Expand uses Postgres containment (parent_id) for hierarchy and an optional
// GraphReader for other relations. Tests use MemoryGraph. Production Helix
// adapter lives in package brain/helixgraph (keeps this package free of the
// Helix SDK). Configure the graph on the Engine before NewAgent, e.g.:
//
//	g, _ := helixgraph.New("http://localhost:6969") // helix start dev
//	eng, _ := brain.NewEngine(store, brain.WithGraph(g))
//
// A new search, find_exact, or large expand replaces the session SearchContext
// ResultSet; continue always pages the latest set.
package brain
