// Package builtins is the host-facing battery pack for Tacklr.
//
// Optional tools (email, Exa web search) are constructed here with the same
// constructor-closure pattern as host tools. Add the results to
// AgentOptions.Tools. The harness does not inject them from AgentOptions
// fields. Planning and child-session tools stay harness-owned.
//
// VFS backend constructors live here so a host builds a /workspace tree from
// one import. Tree, At, Union, MountSession, and Provider stay in package vfs.
// brain.Open stays in package brain.
package builtins
