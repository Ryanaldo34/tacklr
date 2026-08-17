// Package hostcontrol gates server-only AgentHarness capabilities. Go's
// internal import rule prevents external SDK consumers from constructing Token.
package hostcontrol

// Token authorizes server adapters in this module to call host-only harness
// lifecycle methods. It carries no runtime authority or secret.
type Token struct{}
