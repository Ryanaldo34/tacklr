package brain

// WriteKinds maps agent save_* tools to host kind names.
// Empty fields omit that tool. When the process catalog is non-empty, named
// kinds must already be registered (ApplyKinds / WithKinds).
type WriteKinds struct {
	Discovery string // save_discovery
	Fact      string // save_fact
	Memory    string // save_memory
}
