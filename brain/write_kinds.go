package brain

// WriteKinds maps agent save_* tools to host-registered kind names.
// Empty fields skip registering that tool. Kinds must exist in the catalog
// when the catalog is non-empty (ApplyKinds / WithKinds).
type WriteKinds struct {
	Discovery string // e.g. "Discovery"
	Fact      string // e.g. "Fact" or "MemoryFact"
	Memory    string // e.g. "Memory" or "MemoryPreference"
}
