package session

// reservedRuntimeStateKeys is the set of checkpoint keys owned by
// SessionManager modules. Each module registers its keys from init.
var reservedRuntimeStateKeys = map[string]struct{}{}

func reserveStateKeys(keys ...string) {
	for _, key := range keys {
		reservedRuntimeStateKeys[key] = struct{}{}
	}
}

// IsReservedRuntimeStateKey reports keys owned by SessionManager modules.
func IsReservedRuntimeStateKey(key string) bool {
	_, ok := reservedRuntimeStateKeys[key]
	return ok
}

// StripPlanKeys removes reserved module keys from a runtime state map so they
// are not exposed via user-facing StateGet after load.
func StripPlanKeys(state map[string]any) {
	for key := range reservedRuntimeStateKeys {
		delete(state, key)
	}
}
