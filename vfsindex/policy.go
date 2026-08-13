package vfsindex

import "strings"

// Index policy values stored on vfs.MountSpec.IndexPolicy.
// Empty policy normalizes to PolicySelective.
const (
	PolicyNone      = "none"
	PolicySelective = "selective"
	PolicyPrefix    = "prefix"
	PolicyWatch     = "watch"
)

// NormalizePolicy returns a canonical policy string.
// Unknown or empty values become PolicySelective.
func NormalizePolicy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case PolicyNone:
		return PolicyNone
	case PolicyPrefix:
		return PolicyPrefix
	case PolicyWatch:
		return PolicyWatch
	case PolicySelective, "":
		return PolicySelective
	default:
		return PolicySelective
	}
}

// AutoIndex reports whether AfterPersist / IndexPrefix should run for policy.
// prefix and watch both auto-index; selective and none do not (except track set).
func AutoIndex(policy string) bool {
	switch NormalizePolicy(policy) {
	case PolicyPrefix, PolicyWatch:
		return true
	default:
		return false
	}
}
