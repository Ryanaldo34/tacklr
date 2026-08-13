package vfs

import "maps"

// MountSpec is the durable, secret-free description of a mount.
// Safe to JSON into session checkpoints. Never store credentials here —
// Profile names a process-level factory that holds clients/pools.
type MountSpec struct {
	Point    string            `json:"point"`
	Profile  string            `json:"profile"`
	ReadOnly bool              `json:"readOnly,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
	// IndexPolicy controls when the optional vfsindex pipeline runs for paths
	// under this mount: none | selective | prefix | watch. Empty means selective
	// when the harness bridge is enabled. vfs stores the string only; interpretation
	// lives in vfsindex/harness.
	IndexPolicy string `json:"indexPolicy,omitempty"`
}

// MountInfo is agent-safe: point and read-only only.
type MountInfo struct {
	Point    string
	ReadOnly bool
}

func cloneSpec(spec MountSpec) MountSpec {
	out := spec
	out.Params = maps.Clone(spec.Params)
	return out
}
