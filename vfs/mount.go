package vfs

import "maps"

// WorkspacePoint is the only top-level mount. Backends live at
// /workspace/<name> via Tree(At(...)).
const WorkspacePoint = "/workspace"

// MountSpec is the durable, secret-free description of a mount.
// Safe to JSON into session checkpoints. Never store credentials here.
type MountSpec struct {
	Point    string            `json:"point"`
	Profile  string            `json:"profile"`
	ReadOnly bool              `json:"readOnly,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
	// NotIndexed makes index_file refuse files on this mount.
	NotIndexed bool `json:"notIndexed,omitempty"`
}

func cloneSpec(spec MountSpec) MountSpec {
	out := spec
	out.Params = maps.Clone(spec.Params)
	return out
}
