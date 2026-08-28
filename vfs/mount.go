package vfs

import "maps"

// WorkspacePoint is the only top-level mount. Backends live at
// /workspace/<name> via Tree(At(...)).
const WorkspacePoint = "/workspace"

const workspaceProfile = "workspace"

// MountSpec is the durable, secret-free description of a mount.
// Safe to JSON into session checkpoints. Never store credentials here.
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
	// Members are /workspace aliases (params["name"]). The only top-level Point
	// is /workspace. Duplicate aliases → ErrAmbiguous.
	Members []MountSpec `json:"members,omitempty"`
}

func cloneSpec(spec MountSpec) MountSpec {
	out := spec
	out.Params = maps.Clone(spec.Params)
	if len(spec.Members) == 0 {
		out.Members = nil
		return out
	}
	out.Members = make([]MountSpec, len(spec.Members))
	for i, m := range spec.Members {
		out.Members[i] = cloneSpec(m)
	}
	return out
}
