package vfs

import "maps"

// SkillsPoint is the conventional union mount for skill packs.
const SkillsPoint = "/skills"

// WorkspacePoint is the conventional named union for user-owned cloud folders.
const WorkspacePoint = "/workspace"

const (
	skillsProfile    = "skills"
	workspaceProfile = "workspace"
)

// Skills builds the read-only /skills union. IndexPolicy is none so playbooks
// are not ingested as brain artifacts. members are factory SkillMember specs.
func Skills(members ...MountSpec) MountSpec {
	out := make([]MountSpec, len(members))
	for i, m := range members {
		cp := cloneSpec(m)
		cp.Point = ""
		out[i] = cp
	}
	return MountSpec{
		Point:       SkillsPoint,
		Profile:     skillsProfile,
		ReadOnly:    true,
		IndexPolicy: "none",
		Members:     out,
	}
}

// Workspace builds the named writable /workspace union. Each member is one
// alias directory (params["name"]). Duplicate aliases are ErrAmbiguous.
// MkdirAll of a new alias is ErrNotSupported — bind creates aliases.
func Workspace(members ...MountSpec) MountSpec {
	out := make([]MountSpec, len(members))
	for i, m := range members {
		cp := cloneSpec(m)
		cp.Point = ""
		out[i] = cp
	}
	return MountSpec{
		Point:    WorkspacePoint,
		Profile:  workspaceProfile,
		ReadOnly: false,
		Members:  out,
	}
}

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
	// Members, when non-empty, make this mount a union of those backends.
	// Profile is a label only (not opened as a factory). Member Point must be
	// empty; members are not separate mount points. Skills (/skills) is a
	// read-only first-level merge. Workspace (/workspace) is a named writable
	// union: the first path segment is the member alias (params["name"]).
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
