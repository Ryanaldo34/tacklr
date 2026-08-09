// Package vfs is Tacklr's virtual filesystem: session mounts and path I/O.
//
// # Public surface (hosts)
//
//   - MountSession — session-owned mount table + virtual-path I/O
//   - BackendRegistry + LocalFactory / S3Factory — process profiles and pools
//   - MountSpec / MountInfo — durable and agent-safe mount descriptions
//   - Provider / ProviderFactory — implement custom backends
//   - File, FileInfo, DirEntry — I/O result types
//   - DetectMediaType — media-type hint for future content IR
//   - Sentinel errors (ErrNotMounted, ErrReadOnly, …)
//
// Hosts should not need anything else. Mount tables, host roots, and bucket
// details stay inside providers and the unexported mount table.
//
// # Path I/O
//
// Absolute virtual paths only, via MountSession:
//
//	Stat, Open, ReadFile, WriteFile, ReadDir, Remove, MkdirAll
//
// Read-only mounts reject mutating ops with ErrReadOnly. Local paths are jailed
// under the provider root (including symlink evaluation).
//
// # Content IR (future)
//
// Ops return raw bytes/streams. DetectMediaType(path, sample) is the hook for
// codecs; TextDocument IR is not wired into ReadFile yet.
package vfs
