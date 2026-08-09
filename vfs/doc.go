// Package vfs is Tacklr's virtual filesystem mount layer.
//
// # Phase 1 surface
//
// This package currently provides a Unix-style mount table and provider
// attachment only:
//
//   - FS holds an isolated mount namespace (one instance per scope you choose)
//   - Mount / Unmount / Mounts manage attachments at absolute virtual paths
//   - Lookup resolves a virtual path to the longest covering mount
//   - LocalProvider and S3Provider are the first backends
//
// File operations (Open, Read, Write, Stat, ReadDir, …), content intermediate
// representations, and agent-facing tools are intentionally out of scope here.
// They will use this mount table without changing how hosts attach sources.
//
// # Security model
//
// Hosts configure providers with real roots and credentials. Agents must only
// ever see virtual paths under the FS namespace. MountInfo never exposes host
// paths, bucket names, credentials, or backend type. Providers stay opaque;
// routing is by virtual path only. Scope is a property of what you mount and
// (later) which paths ops allow — not a prompt instruction.
//
// # Mount rules (core)
//
// Virtual paths are absolute POSIX paths (slash-separated, always start with
// "/"). Exact mount points are unique. Nested mounts are allowed; Lookup uses
// longest-prefix match with path segment boundaries. Mounts may be read-only
// (Mount's readOnly flag); enforcement lands with file ops. Unmount is by exact
// point. Phase 1 has no open-handle busy check yet.
//
// Providers must be treated as immutable after a successful Mount.
package vfs
