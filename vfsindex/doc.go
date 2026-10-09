// Package vfsindex bridges a vfs.MountSession into brain knowledge objects.
//
// Canonical architecture: docs/knowledge.md. This package is only the artifact
// ingest path (IndexPath / policy / schedulers).
//
// VFS and brain work alone or together. This package is the optional
// composition layer: it imports both, while vfs and brain never import each other.
//
// # Model
//
// Each text-like virtual file becomes a brain Document (parent) with Chunk parts.
// The row contract (ids, property types, content_hash, and replace rules) is
// "File mirror rows" in docs/knowledge.md. In-process writers should call
// IndexPath or DocumentID rather than insert rows themselves.
//
//	Document id = UUID v5, namespace a1b2c3d4-e5f6-7890-abcd-ef1234567890,
//	              name = Namespace.String() + 0x00 + virtual path
//	Document.properties.vfs_path, content_hash, size, mtime, media_type
//	Chunk id    = UUID v5 under the Document id, name "chunk:<position>"
//	              or "block:<block id>"
//	Chunk.properties.start_line, end_line, byte_start, byte_end
//	Chunk.properties.block_id, heading_path = the block id, when structured
//	Chunk.Content = chunk body
//
// Replacing a file puts the parent, soft-deletes the previous chunks, then
// puts the new chunks. content_hash is lowercase hex SHA-256. The same hash
// skips the write. Live file bytes stay the source of truth.
//
// # Index policy (MountSpec.IndexPolicy)
//
//	none       — index_file errors
//	selective  — index_file / host IndexPath
//	prefix     — host may walk the mount with IndexPrefix
//	watch      — same host hint as prefix
//
// Empty policy normalizes to selective (NormalizePolicy / AutoIndex helpers).
// AutoIndex is for a pipeline the host builds. The turn does not walk mounts,
// subscribe to remote changes, or re-index after WriteFile.
//
// # Single pipeline
//
// File bytes become brain chunks only through IndexPath (or UnindexPath).
// The turn calls that from index_file and unindex. A host calls IndexPath
// or IndexPrefix from its own system. content_hash skip returns PathSkipped
// without re-chunking.
//
// # Session-visible body
//
// IndexPath uses MountSession.ReadText (markdown) and MountSession.Open (other
// text). Writes are write-through, so a later index_file sees the last persist.
//
// # Schedulers
//
// SyncScheduler runs IndexPath inline. AsyncScheduler enqueues with coalesce
// (last reason wins), a bounded pending set, and a background worker. Notify
// never blocks on re-chunk. Start creates an AsyncScheduler and does not
// call Notify.
//
// A turn with a brain, a workspace, and a search namespace starts a Bridge
// and registers index_file / unindex. Mounts with IndexPolicy=none are skipped
// by index_file. Keeping those chunks current after the sandbox exits is the
// host's storage pipeline, not this package.
//
// # Kinds
//
// Hosts that use a non-empty kind catalog should register MountIndexKinds()
// (or equivalent fields) before indexing. Open-catalog engines accept any props.
//
// Content search over mounts is brain search/find_exact on Chunks with vfs_path.
// Live grep is run_command → rg on the FUSE tree. This package does not implement grep.
package vfsindex
