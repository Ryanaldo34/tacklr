// Package vfsindex bridges a vfs.MountSession into brain knowledge objects.
//
// Canonical architecture: docs/knowledge.md. This package is only the artifact
// ingest path (IndexPath, IndexFile, UnindexPath).
//
// VFS and brain work alone or together. This package is the optional
// composition layer: it imports both, while vfs and brain never import each other.
//
// # Model
//
// Each text-like virtual file becomes a brain Document (parent) with Chunk parts.
// The row contract (ids, property types, versions, and replace rules) is
// "File mirror rows" in docs/knowledge.md. In-process writers should call
// IndexPath or DocumentID rather than insert rows themselves.
//
//	Document id = UUID v5, namespace a1b2c3d4-e5f6-7890-abcd-ef1234567890,
//	              name = Namespace.String() + 0x00 + virtual path
//	Document.properties.vfs_path, source_version, size, mtime, media_type
//	Chunk id    = from the chunk's content (Engine.PutTree)
//	Chunk.properties.start_line, end_line, byte_start, byte_end
//	Chunk.properties.block_id, heading_path = the block id, when structured
//	Chunk.Content = chunk body
//
// Plain text streams through content-defined chunking (ChunkSizes): a
// boundary depends only on nearby bytes, so an edit changes only the chunks
// around it. Markdown, Docs and Sheets chunk by block. A file whose
// FileInfo.Version and readers match the Document is not opened. A re-index
// embeds and writes only chunks whose text changed. Live file bytes stay the
// source of truth.
//
// # Mounts that are not indexed
//
// A member mounted with vfs.Member.NotIndexed (MountSpec.NotIndexed) is not
// indexed: index_file refuses its files, and IndexPath skips
// them. The turn does not walk mounts, subscribe to remote changes, or
// re-index after WriteFile.
//
// # Single pipeline
//
// File bytes become brain chunks only through IndexPath (or UnindexPath).
// The turn calls that from index_file and unindex. A host calls IndexPath
// from its own system. A version match returns PathSkipped
// without reading the file.
//
// # Session-visible body
//
// IndexPath uses MountSession.ReadText (structured types) and MountSession.Open
// (other text). Writes are write-through, so a later index_file sees the last persist.
//
// # In a turn
//
// A turn with a brain and a workspace builds one MountIndexer and registers
// index_file / unindex over it. Keeping chunks current after the sandbox
// exits is the host's storage pipeline: it calls IndexPath itself.
//
// # Kinds
//
// Hosts that use a non-empty kind catalog should register MountIndexKinds()
// (or equivalent fields) before indexing. Open-catalog engines accept any props.
//
// Content search over mounts is brain search on Chunks with vfs_path.
// Live grep is run_command → rg on the FUSE tree. This package does not implement grep.
package vfsindex
