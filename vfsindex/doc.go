// Package vfsindex bridges a vfs.MountSession into brain knowledge objects.
//
// VFS and brain work alone or together. This package is the optional
// composition layer: it imports both, while vfs and brain never import each other.
//
// # Model
//
// Each text-like virtual file becomes a brain Document (parent) with Chunk parts:
//
//	Document.properties.vfs_path  = virtual path
//	Document.properties.content_hash, size, mtime, media_type
//	Chunk.properties.start_line, end_line, byte_start, byte_end
//	Chunk.Content                 = chunk body (line-oriented windows)
//
// Live VFS bytes remain source of truth. The index is derived and may lag until
// re-index (IndexPath / IndexScheduler.Notify). Hosts wire Notify after writes
// via vfs.MountSession.SetAfterPersist + SyncScheduler (v1) or a future
// AsyncScheduler with the same IndexScheduler interface.
//
// # Kinds
//
// Hosts that use a non-empty kind catalog should register MountIndexKinds()
// (or equivalent fields) before indexing. Open-catalog engines accept any props.
//
// # Usage
//
//	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
//	sched := vfsindex.NewSyncScheduler(idx)
//	ms.SetAfterPersist(func(ctx context.Context, path string) error {
//	    return sched.Notify(ctx, path, vfsindex.ReasonSync)
//	})
//	_ = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
//
// Content search over mounts is brain search/find_exact on Chunks (and later
// host OS tools via FUSE). This package does not implement grep.
package vfsindex
