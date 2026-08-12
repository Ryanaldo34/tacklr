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
//	Chunk.properties.block_id, heading_path  = when chunked from Structured IR
//	Chunk.Content                 = chunk body (heading blocks for Markdown; line windows otherwise)
//
// Live VFS bytes remain source of truth. The index is derived and may lag until
// re-index (IndexPath / IndexScheduler.Notify).
//
// # Session-visible body
//
// IndexPath uses MountSession.ReadText (markdown) and MountSession.Open (other
// text). Both honor the session dirty IR cache, so index_file / IndexPath see the
// same body the agent sees after write/replace_* — even before Sync. AfterPersist
// (WriteFile / Sync) still drives background reindex of persist-only paths so
// search stays warm after durable writes.
//
// # Schedulers
//
// Hosts wire Notify after writes via vfs.MountSession.SetAfterPersist:
//
//	idx, err := vfsindex.NewMountIndexer(ms, eng, scope)
//	sched := vfsindex.NewAsyncScheduler(idx) // or NewSyncScheduler for inline
//	prev := ms.GetAfterPersist()
//	ms.SetAfterPersist(func(ctx context.Context, path string) error {
//	    if prev != nil {
//	        _ = prev(ctx, path)
//	    }
//	    return sched.Notify(ctx, path, vfsindex.ReasonSync)
//	})
//	defer sched.Close()
//	_ = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})
//
// SyncScheduler runs IndexPath inline (tests / hosts that want blocking reindex).
// AsyncScheduler enqueues with coalesce (last reason wins), bounded pending set,
// and a background worker; Notify never blocks on re-chunk.
//
// The tacklr harness creates MountIndexer + AsyncScheduler and registers
// index_file / unindex when Brain + VFS + search namespace are all set.
//
// # Kinds
//
// Hosts that use a non-empty kind catalog should register MountIndexKinds()
// (or equivalent fields) before indexing. Open-catalog engines accept any props.
//
// Content search over mounts is brain search/find_exact on Chunks (and later
// host OS tools via FUSE). This package does not implement grep.
package vfsindex
