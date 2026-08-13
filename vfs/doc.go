// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - MountSession — mounts, path I/O, ReadText / WriteDocument / Sync, ReadLines, SearchText
//   - ContentRev / ContentHash — session-visible content identity (for tools)
//   - BackendRegistry + LocalFactory / S3Factory + AWSS3 — process profiles and pools
//   - MountSpec / MountInfo — durable and agent-safe mount descriptions
//   - Provider / ProviderFactory / S3API — custom backends
//   - File, FileInfo, DirEntry — I/O types (FileInfo.MediaType is the provider's type)
//   - Document / Textual / Structured / TextDocument — content IR
//   - Block / StyleMeta / Span / FindBlock / BlockReplaceSpan — structured view
//   - ContentRegistry + Codec + TextCodec — optional custom decode
//   - DetectMediaType, size-cap constants, sentinel errors
//
// Cache, invalidation, and dirty tracking are internal. Hosts use Sync / SyncAll
// to flush; harness checkpoint calls SyncAll before saving mount Specs.
//
// Optimistic edit policy lives in harness tools (read_lines, replace_lines,
// replace_text, write) that wrap ReadText / TextDocument / WriteDocument.
// ContentRev is the stable token those tools pass — not a large MountSession surface.
//
// This package never imports brain. Brain implements Provider in package brain
// (Engrams as Markdown files). Optional artifact IndexPath lives in package
// vfsindex (imports both; skips Profile=="brain"). SearchText is session-visible
// plaintext (dirty IR) for a future FUSE / host rg. This package does not ship a grep tool.
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
// under the provider root (including symlink evaluation). S3 uses key prefixes
// and delimiter listing for virtual directories (MinIO/AWS compatible).
//
// # Content IR
//
// Raw ops stay byte-oriented. Content access:
//
//	// Progressive page (large files OK; EOF/NextStart for paging)
//	win, err := ms.ReadLines(ctx, "/work/main.go", 1, 51)
//	// win.Rev.Hash is the session-visible content identity when available
//
//	// Full IR for edit; WriteDocument stages dirty cache until Sync
//	text, err := ms.ReadText(ctx, "/work/main.go") // Textual
//	rev := vfs.ContentRev{Path: text.Path(), Hash: vfs.ContentHash(text.Text())}
//	_ = text.SetLine(2, "changed")
//	_ = ms.WriteDocument(ctx, text)
//	_ = ms.SyncAll(ctx) // or harness checkpoint
//	_ = rev // tools compare expected rev before WriteDocument
//
// Session content cache: clone-on-read, write-back IR, Sync flushes to backend.
// Checkpoint stores mount Specs only (not file bytes).
//
// Longer guide: docs/vfs.md in the repo root.
package vfs
