// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - MountSession — mounts, path I/O, ReadText / WriteDocument, ReadLines, FuseMount / Close, HostDir
//   - FuseAvailable — process can mount a kernel tree (/dev/fuse or /dev/macfuse*)
//   - ContentRev / ContentHash — session-visible content identity (for tools)
//   - BackendRegistry + LocalFactory / S3Factory / DriveFactory / GraphFactory + AWSS3 — process profiles
//   - SessionAuth + TokenHolder + Binding — session-scoped user-owned credentials (never on MountSpec)
//   - MountSpec — durable mount description (checkpoint-safe; Members = skills or /workspace union)
//   - Skills / SkillsPoint — /skills flat read-only union from SkillSource factories
//   - Workspace / WorkspacePoint — /workspace named writable union (cloud aliases)
//   - Provider / ProviderFactory / S3API / DriveAPI — custom backends
//   - File, FileInfo, DirEntry — I/O types (File is Close+Stat; io.Reader / io.ReaderAt / io.Writer via comma-ok)
//   - Document / Textual / Structured / TextDocument — content IR
//   - Block / StyleMeta / Span / FindBlock / BlockReplaceSpan — structured view
//   - ContentRegistry + Codec + TextCodec + IdentityCodec — optional custom decode
//   - DetectMediaType, size-cap constants, sentinel errors (including ErrFuseNotMounted, ErrAuthExpired, ErrAmbiguous, ErrPermission)
//
// Providers own IR translation and persist immediately on WriteDocument.
// MountSession routes; it does not encode or hold a dirty document cache.
//
// Optimistic edit policy lives in harness tools (read, write) that wrap
// ReadText / TextDocument / WriteDocument. ContentRev is the stable token
// those tools pass — not a large MountSession surface.
//
// This package never imports brain. Brain implements Provider in package brain
// (Engrams as Markdown files). Optional artifact IndexPath lives in package
// vfsindex (imports both; skips Profile=="brain"). FUSE / host rg read
// ReadText (provider IR plaintext). This package does not ship a grep tool.
//
// Hosts should not need anything else. Mount tables, host roots, and bucket
// details stay inside providers and the unexported mount table.
//
// # Path I/O
//
// Absolute virtual paths only, via MountSession:
//
//	Stat, Open, ReadFile, WriteFile, ReadDir, Remove, MkdirAll, FuseMount, Close
//	File is Close + Stat. Read / ReadAt / Write are optional (comma-ok).
//	FuseMount is explicit (host kernel tree). Mount points must be one segment
//	(/work, /engram, /workspace). If ReadText succeeds, the kernel sees that plaintext
//	(read-only unless IdentityCodec). Otherwise Stat + io.ReaderAt. Close
//	unmounts. HostDir is the last FuseMount directory.
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
//	// Full IR for edit; WriteDocument persists through the provider now
//	text, err := ms.ReadText(ctx, "/work/main.go") // Textual
//	rev := vfs.ContentRev{Path: text.Path(), Hash: vfs.ContentHash(text.Text())}
//	_ = text.SetLine(2, "changed")
//	_ = ms.WriteDocument(ctx, text)
//	_ = rev // tools compare expected rev before WriteDocument
//
// Longer guide: docs/vfs.md in the repo root.
package vfs
