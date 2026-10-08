// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - Tree / At / Union / OpenVFS — host builds one /workspace tree per turn
//     (agent-visible). Union merges Opens at one alias. Skill catalogs use a
//     SkillsPath on that tree (empty means /workspace/skills). With no workspace,
//     SkillsPath is a local directory.
//   - MountSession — path I/O, ReadText / WriteDocument, ReadLines, FuseMount / Close, HostDir
//   - FuseMount / FuseProbe — kernel tree; the mount error means FUSE is unavailable
//   - ContentRev / ContentHash — session-visible content identity (for tools)
//   - SessionAuth + TokenHolder + Binding — session-scoped user-owned credentials (never on MountSpec)
//   - MountSpec — durable mount description (checkpoint-safe; one record per /workspace/<name>)
//   - WorkspacePoint — /workspace (the only top-level mount)
//   - Provider / Open / S3API / DriveAPI / GraphAPI — custom backends (Blob uses S3API).
//     Hosts construct default backends from this package (Local, S3, Blob,
//     Drive, Graph, Memory, NewGoogleDrive, NewGraph).
//   - File, FileInfo, DirEntry — I/O types (File is Close+Stat; io.Reader / io.ReaderAt / io.Writer via comma-ok)
//   - Document / Textual / Structured / TextDocument — content IR
//   - Block / StyleMeta / Span / FindBlock / BlockReplaceSpan — structured view
//   - ContentRegistry + Codec + TextCodec + IdentityCodec — optional custom decode
//     (Register is first-wins; Lookup is exported; overwrite returns ErrAlreadyRegistered)
//   - DetectMediaType, size-cap constants, sentinel errors (including ErrFuseNotMounted, ErrAuthExpired, ErrAmbiguous, ErrPermission, ErrAlreadyRegistered)
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
// Hosts should not need anything else. Host paths and bucket details stay
// inside providers. MountSession only keeps the mount map and routes paths.
//
// # Path I/O
//
// Absolute virtual paths only, via MountSession:
//
//	Stat, Open, ReadFile, WriteFile, ReadDir, Remove, MkdirAll, FuseMount, Close
//	File is Close + Stat. Read / ReadAt / Write are optional (comma-ok).
//	FuseMount is explicit (host kernel tree). The only mount point is /workspace.
//	If ReadText succeeds, the kernel sees that plaintext
//	(read-only unless IdentityCodec). Otherwise Stat + io.ReaderAt. Close
//	unmounts. HostDir is the last FuseMount directory.
//
// Read-only mounts reject mutating ops with ErrReadOnly. Local paths are jailed
// under the provider root (including symlink evaluation). S3 and Azure Blob use
// key prefixes and delimiter listing for virtual directories (MinIO/AWS/Azurite).
//
// # Content IR
//
// Raw ops stay byte-oriented. Content access:
//
//	// Progressive page (large files OK; EOF/NextStart for paging)
//	file := ms.Route(ctx, "/workspace/work/main.go")
//	win, err := file.ReadLines(ctx, 1, 51)
//	// win.Rev.Hash is the session-visible content identity when available
//
//	// Full IR for edit; the provider persists WriteDocument
//	text, err := file.ReadText(ctx) // Textual
//	rev := vfs.ContentRev{Path: text.Path(), Hash: vfs.ContentHash(text.Text())}
//	_ = text.SetLine(2, "changed")
//	_ = file.WriteDocument(ctx, text)
//	_ = rev // tools compare expected rev before WriteDocument
//
// Longer guide: docs/vfs.md in the repo root.
package vfs
