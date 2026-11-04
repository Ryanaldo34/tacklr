// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - Tree / At / Union / OpenVFS — host builds one /workspace tree per turn
//     (agent-visible). Union merges Opens at one alias. Package workspace
//     gives the agent its tools, skills and overflow on that tree.
//   - MountSession — path I/O, ReadText / WriteDocument, ReadLines
//   - ContentRev / ContentHash — session-visible content identity (for tools)
//   - SessionAuth + TokenHolder + Binding — session-scoped user-owned credentials (never on MountSpec)
//   - GitRepo + GitToken / GitSSHKey — git clone onto worker disk; the agent runs git itself
//   - MountSpec — durable mount description (checkpoint-safe; one record per /workspace/<name>)
//   - WorkspacePoint — /workspace (the only top-level mount)
//   - Provider / Open / S3API / DriveAPI / GraphAPI — custom backends (Blob uses S3API).
//     Hosts construct default backends from this package (Local, S3, Blob,
//     Drive, Graph, Git). NewGoogleDrive, NewGoogleDocs, NewGoogleSheets and
//     NewMicrosoftGraph call the Google and Graph REST APIs over net/http.
//     The S3 and Azure Blob SDK adapters are in packages vfs/awss3 and
//     vfs/azureblob, so this package does not link those SDKs.
//   - File, FileInfo, DirEntry — I/O types (File is Close+Stat; io.Reader / io.ReaderAt / io.Writer via comma-ok).
//     FileInfo.Version changes when the content changes (ETag, md5Checksum, cTag, or local size+mtime+inode).
//     Read handles stream on every backend; remote ReadAt uses range requests with read-ahead.
//   - Document / Textual / Structured / TextDocument — content IR
//   - Block / StyleMeta / Span / FindBlock / BlockReplaceSpan — structured view
//   - ContentRegistry + Codec + TextCodec + IdentityCodec — each MountSession's
//     codecs (MountSession.Codecs): TextCodec, the mounted backends' own types
//     (Drive's Google Docs and Sheets), and what the host adds with
//     OpenVFS.WithCodecs (vfs/adapters has Word and Excel). Nothing registers
//     a codec on import; a second codec for a type returns ErrAlreadyRegistered
//   - DetectMediaType, size-cap constants, sentinel errors (including ErrAuthExpired, ErrAmbiguous, ErrPermission, ErrAlreadyRegistered)
//
// Providers own IR translation and persist immediately on WriteDocument.
// MountSession routes; it does not encode or hold a dirty document cache.
//
// Optimistic edit policy lives in harness tools (read, write) that wrap
// ReadText / TextDocument / WriteDocument. ContentRev is the stable token
// those tools pass — not a large MountSession surface.
//
// This package never imports brain; knowledge is not a mount. Optional
// artifact indexing lives in package vfsindex (imports both). Package
// vfs/fuse shows a MountSession on the host kernel; host tools there read
// ReadText (provider IR plaintext). This package does not ship a grep tool.
//
// Hosts should not need anything else. Host paths and bucket details stay
// inside providers. MountSession only keeps the mount map and routes paths.
//
// # Path I/O
//
// Absolute virtual paths only, via MountSession:
//
//	Stat, Open, ReadFile, WriteFile, ReadDir, Remove, MkdirAll
//	File is Close + Stat. Read / ReadAt / Write are optional (comma-ok).
//	The only mount point is /workspace.
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
