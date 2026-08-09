// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - MountSession — mounts, path I/O, ReadText / WriteDocument / Sync, ReadLines
//   - BackendRegistry + LocalFactory / S3Factory + AWSS3 — process profiles and pools
//   - MountSpec / MountInfo — durable and agent-safe mount descriptions
//   - Provider / ProviderFactory / S3API — custom backends
//   - File, FileInfo, DirEntry — I/O types
//   - Document / Textual / TextDocument — content IR
//   - ContentRegistry + Codec + TextCodec — optional custom decode
//   - DetectMediaType, size-cap constants, sentinel errors
//
// Cache, invalidation, and dirty tracking are internal. Hosts use Sync / SyncAll
// to flush; harness checkpoint calls SyncAll before saving mount Specs.
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
//	// Window only (stream; keep returned lines) — prefer for tool read_file
//	part, err := ms.ReadLines(ctx, "/work/main.go", 1, 51)
//
//	// Full IR for edit; WriteDocument stages dirty cache until Sync
//	text, err := ms.ReadText(ctx, "/work/main.go")
//	_ = text.SetLine(2, "changed")
//	_ = ms.WriteDocument(ctx, text)
//	_ = ms.SyncAll(ctx) // or harness checkpoint
//
// Session content cache: clone-on-read, write-back IR, Sync flushes to backend.
// Checkpoint stores mount Specs only (not file bytes).
//
// Longer guide: docs/vfs.md in the repo root.
package vfs
