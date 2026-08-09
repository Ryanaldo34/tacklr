// Package vfs is Tacklr's virtual filesystem: session mounts, path I/O, and content IR.
//
// # Public surface (hosts)
//
//   - MountSession — mounts + path I/O + OpenDocument / ReadLines / WriteDocument
//   - BackendRegistry + LocalFactory / S3Factory + AWSS3 — process profiles and pools
//   - MountSpec / MountInfo — durable and agent-safe mount descriptions
//   - Provider / ProviderFactory / S3API — implement custom backends
//   - File, FileInfo, DirEntry — I/O result types
//   - Document / Textual / TextDocument — content IR (LLVM of filesystems)
//   - ContentRegistry + Codec + TextCodec — media-type decode
//   - DetectMediaType — media-type hint for codec routing
//   - MaxReadFileBytes / MaxLineBytes — size caps
//   - Sentinel errors (ErrNotMounted, ErrReadOnly, ErrNoCodec, ErrLineTooLong, …)
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
//	// Full IR for edit + write-back
//	text, err := ms.ReadText(ctx, "/work/main.go")
//	_ = text.SetLine(2, "changed")
//	_ = ms.WriteDocument(ctx, text)
//
// OpenDocument is ReadFile + DetectMediaType + Codec.Decode (single read).
// WriteFile/WriteDocument use provider PutFile when available (one S3 Put).
// StyleMeta / Structured are reserved for rich docs and unused by plaintext.
//
// Longer guide: docs/vfs.md in the repo root.
package vfs
