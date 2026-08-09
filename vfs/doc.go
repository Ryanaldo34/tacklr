// Package vfs is Tacklr's virtual filesystem mount layer.
//
// # Surface
//
//   - MountSession — session-owned mount table (attach/detach for the session life)
//   - FS — underlying Unix-style mount namespace (usually behind MountSession)
//   - MountSpec — durable, secret-free mount description (checkpoint-safe)
//   - BackendRegistry + ProviderFactory — process-level profiles and pools
//   - Materialize — rebuild a live FS from specs after restart
//   - LocalFactory / S3Factory — common backends; S3 shares one client per profile
//
// Hosts manage mounts on MountSession for the whole session lifecycle. Specs()
// is the durable view written into session checkpoints; restarts rehydrate via
// Materialize with the same registry profiles (not credentials in JSON). The
// agent harness does not own attach/detach — it only checkpoints Specs.
//
// File operations (Open, Read, Write, …), content IR, and agent tools are out of
// scope here; they will use this mount table without changing how hosts attach
// sources.
//
// # Security
//
// Hosts register factories with real roots and credentials. Agents only see
// virtual paths. MountInfo never exposes host paths, buckets, or profiles.
// MountSpec stores profile ids and non-secret params only.
//
// # Mount rules
//
// Absolute POSIX virtual paths; unique exact points; nested mounts allowed;
// Lookup uses longest-prefix with segment boundaries; ReadOnly is recorded on
// the spec (enforced with file ops).
package vfs
