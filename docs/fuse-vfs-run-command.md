# Finish the FUSE projection: drop discovery tools; collapse read / write

| Field | Value |
|-------|--------|
| **Author** | Tacklr engineering |
| **Date** | 2026-08-15 |
| **Product** | Tacklr — opinionated Go agent harness SDK |
| **Repo** | `github.com/ryanaldo34/tacklr` |
| **Status** | Complete — Phases 0–5 shipped. Agent catalog is `read`, `write`, `run_command` (plus index tools when Brain is wired). |
| **Depends on** | host-owned `vfs.MountSession`, `VFSProjection`, `run_command`, write-through IR |

---

## Overview

Phases 0–2 of this train are in the tree. A live Runtime turn with a VFS and a FUSE device gets a kernel projection. `run_command` runs `/bin/sh -c` with cwd at that root. Providers persist on write. The harness and the mount table are turn-scoped: Runtime injects a `MountSession` at construct and the turn owner unmounts it.

The **agent file catalog** is collapsed. Discovery (`find_files`, `find_content`), line editors (`read_lines`, `replace_lines`, `replace_text`), and directory tools (`list`, `stat`, `mkdir`, `remove`) are gone. The model sees `read`, `write`, and `run_command`. Live names and grep go through `run_command` → `ls` / `rg` / `fd` on the FUSE tree. Writable FUSE, jail, broker, and a custom shell stay out.

---

## Architecture as built (do not re-litigate)

```text
  Model
    → TurnManager (one turn: tools, plan, checkpoint)
         → MountSession (injected; closed with the turn)
              → providers (local / S3 / brain)     persist immediately
              → FuseMount (read-only kernel tree)  HostDir = cwd for run_command
    → durable.Runtime
         Prompt/Resume.Auth    work-item tokens + bindings (next turn)
         Snapshot.Mounts       secret-free recipes (source ids, no bytes)
         VFSProjection         FuseProjection | DirectProjection
         turn end              Close harness + MountSession + HostDir
```

| Layer | Owner | Rule |
|-------|--------|------|
| `MountSession` | Injector (`OpenTurnVFS`, or embedder) | Fresh `/workspace` tree each turn from `OpenVFS` + bind tokens |
| FUSE | Runtime via `vfs.Projection.Attach` | Attach after construct; skip if `HostDir() != ""` |
| TurnManager | Turn | `NewTurnManager` with `MountSession` set; `Close` parks MCP/vfsindex — **does not** unmount FUSE (workers inherit) |
| IR | Provider | `WriteDocument` / `WriteFile` persist now. There is no session dirty cache (`vfs/cache.go` is gone). `ReadText` is provider plaintext. |
| Tests without `/dev/fuse` | `DirectProjection` | `Available()==true`, `Attach` is a no-op; VFS tools work; `run_command` still needs `HostDir` |

**Production without a FUSE device.** `OpenTurnVFS` returns nil when `!projection.Available()`. There is no MountSession, so no VFS tools and no `run_command`. Tests that need the tree inject `DirectProjection`. Embedders that want the same in-process tree pass a `MountSession` themselves.

**Path identity (shipped).** FUSE root is virtual `/`. The only `Specs()` point is `/workspace`. `FuseMount` rejects multi-segment points. Hosts `At("work", builtins.Local(jail))`. Agent tools take `/workspace/work/note.md`. Host commands take `workspace/work/note.md` relative to `HostDir()`.

**Byte identity (shipped).** Textual FUSE `Read` / `getattr` use `ReadText`. Binaries use `Stat` + `io.ReaderAt`. Writes are write-through, so host `rg` sees the last persist, not a dirty IR buffer.

**`run_command` (shipped).** `tools_vfs.go`. cwd = `HostDir()`; empty HostDir → `ErrFuseNotMounted`. Timeout 60 s, 1 MiB shared stdout+stderr, `Setpgid` + kill group, `exit=N` is success. `PermissionRequired` by default. Injected when `session.VFS != nil`.

---

## Shipped (do not redo)

| Item | Where |
|------|--------|
| Zero FUSE TTLs; single-segment reject; `HostDir`; `FuseAvailable`; `ErrFuseNotMounted` | `vfs/fuse_node.go`, `vfs/errors.go` |
| Kernel identity smoke (skip without device) | `vfs/fuse_test.go` |
| `VFSProjection` / `FuseProjection` / `DirectProjection` | `server/projection.go` |
| FUSE attach; fail-hard on device + mount fail; skip remount if `HostDir` set | `durable.OpenTurnVFS` |
| Turn-scoped mounts; TurnManager Close does not unmount | `openTurnVFS`, `EventStream.Close`, `TurnManager.Close` |
| host `/workspace/work` | `OpenVFS` `At("work", Local(jail))` |
| host skills packs | `OpenSkills` (host-only Tree; not on the agent `/workspace`) |
| `run_command` | `tools_vfs.go` |
| Fuse mount metrics / events | `telemetry` + Registry |
| go-fuse as a direct module | `go.mod` |
| Typed park / permission bags on `RestoreCheckpoint` | `applyCheckpoint` |

---

## Remaining work (shipped)

```text
Phase 3   Drop find_files and find_content. (Also dropped list/stat/mkdir/remove; live names via run_command.)
Phase 4a  Rename read_lines → read. Path-only read returns the first page.
Phase 4b  Fold replace_lines + replace_text + write into one write (mode count).
Phase 5   Docs pass (vfs.md, knowledge.md, README, vfs/doc.go).
Next      Writable FUSE (separate plan) so run_command can mkdir / rm / echo >.
Later     Jail / broker / eBPF; custom shell.
```

Do not start Phase 3 until a FUSE-capable host shows `run_command` → `ls` / `rg` matches `ReadDir` / `ReadText`. Kernel tests already skip without a device. Do not skip that gate by rewriting tests to avoid `run_command`.

### Phase 3 — drop `find_files` and `find_content`

`find_files` is a Go walk. `find_content` is indexed brain chunks with `vfs_path`, not live `rg`. After this phase:

| Tool | Fate |
|------|------|
| `list`, `stat` | Stay. Always injected when a `MountSession` exists. Descriptions prefer `run_command` → `ls` / `stat`. |
| `find_files` | Remove. Live names: `run_command` → `fd` / `find`. |
| `find_content` | Remove. Live grep: `run_command` → `rg`. Indexed recall: brain `search`. |
| `index_file`, `unindex` | Stay. Help text: index, then `search`; live grep is `rg`. |
| `mkdir`, `remove` | Stay until writable FUSE. |
| `run_command` | Stay. |

This is **not** behavior-preserving for index-only hits. Say that in the PR and in `docs/vfs.md`. Do not add aliases.

Rewrite tests that called `find_files` / `find_content` to `run_command` (FUSE host) or brain `search` (index). Keep `list` / `stat` tests. No new `MountSession` APIs.

### Phase 4a — `read_lines` → `read`

Keep `ReadLines` / `ReadText` / `FindBlock` / `lineWindowFromTextDoc`. Change the tool name and args.

```go
type readArgs struct {
    Path    string `json:"path"`
    Rev     string `json:"rev,omitempty"`
    Start   int    `json:"start,omitempty"`
    End     int    `json:"end,omitempty"`
    BlockID string `json:"block_id,omitempty"`
    Outline bool   `json:"outline,omitempty"`
    IR      bool   `json:"ir,omitempty"`
}
```

| Inputs | Outcome |
|--------|---------|
| `path` only | First page: `start=1`, `end=1+MaxLinesPerWindow`, numbered lines + `rev` |
| `path` + `start`/`end` | Same window as today’s `read_lines` |
| `path` + `block_id` | Block span |
| `outline=true` | Outline section |
| `rev` set and mismatch | `vfs.ErrStaleContent` |
| `ir=true` | Extra `media_type` / `encoding` / `line_count`; full `text=` only when no window/block |

`read({path})` must work. That is the behavior change: today’s `read_lines` errors when start/end and block/outline are all unset.

Output stays `path=… rev=…` lines. Port every `read_lines` outcome test by name. Cover path-only and stale `rev`.

### Phase 4b — one `write`

One mutation mode per call. Count **pointer presence**, not first-match.

```go
type writeArgs struct {
    Path           string   `json:"path"`
    Rev            string   `json:"rev,omitempty"`
    Content        *string  `json:"content,omitempty"`
    IRText         *string  `json:"ir_text,omitempty"`
    Start          *int     `json:"start,omitempty"`
    End            *int     `json:"end,omitempty"`
    Lines          []string `json:"lines,omitempty"`
    Body           *string  `json:"body,omitempty"`
    Old            *string  `json:"old,omitempty"`
    New            *string  `json:"new,omitempty"`
    ReplaceAll     bool     `json:"replace_all,omitempty"`
    BlockID        string   `json:"block_id,omitempty"`
    IncludeHeading bool     `json:"include_heading,omitempty"`
}
```

| Mode | Populated when |
|------|----------------|
| full | `content != nil` or `ir_text != nil` |
| substring | `old != nil` |
| block | `block_id != ""` |
| lines | `start != nil` |

If both `content` and `ir_text` are set and they differ → `write: content and ir_text disagree` (still one mode). Empty `old` → `old is required`. Count 0 → `write: no mutation`. Count > 1 → `write: exactly one of content|ir_text, old, block_id, start`.

| Mode | Omitted pointer | Behavior |
|------|-----------------|----------|
| lines | `end == nil` or `*end < *start` | `invalid range` |
| substring | `new == nil` | treat as `""` |

`rev` required when the path exists. Create only via full mode (`content` / `ir_text`). Empty `content` creates or truncates. Reuse `loadMatching`, `stage`, `Classify`, `NewTextDocument`, `ReplaceLines`, `SetText`. Delete `newReplaceLines` / `newReplaceText`. Keep `mkdir` / `remove`.

Port every replace/write outcome plus: empty create, empty overwrite, nil `end`, nil `new`, mixed modes error.

### Phase 5 — docs

Update `docs/vfs.md`, `docs/knowledge.md`, `README.md`, `vfs/doc.go` so they match this file: host-owned session, write-through IR, `run_command`, collapsed tools, no dirty cache, no “FUSE not started”.

---

## Non-goals (still)

| Item | Why |
|------|-----|
| Writable FUSE | Next plan so bash can `mkdir` / `rm` / `echo >` |
| Jail, broker, eBPF, custom shell | After this train |
| Auto `FuseMount` in `NewMountSession` | Host starts it |
| FUSE methods on `TurnManager` | Host owns projection |
| Path rewrite | Kernel tree is the identity |
| Worker / subagent FUSE | `workerOptsFromSpec` still omits `MountSession` |
| Windows / WinFsp | Unix `/bin/sh` + `/dev/fuse` only |
| Restore `vfs/cache.go` | Write-through is the contract |
| Live harness cache across turns | Turn-scoped Close is the contract |

---

## Decisions that still hold

1. FUSE root is virtual `/`. Single-segment points only.
2. Harness tools, `MountInfo`, `Specs`, and checkpoints never print `HostDir`. The child may `pwd` it.
3. `run_command` is cwd-only `/bin/sh -c`. No chroot in this train. Residual escape risk is documented, not tested as a negative.
4. Registry `run_command` is `PermissionRequired: true` by default.
5. `list` / `stat` stay whenever a `MountSession` exists. Do not gate them on `FuseAvailable()`.
6. `write` modes are counted by field presence. IR body field is `ir_text`.
7. Tests assert positive outcomes. Kernel and host-exec tests `Skip` without a FUSE device.
8. stdlib plus existing go-fuse. No new module.

---

## Residual risk (unchanged)

| Risk | Notes |
|------|--------|
| `run_command` is a full host shell as the process user | Permission gate is not a jail |
| `ls /` and `echo x > /tmp/pwn` leave the tree | cwd-only; writable FUSE does not fix this |
| Child `pwd` shows `HostDir` | Do not redact stdout |
| Read-only FUSE does not stop writes **outside** the mount | Next jail plan |
| Production without `/dev/fuse` has no VFS tools | Use `DirectProjection` in tests; embedders pass a session |

---

## Remaining PR plan

Each PR is independently reviewable. Do not combine Phase 3 removal with the `write` collapse.

### PR A — Drop `find_files` and `find_content`

- **Title:** `tools: drop find_files and find_content; keep list/stat`
- **Files:** `tools_vfs.go`, `tools_vfsindex.go`, their tests, brain tool help that names `find_content`, `docs/vfs.md`
- **Depends on:** shipped `run_command` + a FUSE-capable identity check (`vfs/fuse_test.go` or `tools_command_test.go`)
- **Changes:** Delete the two tools and helpers that exist only for them. Keep `list` / `stat` / `index_file` / `unindex`. Point help at `run_command` and brain `search`. Rewrite outcome tests. No aliases.

### PR B — `read_lines` → `read`

- **Title:** `tools: replace read_lines with read`
- **Files:** `tools_vfs.go`, `tools_vfs_test.go`, index/brain help, `vfs/doc.go`
- **Depends on:** none required (can land before or after A)
- **Changes:** Rename; path-only first page; optional `rev` / `ir`. Port tests. No new session methods.

### PR C — one `write`

- **Title:** `tools: single write tool (full, span, old/new, block)`
- **Files:** `tools_vfs.go`, `tools_vfs_test.go`, docs
- **Depends on:** B (descriptions say “pass rev from read”)
- **Changes:** Mode count by pointer presence. Delete replace tools. Keep `mkdir` / `remove`. Port every existing edit outcome.

### PR D — docs pass

- **Title:** `docs: FUSE projection, run_command, read/write surface`
- **Files:** `docs/vfs.md`, `docs/knowledge.md`, `README.md`, `vfs/doc.go`
- **Depends on:** A–C for final names
- **Changes:** Match this file. Remove “FUSE not started”, “dirty IR cache”, “provider jail at /tmp/tacklr”.

### Not in this train

- Writable FUSE nodes
- chroot / namespace jail
- Capability broker, allowlists, eBPF
- Custom agent shell
- Worker VFS / `run_command`

---

## References

- `vfs/session.go` — host-owned path I/O; no dirty cache
- `vfs/fuse_node.go` — `FuseMount`, `Close`, `HostDir`, `FuseAvailable`
- `vfs/fuse_test.go` — kernel identity smoke
- `vfs/document_session.go` — write-through `WriteDocument`
- `server/projection.go` — `VFSProjection`
- `durable/vfs.go` — `OpenTurnVFS`, `CloseTurnVFS`
- `tools_vfs.go` — `read`, `write`, `run_command`
- `tools_vfsindex.go` — `index_file` / `unindex` / `find_content` (until PR A)
- `agent.go` — turn-scoped `Close` (does not close MountSession)
- `agent_construct.go` — `injectBuiltinTools`
- Runtime catalog `OpenVFS` — `At("work", Local(jail))`
- `docs/vfs.md`, `docs/knowledge.md`, `README.md` — update in PR D

---

## Revision summary

2026-08-13 — Initial train (start FUSE, `run_command`, collapse tools).

2026-08-15 — Reoriented after the host-owned session refactor. Phases 0–2 are shipped (`vfs.Projection`, `OpenTurnVFS`, `run_command`, write-through IR, turn-scoped harness). This file now covers only Phase 3–5 and names the next plan (writable FUSE). Production without a FUSE device no longer gets an in-process VFS tree; tests use `DirectProjection`.
