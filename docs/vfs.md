# Virtual filesystem (`vfs`)

Tacklr’s virtual filesystem gives agents one path-based interface over storage backends (local disk, S3, **brain Engrams**, and later Drive/Docs). Hosts own mounts and credentials; agents only see virtual paths like `/work/main.go` or `/engram/deal/acme.md`.

Package: [`github.com/ryanaldo34/tacklr/vfs`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/vfs).

Knowledge objects, search, and the graph are documented in **[docs/knowledge.md](knowledge.md)** (canonical). This file is the VFS surface: mounts, IR, write-back, and index *policy*.

## Big picture

```text
  Host mounts backends once
           │
           ▼
  MountSession  ── virtual paths like /work/main.go
           │
     ┌─────┴─────┐
     │           │
  bytes I/O    document I/O (content IR)
  ReadFile     OpenDocument / ReadText
  WriteFile    WriteDocument
     │           │
     │           ▼
     │      TextDocument (in memory)
     │        lines + text
     │           │
     └───────────┘
           │
           ▼
     Provider (local disk / S3 / brain Engrams)
```

| Layer | Role |
|-------|------|
| **MountSession** | Session-owned tree of virtual mount points + path ops |
| **Provider** | Bytes for one backend (local jail, S3 prefix, …) |
| **Document IR** | Agent-facing view of a file (lines + optional structured blocks) |
| **Codec** | Bytes ↔ Document by media type |

Mental model: **mounts are the filesystem**; **IR is a checkout of one file**. The session holds an optional **page cache** of textual IR with write-back; the **backend** is source of truth after `Sync`.

### Session cache and durability

| Layer | Role |
|-------|------|
| Content cache | Internal session map of textual IR (clean/dirty); not part of the public API |
| `WriteDocument` | Stages dirty IR (**no** backend Put yet) |
| `Sync` / `SyncAll` | Flushes dirty paths to the backend (only flush knobs hosts need) |
| Harness checkpoint | `SyncAll`, then saves **mount Specs only** |
| Crash before Sync | Dirty edits lost; last successful Sync wins |

```text
ReadText → miss → Get → cache clean → clone
WriteDocument → dirty (backend unchanged)
ReadText → dirty hit → clone (no Get)
SyncAll → Put → clean
checkpoint → SyncAll → save Specs
```

`WriteFile` is write-through and drops any cached IR for that path.

---

## Mounts

Hosts register factories on a process-scoped `BackendRegistry`, then attach mounts on a `MountSession`.

```go
reg := vfs.NewBackendRegistry()
_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: "/var/agent/scratch"})
// S3: reg.Register(vfs.S3Factory{ID: "docs", Client: vfs.AWSS3{Client: s3c}, DefaultBucket: "my-bucket"})

ms := vfs.NewMountSession("sess-1", reg)
_ = ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"})
```

| Type | Meaning |
|------|---------|
| `MountSpec` | Durable mount description (point, profile, read-only, params, **indexPolicy**). Checkpoint-safe; no secrets. |
| `MountInfo` | Agent-safe view: point + read-only only |
| `Params` | Backend options (`subpath`, `bucket`, `prefix`, …) |
| `IndexPolicy` | Optional string: `none` \| `selective` \| `prefix` \| `watch` (empty → selective when the index bridge is on) |

`MountSession.SpecAt` returns the full durable `MountSpec` for a virtual path (for policy and host tooling).

Auth and host roots live on factories, not on mounts or checkpoints.

Raw path I/O (absolute virtual paths only):

```text
Stat, Open, ReadFile, WriteFile, ReadDir, Remove, MkdirAll
```

Read-only mounts return `ErrReadOnly` on writes. Local paths are jailed under the provider root. S3 uses key prefixes and delimiter “directories.”

---

## Content IR

IR is the **agent-facing view of file content**. Codecs turn raw bytes into a `Document`. Agents never see host paths or bucket keys.

```text
virtual path → Provider (bytes) → Codec → Document IR
```

### Schemas

**Base**

| Type | Methods / fields |
|------|------------------|
| `Document` | `Path()`, `MediaType()` |
| `Textual` | + `Encoding()`, `Text()`, `LineCount()`, `Line(n)`, `Lines(start, end)` |
| `Structured` | + `Blocks()` — projected outline; empty when the media type has no projector |

**Concrete text (what ships today)** — `*TextDocument` implements `Document`, `Textual`, and `Structured`:

| Field / method | Meaning |
|----------------|---------|
| `path` | Virtual path only (`/work/main.go`) |
| `mediaType` | e.g. `text/x-go`, `text/plain`, `text/markdown`, `application/json` |
| `encoding` | `utf-8` |
| `text` | Full body string |
| line index | Line-start offsets into `text` (not a second stored body) |
| `Blocks()` | Structure projected by media type (Markdown headings today; nil/empty otherwise) |

In memory:

```text
*TextDocument
├── path:       "/work/note.txt"
├── mediaType:  "text/plain"
├── encoding:   "utf-8"
├── text:       "a\nB\nc\n"
└── starts:     [0, 2, 4, 6]   // byte offsets of each line
```

**Block schema** (shared for Markdown now; Word / Google Docs later):

| Type | Fields |
|------|--------|
| `StyleMeta` | `Kind`, `Level`, `Span`, `Attributes` |
| `Span` | `StartLine`, `EndLine` (1-based half-open) |
| `Block` | `ID`, `Kind`, `Text`, `Style` |
| Helpers | `FindBlock`, `BlockReplaceSpan` (media-agnostic) |

### How IR is “stored”

**It is not persisted as IR.**

1. Source of truth = **raw file** on the mount (local dir, S3 object, …).
2. `OpenDocument` / `ReadText` **read bytes** (cap 32 MiB), sniff media type, run a codec.
3. IR lives **in memory** for that call (`*TextDocument`).
4. `TextDocument` keeps **one** UTF-8 body plus a **line-start index** (`[]int`). Line views slice into the body until an edit rebuilds it — no second full copy of the text as `[]string`.

Raw `ReadFile` / `WriteFile` stay byte-only and do not build IR.

### I/O and memory notes

| Concern | Behavior |
|---------|----------|
| Max file size | 32 MiB (`MaxReadFileBytes`) on full read/write |
| Max line size | 1 MiB (`MaxLineBytes`) when streaming lines |
| Full read buffer | When `File.Stat` reports size: one allocation + `ReadFull`; oversize rejected before body |
| IR footprint | ~1× content + line-offset index; decode reuses the read buffer as the string body |
| **ReadLines** | Returns `LineWindow` (lines + EOF + NextStart); streams large files; no full-object 32 MiB reject |
| **MaxLineScanBytes** | Per-call stream scan budget (64 MiB), separate from full IR cap |
| **MaxLinesPerWindow** | Max lines returned per `ReadLines` call (500) |
| **WriteDocument** | Write-back cache; flush with `Sync` / `SyncAll` |
| **WriteFile** | Write-through backend; drop IR cache for path |
| **Dirty overlay** | `Stat` / `ReadDir` / `Open` / `ReadFile` / `Remove` see write-back creates before Sync |
| **AfterPersist** | Optional host hook after successful backend write (`WriteFile` / `Sync`) |
| **ContentRev** | Session-visible content identity (`Path` + SHA-256 hex of body); `LineWindow.Rev` when known |

Tool guidance:

| Need | API |
|------|-----|
| Line window / page large file | `ReadLines` → page with `NextStart` until `EOF` (see `Rev`) |
| Stable edit token | `ContentRev` / `ContentHash` — tools compare expected rev before write |
| Find in mounts (indexed) | Optional `vfsindex` → brain `search` / `find_exact` on Chunks; then `ReadLines` around hit |
| Edit (SDK) | `ReadText` → mutate → `WriteDocument` → checkpoint/`SyncAll` |
| Edit (agent) | Harness tools (`read_lines`, `replace_lines`, `replace_text`, `write`, …) wrap the above with rev checks |
| Flush now | `Sync` / `SyncAll` |
| Raw bytes | `ReadFile` / `WriteFile` |

`vfs` does not implement content grep. Live OS-tool search (real `rg`/`grep`) is planned via a future FUSE (or materialize) host projection. Indexed recall of mount content is the optional `vfsindex` bridge when a brain engine is wired.

### Codec routing

| Step | What |
|------|------|
| Read | `ReadFile` once (32 MiB cap) |
| Detect | **Provider** sets `FileInfo.MediaType` on Stat (S3 Content-Type or key/name; local extension + peek). Session does not sniff. |
| Lookup | `ContentRegistry` media type → `Codec` |
| Fallback | Unregistered but text-like (`text/*`, JSON, YAML, …) → `TextCodec` |
| Decode | `Codec.Decode(path, mediaType, data)` — no second read or re-sniff |
| Else | `ErrNoCodec` (e.g. PNG) |

`DetectMediaType` is a helper **providers** call when filling `MediaType`. Empty / missing type is treated as `application/octet-stream` (no IR).

FUSE: hosts call `MountSession.FuseMount(dir)` for a read-only kernel tree. File `Read`/`getattr` use `ReadText` (dirty plaintext). Binary files use `Stat.Size` and `io.ReaderAt` when the handle supports it. `session.Mount` attaches a provider; `FuseMount` is the host kernel mount. `Close` unmounts.

`TextCodec` requires valid UTF-8 and builds a `TextDocument` labeled with the caller’s media type.

### Line rules

- Lines are **1-based**.
- `Lines(start, end)` is **half-open**: start inclusive, end exclusive. `Lines(1, 3)` → lines 1 and 2.
- Empty file → `LineCount == 0`.
- Trailing `\n` → last line is empty (same as `strings.Split`).
- `\r` before `\n` stays on the line text.

---

## Write-back

Edit the IR in memory, then persist:

```text
ReadText → mutate IR → WriteDocument → WriteFile (raw UTF-8 bytes)
```

### Mutate (`*TextDocument`)

| Method | What |
|--------|------|
| `SetText(s)` | Replace whole body; rebuild lines |
| `SetLine(n, line)` | Replace one line (1-based); `line` must not contain `\n` |
| `ReplaceLines(start, end, lines)` | Half-open splice: insert / delete / replace |
| `Bytes()` | UTF-8 body for write |

Examples starting from `"a\nb\nc\n"` → lines `[a, b, c, ""]`:

| Call | Resulting lines |
|------|-----------------|
| `SetLine(2, "B")` | `[a, B, c, ""]` |
| `ReplaceLines(3, 4, []string{"C","D"})` | `[a, B, C, D, ""]` |
| `ReplaceLines(2, 4, nil)` | delete lines 2–3 |
| `ReplaceLines(1, 1, []string{"x"})` | insert `x` at start (or into empty file) |
| `SetText("only\n")` | full replace |

### Persist

```go
text, _ := ms.ReadText(ctx, "/work/note.txt")
_ = text.SetLine(2, "changed")
_ = text.ReplaceLines(3, 4, []string{"C", "D"})
_ = ms.WriteDocument(ctx, text)
```

- Writes to `doc.Path()` via `WriteFile`
- Textual documents only (`ErrNotTextual` otherwise)
- Respects read-only mounts (`ErrReadOnly`)
- Same 32 MiB size cap as reads
- A “line” string that contains `\n` → `ErrInvalidLine`

Plaintext write-back is UTF-8 of `Text()` — no separate encode step.

---

## Full lifecycle example

```go
ctx := context.Background()

// --- Mount ---
reg := vfs.NewBackendRegistry()
_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: "/var/agent/scratch"})
ms := vfs.NewMountSession("sess-1", reg)
_ = ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"})

// Optional seed via raw bytes
_ = ms.WriteFile(ctx, "/work/note.txt", []byte("a\nb\nc\n"))

// --- Read a window only (tools) ---
win, _ := ms.ReadLines(ctx, "/work/note.txt", 1, 3) // win.Lines == ["a","b"]; check win.EOF

// --- Full IR for edit ---
text, err := ms.ReadText(ctx, "/work/note.txt")
// text.Line(1) == "a"
// text.LineCount() == 4   // trailing \n → last empty line

// --- Edit (memory only) ---
_ = text.SetLine(2, "B")
_ = text.ReplaceLines(3, 4, []string{"C", "D"})
// body now: "a\nB\nC\nD\n"

// --- Write-back (streamed PutFile) ---
_ = ms.WriteDocument(ctx, text)

// --- Verify ---
raw, _ := ms.ReadFile(ctx, "/work/note.txt")
// raw == []byte("a\nB\nC\nD\n")
```

Lifecycle as a diagram:

```text
1. Mount     BackendRegistry + MountSession.Mount
             /work → local folder (or S3 prefix)

2. Read      ReadText / OpenDocument
             bytes → DetectMediaType → TextCodec → *TextDocument

3. Edit      SetLine / ReplaceLines / SetText
             (memory only)

4. Write     WriteDocument
             Text() as UTF-8 → WriteFile(path) → provider

5. Next open reads disk again → fresh IR
```

### Same paths on S3

```text
Mount:  /data  →  S3Factory (bucket + prefix)
Read:   ReadText("/data/app.go")   // GetObject → TextDocument
Edit:   SetLine / ReplaceLines
Write:  WriteDocument              // PutObject with Text() bytes
```

The agent always uses `/data/app.go`. It never sees the bucket name.

---

## Structured view (blocks)

Some media types project a **block outline** over the same textual body (shared schema for Markdown now; Word/Docs later).

| Type | Role |
|------|------|
| `Structured` | `Blocks() []Block` |
| `Block` | `ID`, `Kind`, `Text`, `Style` (`Level`, `Span`, attributes) |
| `Span` | 1-based half-open line range into the text body |

**Markdown** (`text/markdown`): ATX headings (`#`…`######`) become `heading` blocks; content before the first heading is `preamble`. Headings inside fenced code are ignored. Block ids are hierarchical slugs (e.g. `api/errors`). Structure is **recomputed** from the current text on each `Blocks()` call—not a second stored body.

Parent heading spans **contain** child sections (section replace includes nested content). Default replace under a heading is **body only** (skips the heading line) unless tools pass `include_heading`.

Hosts:

```go
doc, _ := ms.ReadText(ctx, "/work/README.md")
for _, b := range doc.Blocks() {
    // b.ID, b.Style.Span, b.Text
}
start, end, _ := vfs.BlockReplaceSpan(b, false) // body only under a heading
_ = doc.ReplaceLines(start, end, []string{"new body"})
```

Agent tools use the same ideas with generic names: `block_id`, optional `outline` on read, `block_id` + `body` on replace. Citations: `path#block_id`. Projectors stay internal to `vfs` by media type — hosts do not call Markdown outline helpers.

## Two jobs (do not mix)

Full model, search, and graph: **[docs/knowledge.md](knowledge.md)**.

| Job | Store | Sync |
|-----|--------|------|
| **Artifact file** (`/work`, S3, …) | Local/S3 bytes | **IndexPath** (hash, Document+Chunk) |
| **Engram** | Engine object | **brain.Provider** read/write (Markdown + YAML) |

`vfs` never imports `brain`. Brain implements `vfs.Provider`. Package
[`vfsindex`](../vfsindex) indexes **non-brain** mounts only.

### Engrams as files (`brain.Provider`)

Host-defined `KindSpec`s are domains (Deal, Person are examples, not product types).
Kind names must be path-safe: no `/` or `..`. Only **parent** kinds become directories;
parts/chunks are never files. See [knowledge.md](knowledge.md) for the file format,
write-through sequence, and `save_*` behavior.

**Factory params** (`MountSpec.Profile == "brain"`):

| Param | Meaning |
|-------|---------|
| `mode` | `prefix` (default) or `roots` |
| `kind` | Required for `roots` — one kind per mount (`/deal/acme.md`) |
| `kinds` | Comma allow-list. Empty catalog: pass `kinds=` or list kinds that already have objects |

```text
# default harness mount when Brain + VFS + namespace and no host brain mount
Mount { Point: "/engram", Profile: "brain", IndexPolicy: none, Params: { mode: prefix } }
→ /engram/deal/acme.md

# host roots layout
Mount { Point: "/deal", Profile: "brain", Params: { mode: roots, kind: Deal } }
→ /deal/acme.md
```

File format is **Markdown + YAML front matter** (`id`, `domain`/`kind`, `slug`, `title`,
then kind fields). Body → `Object.Content`. A `---` line inside the YAML block ends
front matter (standard limitation). `vfs_path` is stored on the object, not in the file.

First save without `id` allocates a UUID and rewrites front matter on the next read.
**Rename** is not a move: delete + create + re-link.

`save_*` writes the Engram file on the Provider when one is mounted; otherwise it
falls back to `Engine.Put`. Scratch `/memory` is **not** attached when a brain
Provider mount exists (deprecated for discoveries).

**Shape A graph tools:** `link` / `unlink` / `expand` / `find_links` speak **paths**.
There are no `.links` directories; `list`/`ls` never lists edges. Artifact paths
must be indexed (`index_file` / prefix policy) before they can be linked.

### Index policy

| Policy | Pipeline triggers |
|--------|-------------------|
| `none` | No auto jobs; `index_file` **errors** |
| `selective` | Only `index_file` / host IndexPath; after a successful `index_file`, AfterPersist reindexes that path (track set) |
| `prefix` | IndexPrefix at bridge start + AfterPersist under the mount |
| `watch` | Same auto triggers as `prefix` |

Empty → **selective**.

### Single fan-in

```text
  index_file ──┐
  IndexPrefix ─┼──► IndexPath ──► brain Document+Chunks (hash skip)
  AfterPersist ┘       │  (never walks Profile=="brain")
```

Brain-profile mounts set `IndexPolicy=none` automatically and are never re-indexed
as Document/Chunk artifacts. Engram writes go through the Provider (`Put`), not IndexPath.

### Decoupling

| Package | May know |
|---------|----------|
| `vfs` | Specs (incl. IndexPolicy string), `AfterPersist` hook only |
| `brain` | Objects/props only; no VFS |
| `vfsindex` | Both; owns `IndexPath` / `IndexPrefix` / schedulers / policy helpers |
| harness (`tacklr`) | BrainFactory + `/engram` default, skip-index on brain profile, tools |

### Host wiring

```go
idx, _ := vfsindex.NewMountIndexer(ms, eng, scope)
_ = eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...) // non-empty kind catalogs only
_ = idx.IndexPrefix(ctx, "/work", vfsindex.IndexOpts{})

// Prefer AsyncScheduler so writes are not blocked on re-chunk
sched := vfsindex.NewAsyncScheduler(idx)
prev := ms.GetAfterPersist()
ms.SetAfterPersist(func(ctx context.Context, path string) error {
    if prev != nil {
        _ = prev(ctx, path)
    }
    // Gate with vfsindex.AutoIndex(spec.IndexPolicy) or selective track
    return sched.Notify(ctx, path, vfsindex.ReasonSync)
})
defer sched.Close()
```

### Agent tools (default on when prerequisites hold)

When the harness has **Brain + MountSession + search namespace**, it owns a
`MountIndexer` + `AsyncScheduler`, composes policy-gated `AfterPersist`, and registers:

| Tool | Role |
|------|------|
| `index_file` | Selective ingest of key virtual **files** (max 8); errors under `none` |
| `unindex` | Soft-delete the brain mirror; drops selective track |
| `find_content` | Index-backed search requiring `vfs_path` (temporary until `run_command`) |
| `find_files` | Bounded live VFS walk by name/glob (VFS-only; temporary) |
| `save_*` | Write the Engram file on the brain Provider (or `Engine.Put` if no brain mount) |
| `link` / `expand` / `find_links` | Path-native graph (G1): prefer virtual paths; surface neighbor `vfs_path` |

Omit Brain, VFS, or namespace to opt out (no tools, no harness indexer, no async hook).

### Session-visible body vs AfterPersist

`IndexPath` uses `MountSession.ReadText` / `Open`, which honor the **dirty IR cache**.
So `index_file` after `write` / `replace_*` indexes the session-visible body **before
Sync**. `AfterPersist` (fired by `WriteFile` / `Sync`) drives background reindex when
policy (or selective track) allows. Write success is never blocked by reindex failures.

Markdown files are chunked by **heading/preamble blocks** (`block_id` and `heading_path` properties) when `Blocks()` is non-empty; other text still uses line windows.

Indexed content is queried with `find_content`, `search` / `find_exact` (prefer hits with `vfs_path`).
Live OS-tool search over mounts is deferred to a future FUSE + `run_command` path.

---

## Stable content refs (agent tools, no bash)

`vfs` exposes a **small** identity helper; **edit policy lives in harness tools**.

| Layer | Responsibility |
|-------|----------------|
| `vfs.ContentHash` / `ContentRev` / `LineWindow.Rev` | Identity of session-visible body |
| `ReadText`, `ReplaceLines`, `WriteDocument` | Low-level IR mutate + write-back |
| Harness `read_lines`, `replace_lines`, `replace_text`, `write`, `list`, `stat`, `mkdir`, `remove` | Require `rev` on edits, reject stale, format numbered lines |

```text
read_lines   → path + rev + numbered window
replace_*    → must pass rev; on mismatch ErrStaleContent → re-read
checkpoint   → SyncAll flushes dirty IR (existing)
```

No FUSE and no shell are required for this path.

---

## Errors

| Situation | Sentinel |
|-----------|----------|
| Path not under a mount | `ErrNotMounted` |
| Write on read-only mount | `ErrReadOnly` |
| Missing file | `ErrNotExist` |
| No codec for media type | `ErrNoCodec` |
| Not a textual document | `ErrNotTextual` |
| Bad line index / range | `ErrLineOutOfRange` |
| Line string contains `\n` | `ErrInvalidLine` |
| Single line exceeds `MaxLineBytes` | `ErrLineTooLong` |
| File over `MaxReadFileBytes` | `ErrTooLarge` (wrapped with max size) |
| Invalid UTF-8 on text decode / stream | `ErrInvalidUTF8` |

---

## Not in this package (yet)

- Word / Google Docs codecs that fill the same `Block` / `StyleMeta` schema from native formats
- Structured or style-preserving write-back for rich (non-text) docs
- Streaming multi-GB line indexes
- Setext headings / full CommonMark AST (Markdown projector is ATX + fence-aware only)

---

## Design note: “LLVM of filesystems”

| LLVM idea | VFS analogue |
|-----------|--------------|
| IR independent of backend | `Document` independent of local / S3 / Drive |
| Frontends lower to IR | Codecs decode bytes → Document |
| Target-specific codegen | Codecs encode back to native formats (text today) |
| Stable contract | `Document` / `Textual` / `Structured` interfaces |

Plaintext is the first frontend. Rich cloud documents are additional frontends targeting the same IR.
