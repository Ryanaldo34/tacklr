# Virtual filesystem (`vfs`)

Tacklr’s virtual filesystem gives agents one path-based interface over storage backends (local disk, S3, and later Drive/Docs). Hosts own mounts and credentials; agents only see virtual paths like `/work/main.go`.

Package: [`github.com/ryanaldo34/tacklr/vfs`](https://pkg.go.dev/github.com/ryanaldo34/tacklr/vfs).

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
     Provider (local disk / S3 / …)
```

| Layer | Role |
|-------|------|
| **MountSession** | Session-owned tree of virtual mount points + path ops |
| **Provider** | Bytes for one backend (local jail, S3 prefix, …) |
| **Document IR** | Structured view of a file (lines today; blocks/styles later) |
| **Codec** | Bytes ↔ Document by media type |

Mental model: **mounts are the filesystem**; **IR is a checkout of one file**. Edit the checkout, then write it back as bytes. The IR itself is not stored on disk.

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
| `MountSpec` | Durable mount description (point, profile, read-only, params). Checkpoint-safe; no secrets. |
| `MountInfo` | Agent-safe view: point + read-only only |
| `Params` | Backend options (`subpath`, `bucket`, `prefix`, …) |

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
| `Structured` | + `Blocks()` *(reserved; plaintext does not implement this)* |

**Concrete text (what ships today)** — `*TextDocument`:

| Field | Meaning |
|-------|---------|
| `path` | Virtual path only (`/work/main.go`) |
| `mediaType` | e.g. `text/x-go`, `text/plain`, `application/json` |
| `encoding` | `utf-8` |
| `text` | Full body string |
| `lines` | Split of `text` on `\n` (no `\n` in elements; `\r` kept) |

In memory:

```text
*TextDocument
├── path:       "/work/note.txt"
├── mediaType:  "text/plain"
├── encoding:   "utf-8"
├── text:       "a\nB\nc\n"
└── lines:      ["a", "B", "c", ""]
```

**Reserved for Word / Google Docs later**

| Type | Fields |
|------|--------|
| `StyleMeta` | `Kind`, `Level`, `Span`, `Attributes` |
| `Span` | `StartLine`, `EndLine` (1-based half-open) |
| `Block` | `ID`, `Kind`, `Text`, `Style` |

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
| **ReadLines** | Streams; keeps only `[start,end)`; stops after last needed line |
| **WriteFile / WriteDocument** | Provider `PutFile` when available (one S3 `Put`, direct local write); no Open→buffer→Put for full writes |

Tool guidance:

| Need | API |
|------|-----|
| Show / cite a line window | `ReadLines(ctx, path, start, end)` |
| Edit then save | `ReadText` → mutate → `WriteDocument` |
| Raw bytes | `ReadFile` / `WriteFile` |

### Codec routing

| Step | What |
|------|------|
| Read | `ReadFile` once (32 MiB cap) |
| Detect | Extension map, else byte sniff → media type |
| Lookup | `ContentRegistry` media type → `Codec` |
| Fallback | Unregistered but text-like (`text/*`, JSON, YAML, …) → `TextCodec` |
| Decode | `Codec.Decode(path, mediaType, data)` — no second read or re-sniff |
| Else | `ErrNoCodec` (e.g. PNG) |

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
window, _ := ms.ReadLines(ctx, "/work/note.txt", 1, 3) // ["a","b"]

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

- Agent builtins (`read_file` with `start_line` / `end_line`) — close over `session.VFS` later
- Markdown / Word / Google Docs codecs and filled `StyleMeta` / `Block` trees
- Structured or style-preserving write-back for rich docs
- Streaming multi-GB line indexes

---

## Design note: “LLVM of filesystems”

| LLVM idea | VFS analogue |
|-----------|--------------|
| IR independent of backend | `Document` independent of local / S3 / Drive |
| Frontends lower to IR | Codecs decode bytes → Document |
| Target-specific codegen | Codecs encode back to native formats (text today) |
| Stable contract | `Document` / `Textual` / `Structured` interfaces |

Plaintext is the first frontend. Rich cloud documents are additional frontends targeting the same IR.
