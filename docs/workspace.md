# Workspace

The workspace is the agent's file system: one tree under `/workspace`, built from the sources you choose. The agent sees only virtual paths, such as `/workspace/work/main.go`. It never sees host paths, bucket names, other sessions, or credentials.

The workspace is in package `workspace`, over the virtual file system in package `vfs`. It runs on Linux workers only; the harness core does not import it.

## Build a workspace

Set `worker.Config.Workspace`. Each `vfs.At` mounts one source as a directory under `/workspace`:

```go
cfg := worker.Config{
	Agent: agent, Store: store, Secrets: secrets,
	Workspace: workspace.Config{
		Open: vfs.Tree(
			vfs.At("work", vfs.SessionLocal("/srv/tacklr/work")),
			vfs.At("docs", vfs.S3(awss3.API{Client: s3Client}, "acme-docs")).ReadOnly(),
		).WithCodecs(adapters.Codecs()...), // read and write Word and Excel files
		OverflowPath: "/workspace/work/.overflow",
	},
	FuseRoot: "/var/lib/tacklr/fuse", // optional: adds run_command
}
```

| `workspace.Config` field | Effect |
|---|---|
| `Open` | Builds the session's tree for each turn. Usually `vfs.Tree(...)`. |
| `SkillsPath` | The skills directory. Empty is `/workspace/skills`. |
| `OverflowPath` | A writable directory for the full text of tool results that the context window cut. |
| `IndexReaders` | The source's sharing list for each indexed file, so search returns a file only to users who can open it. |

A member's options: `.ReadOnly()` refuses writes, and `.NotIndexed()` keeps its files out of the knowledge store.

## Sources

| Source | Package | Notes |
|---|---|---|
| `vfs.Local(dir)` | `vfs` | A directory that every session shares. Paths cannot leave it. |
| `vfs.SessionLocal(dir)` | `vfs` | A directory for each session (and each specialist) under `dir`. |
| `vfs.S3(api, bucket)` | `vfs/awss3` for `api` | An S3 bucket, or a prefix of it. |
| `vfs.Blob(api, container)` | `vfs/azureblob` for `api` | An Azure Blob container. |
| `vfs.Drive(api, holder)` | `vfs` | A user's Google Drive, with native Docs and Sheets. |
| `vfs.Graph(api, holder, account)` | `vfs` | A user's OneDrive or a SharePoint library. |
| `vfs.Git(repo)` | `vfs` | A clone of a repository on the worker's disk. |
| `vfs.Union(opens...)` | `vfs` | A read-only merge of several sources, for example skill packs. |

The S3 and Azure Blob adapters are in their own packages, so a worker links a cloud SDK only when it uses it. The Google and Microsoft adapters use the REST APIs over `net/http` and link no vendor SDK.

### Git

`vfs.Git` clones one remote into `Dir/<session>/<member>` on the worker's disk and mounts the working tree. The first open in a session clones; later opens on the same worker reuse the checkout.

```go
api := vfs.GitRepo{
	URL:   "https://github.com/acme/api.git",
	Auth:  vfs.GitToken("x-access-token", tokenFromVault), // a func(ctx) (string, error); or vfs.GitSSHKey(key, knownHosts)
	Ref:   "main",                                // empty: the remote's HEAD
	Depth: 1,                                     // 0: full history
	Dir:   "/var/lib/tacklr/git",
}
```

- The agent runs git itself (`status`, `commit`, `push`) with `run_command`, so the worker needs `git`, and `ssh` for SSH remotes.
- The clone credential is used once and is not written to `.git/config`. To push, the shell needs credentials of its own, and the agent can read anything the shell can use.
- The checkout is on one worker's disk. Work that is not pushed is lost when the session moves to another worker.

## Users' own folders

Google Drive and Microsoft 365 folders belong to the user, so the client sends the credentials. The client runs OAuth and sends a short-lived access token for each session. The server is not an OAuth client.

Over ACP, the client uses these extension methods:

| Method | Does |
|---|---|
| `_tacklr/vfs/bind` | Adds a folder: `{sessionId, backends: [{provider, point, auth, params}]}`. `provider` is `gdrive` or `msgraph`, and `params.name` is the directory name under `/workspace`. |
| `_tacklr/vfs/refresh` | Sends a new access token. |
| `_tacklr/vfs/unbind` | Removes a folder by its name. |
| `_tacklr/vfs/token` | Sent by the agent: asks the client for a new token after a 401, when the client said it can refresh. |

A change applies from the session's next prompt. A bound folder is read-only unless the client sets `"readOnly": false`.

In your `workspace.Config.Open`, build the member from the turn's binding:

```go
func open(ctx context.Context, id string, req vfs.Request) (*vfs.MountSession, error) {
	members := []vfs.Member{vfs.At("work", vfs.SessionLocal("/srv/tacklr/work"))}
	if b, ok := vfs.BindingByName(req.Bindings, "drive"); ok && b.Auth.Token != "" {
		h := vfs.NewTokenHolder(b.Auth)
		members = append(members, vfs.At("drive", vfs.Drive(vfs.NewGoogleDrive(h, "", nil), h)))
	}
	return vfs.Tree(members...)(ctx, id, req)
}
```

The token goes to `session.SecretStorage`, sealed. It is never in the checkpoint, in Temporal history, or in the ACP wire store. See [Sessions in production](session.md#credentials).

| Provider | Read-only scopes | Writable scopes | Parameters |
|---|---|---|---|
| Google Drive | `drive.readonly` | `drive`, `documents`, `spreadsheets` | `folderId` (empty: My Drive) |
| Microsoft Graph | `Files.Read` | `Files.ReadWrite` | `account` (`organization` or `personal`), `driveId`, `siteId`, `itemId` |

An `organization` account needs a `siteId` or a `driveId`, and a SharePoint library also needs `Sites.Read.All` or `Sites.ReadWrite.All`.

## The agent's file tools

| Tool | Does |
|---|---|
| `read` | Reads a page of numbered lines, a line range, a heading's section, or a sheet's cells. A Google Doc or Word file reads as HTML, one block on each line. A spreadsheet reads as an outline first. |
| `write` | Creates or edits a text or code file: the whole content, a replacement of one string, or a line range. |
| `write_document` | Creates or edits a Google Doc or a Word file. |
| `write_spreadsheet` | Creates a spreadsheet, or edits one cell and its format. |
| `run_command` | Runs a shell command in the session's workspace. Needs `FuseRoot`. |

Writes ask the user for permission unless the tool is in `AgentOptions.AllowTools`, and they run only after the agent has a plan. A write checks the file's revision from the last `read`, so a stale edit fails, and the agent reads again.

A write goes to the source at once. Nothing is cached between turns.

## Documents and spreadsheets

Codecs turn a file's bytes into a document that the tools can read and edit by line, by block, or by cell:

- Text, Markdown, JSON, YAML, and code use the text codec. Markdown also has heading blocks.
- Google Docs and Sheets use the Docs and Sheets APIs on a Drive source.
- Word (`.docx`) and Excel (`.xlsx`) need `WithCodecs(adapters.Codecs()...)` on the tree.

Other types, such as images, can be listed and read as bytes but not edited by the tools.

## The shell

With `worker.Config.FuseRoot`, the worker mounts each session's tree with FUSE at `FuseRoot/<session>` and gives the agent `run_command`. The command runs in a sandbox that sees only that session's workspace. System directories are read-only, and each command starts with a small environment.

The worker needs FUSE and user namespaces. Without `FuseRoot`, the workspace stays in the process, and the agent has no shell.

To mount a tree yourself, use `fuse.Mount(ms, dir)` (package `vfs/fuse`) and add `shell.RunCommand(dir)` to `AgentOptions.Tools`.

## Use the workspace from Go

A `*vfs.MountSession` is the tree for one session. `Route` picks the source of a path, and the file operations are on the route:

```go
ms, err := vfs.Tree(vfs.At("work", vfs.Local("/srv/tacklr/work")))(ctx, "session-1", vfs.Request{})

note := ms.Route(ctx, "/workspace/work/note.txt")
err = note.WriteFile(ctx, []byte("a\nb\nc\n"))

doc, err := note.ReadText(ctx) // the file as an editable document
err = doc.SetText(strings.ToUpper(doc.Text()))
err = note.WriteDocument(ctx, doc)
```

| `Route` method | Does |
|---|---|
| `ReadFile`, `WriteFile` | Whole files as bytes. |
| `Open` | Streams a file, with range reads on remote sources. |
| `ReadLines` | A window of lines from a large file, without reading all of it. Lines are 1-based. |
| `ReadText`, `OpenDocument`, `WriteDocument` | A file as a document, through its codec. |
| `Stat`, `ReadDir`, `MkdirAll`, `Remove` | The usual file operations. |

`FileInfo.Version` changes when a file's content changes (an ETag on S3 and Azure Blob, a checksum on Drive, a content tag on Graph), so you can find changed files without reading them.

### Limits

| Limit | Value |
|---|---|
| A whole-file read or write | 32 MiB (`vfs.MaxReadFileBytes`) |
| One line | 1 MiB (`vfs.MaxLineBytes`) |
| Lines in one `ReadLines` window | 500 |

`Open` and `ReadLines` stream, so they have no file size limit.

### Errors

| Error | Means |
|---|---|
| `vfs.ErrNotExist` | The file does not exist. |
| `vfs.ErrNotMounted` | The path is not under a mount. |
| `vfs.ErrReadOnly` | The mount is read-only. |
| `vfs.ErrStaleContent` | The file changed since it was read. Read it again. |
| `vfs.ErrConflict` | Another writer changed the document first. |
| `vfs.ErrTooLarge` | The file is over the size limit. |
| `vfs.ErrNoCodec`, `vfs.ErrNotTextual` | The file's type cannot be read as a document. |
| `vfs.ErrAuthExpired` | The user's token expired. The client must send a new one. |
| `vfs.ErrPermission` | The source refused the request. |

## Write a source

A source is a `vfs.Open` function that returns a `vfs.Provider`:

```go
type Provider interface {
	Validate(ctx context.Context) error
	Stat(ctx context.Context, name string) (FileInfo, error)
	OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error)
	ReadDir(ctx context.Context, name string) ([]DirEntry, error)
	Remove(ctx context.Context, name string) error
	MkdirAll(ctx context.Context, name string, perm fs.FileMode) error
}
```

A `File` adds `io.Reader`, `io.ReaderAt`, or `io.Writer` when the source supports them. `Stat` must set `FileInfo.MediaType` on files, and should set `Version`. Keep your clients and credentials in the `Open` closure, not in the mount, so they never reach a checkpoint.
