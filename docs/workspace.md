# Workspace

The workspace is the agent's file system: one tree under `/workspace`, built from the sources you choose. The agent sees only virtual paths, such as `/workspace/work/main.go`. It never sees host paths, bucket names, other sessions, or credentials.

The workspace is a `vfs` tree that your sandbox supervisor opens on the sandbox host (package `sandbox/supervisor`). The agent works on it with `run_command`: bash scripts in a sandboxed worker. The session runtime never opens the tree, and the sandboxed worker never holds a credential.

## Build a workspace

Give your supervisor the tree in `supervisor.Config.Workspace`. Each `vfs.At` mounts one source as a directory under `/workspace`:

```go
sup, err := supervisor.New(supervisor.Config{
	WorkerPath:  "/usr/libexec/tacklr-sandbox-worker",
	PackagesDir: "/usr/lib/tacklr-sandbox/packages",
	TLS:         serverTLS,
	Workspace: vfs.Tree(
		vfs.At("work", vfs.S3(awss3.API{Client: s3Client}, "acme-work")),
		vfs.At("docs", vfs.S3(awss3.API{Client: s3Client}, "acme-docs")).ReadOnly(),
	).WithCodecs(adapters.Codecs()...), // Word and Excel files read as text
})
```

Then give the session runtime a client of the supervisor (`worker.Config.Sandbox` on Temporal). Each `run_command` sends the session ID and the turn's bindings, and the supervisor opens the tree with them. The Temporal example builds its supervisor this way (`examples/temporal/cmd/supervisor`).

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
| `vfs.Union(opens...)` | `vfs` | A read-only merge of several sources, for example skill packs. |

`vfs.Local` and `vfs.SessionLocal` are directories on the sandbox host, so a session's files there stay on that host. The S3 and Azure Blob adapters are in their own packages, so a supervisor links a cloud SDK only when it uses it. The Google and Microsoft adapters use the REST APIs over `net/http` and link no vendor SDK.

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

In your supervisor's `Workspace`, build the member from the turn's binding:

```go
func open(ctx context.Context, id string, req vfs.Request) (*vfs.MountSession, error) {
	members := []vfs.Member{vfs.At("work", vfs.SessionLocal("/srv/tacklr/work"))}
	if b, ok := vfs.BindingByName(req.Bindings, "drive"); ok && b.Auth.Token != "" {
		h := req.Holder(b) // refreshes through the session runtime
		members = append(members, vfs.At("drive", vfs.Drive(vfs.NewGoogleDrive(h, "", nil), h)))
	}
	return vfs.Tree(members...)(ctx, id, req)
}
```

The token goes to `session.SecretStorage`, sealed. It is never in the checkpoint, in Temporal history, or in the ACP wire store. It reaches the supervisor only for the length of a `run_command`, and never the sandboxed worker. See [Sessions in production](session.md#credentials).

When a token is near expiry, or Drive or Microsoft 365 refuses it, the supervisor asks the session runtime for a new one on the `run_command` stream. Set `sandbox.Client.Refresh` to answer: it gets the session ID and the binding (without its token) and returns a new `mount.Credential`, for example from your identity provider. Without `Refresh`, a source with an expired token fails, and a workspace that cannot open refuses the worker.

| Provider | Read-only scopes | Writable scopes | Parameters |
|---|---|---|---|
| Google Drive | `drive.readonly` | `drive`, `documents`, `spreadsheets` | `folderId` (empty: My Drive) |
| Microsoft Graph | `Files.Read` | `Files.ReadWrite` | `account` (`organization` or `personal`), `driveId`, `siteId`, `itemId` |

An `organization` account needs a `siteId` or a `driveId`, and a SharePoint library also needs `Sites.Read.All` or `Sites.ReadWrite.All`.

## How the agent works on files

The agent has one tool for files, `run_command`. Each call starts a sandboxed worker (WASIX bash, coreutils, grep, sed and findutils, with no network) and runs the script in `/workspace`. The worker's file calls go to its supervisor over a local socket, and the supervisor serves them from the tree (`vfs.Files`), so a script reads and writes the sources directly.

- Text, Markdown, JSON, YAML and code read and write as bytes.
- Google Docs, Sheets, Word (`.docx`) and Excel (`.xlsx`) read as their text and cannot be written. Word and Excel need `WithCodecs(adapters.Codecs()...)` on the tree.
- Other types, such as images, read and write as bytes.
- A write goes to the source when the script closes the file. Nothing is cached between calls.

`run_command` asks the user for permission unless it is in `AgentOptions.AllowTools`. Without a sandbox client, the agent has no file access. To add the tool yourself, add `sandbox.RunCommand(client)` to `AgentOptions.Tools`.

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
