package workspace

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// maxIndexFilePaths caps paths per index_file call: key files, not a bulk walk.
const maxIndexFilePaths = 8

// vfsIndexTools are index_file and unindex over the turn's indexer.
type vfsIndexTools struct {
	idx *vfsindex.MountIndexer
}

type indexFileArgs struct {
	Path  string   `json:"path,omitempty" desc:"Absolute virtual file path (e.g. /work/docs/API.md). Not a directory. One of path or paths required."`
	Paths []string `json:"paths,omitempty" desc:"Optional batch of absolute virtual file paths (max 8). Prefer key files for the current plan only."`
}

type unindexArgs struct {
	Path string `json:"path" desc:"Absolute virtual path to remove from the search index. Does not delete the file."`
}

func (v vfsIndexTools) newIndexFile() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "index_file",
		DisplayName: "Index {path}",
		Description: `Index one or more files into the knowledge store so later search can find them without carrying the full file body. Call for papers, protocols, notes, or specs that this plan or later todos will need, typically near the end of a research or discovery todo.

Requires an active plan. Returns one status line per path: indexed, skipped (unchanged, binary, or empty), or error. Does not return file contents. The whole call fails (nothing indexed) if more than 8 paths are passed, a path is a directory, a path is missing, or indexing is disabled on that mount.`,
		Category: message.ToolCategoryExecute,
		Access:   tacklr.ToolWriteAccess,
		Timeout:  120 * time.Second,
		Handler:  v.indexFile,
	})
}

func (v vfsIndexTools) newUnindex() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "unindex",
		DisplayName: "Unindex {path}",
		Description: `Remove a file from the knowledge index so it no longer appears in search. Call when the wrong file was indexed or it should no longer be found for this task. Does not delete the file on disk. Requires an active plan. Returns unindexed path=… when a record was removed, or noop path=… when nothing was indexed.`,
		Category:    message.ToolCategoryDelete,
		Access:      tacklr.ToolWriteAccess,
		Timeout:     30 * time.Second,
		Handler:     v.unindex,
	})
}

// collectIndexPaths normalizes path/paths into an absolute file list.
// Oversize batches error with no partial work.
func collectIndexPaths(path string, paths []string) ([]string, error) {
	single := strings.TrimSpace(path)
	n := len(paths)
	if single != "" {
		n++
	}
	if n == 0 {
		return nil, fmt.Errorf("index_file: path or paths is required")
	}
	if n > maxIndexFilePaths {
		return nil, fmt.Errorf("index_file: at most %d paths per call (got %d); no files indexed", maxIndexFilePaths, n)
	}
	out := make([]string, 0, n)
	if single != "" {
		abs, err := vfs.CleanPath(single)
		if err != nil {
			return nil, err
		}
		out = append(out, abs)
	}
	for _, p := range paths {
		abs, err := vfs.CleanPath(p)
		if err != nil {
			return nil, err
		}
		out = append(out, abs)
	}
	return out, nil
}

func (v vfsIndexTools) indexFile(ctx context.Context, args indexFileArgs, cc *tacklr.CallContext) (string, error) {
	paths, err := collectIndexPaths(args.Path, args.Paths)
	if err != nil {
		return "", err
	}
	// Check every path before any index write, so a rejected path
	// does not leave earlier paths of the batch indexed. IndexFile
	// reuses the Stat.
	type job struct {
		path string
		st   vfs.FileInfo
	}
	jobs := make([]job, 0, len(paths))
	for _, p := range paths {
		if spec, err := v.idx.VFS.SpecAt(p); err == nil && spec.NotIndexed {
			return "", fmt.Errorf("index_file: indexing is off for the mount of %s", p)
		}
		st, err := v.idx.VFS.Route(ctx, p).Stat(ctx)
		if err != nil {
			return "", vfsToolErr("index_file", p, err)
		}
		if st.IsDir {
			return "", fmt.Errorf("index_file: path must be a file, not a directory: %s", p)
		}
		jobs = append(jobs, job{path: p, st: st})
	}
	cc.EmitUpdate(fmt.Sprintf("Indexing %d path(s)…", len(jobs)))
	var b strings.Builder
	b.Grow(len(jobs) * 48)
	for i, j := range jobs {
		if i > 0 {
			b.WriteByte('\n')
		}
		res, err := v.idx.IndexFile(ctx, j.path, j.st)
		if err != nil {
			fmt.Fprintf(&b, "error path=%s: %v", j.path, err)
			continue
		}
		fmt.Fprintf(&b, "%s path=%s", res, j.path)
	}
	return b.String(), nil
}

func (v vfsIndexTools) unindex(ctx context.Context, args unindexArgs, cc *tacklr.CallContext) (string, error) {
	p, err := vfs.CleanPath(args.Path)
	if err != nil {
		return "", err
	}
	cc.EmitUpdate("Unindexing " + p)
	removed, err := v.idx.UnindexPath(ctx, p)
	if err != nil {
		return "", fmt.Errorf("unindex: %w", err)
	}
	if !removed {
		return "noop path=" + p, nil
	}
	return "unindexed path=" + p, nil
}
