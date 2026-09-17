package tacklr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// maxIndexFilePaths caps paths per index_file call (selective, not bulk).
const maxIndexFilePaths = 8

// vfsIndexTools closes over the vfsindex.Bridge (indexer + policy/track).
type vfsIndexTools struct {
	br *vfsindex.Bridge
}

func newVFSIndexTools(br *vfsindex.Bridge) []*Tool {
	v := vfsIndexTools{br: br}
	return []*Tool{
		v.newIndexFile(),
		v.newUnindex(),
	}
}

type indexFileArgs struct {
	Path  string   `json:"path,omitempty" desc:"Absolute virtual file path (e.g. /work/docs/API.md). Not a directory. One of path or paths required."`
	Paths []string `json:"paths,omitempty" desc:"Optional batch of absolute virtual file paths (max 8). Prefer key files for the current plan only."`
}

type unindexArgs struct {
	Path string `json:"path" desc:"Absolute virtual path whose brain mirror should be soft-deleted. Does not delete the VFS file."`
}

func (v vfsIndexTools) newIndexFile() *Tool {
	return NewTool(ToolConfig{
		Name:        "index_file",
		DisplayName: "Index {path}",
		Description: `Index a file so it can be found later by search instead of re-reading the whole file.

WHEN TO USE
- A file matters for this plan or later todos (papers, protocols, notes, specs).
- Near the end of a research or discovery todo, before completing it, so the next step does not need the full file body.

WHEN NOT TO USE
- Entire directory trees, binaries, generated files, or secrets.
- A one-off read for the current step only — use read.
- Prefer few paths (max 8 per call).

HOW TO USE
1) Confirm the file with read (live names/grep: run_command → fd / find / rg).
2) Index the path, or a short paths list.
3) Later: search to recall; open the live file with read using the path and line/block from the hit.

Requires an active plan. Returns status only, not file contents.`,
		Category: ToolCategoryExecute,
		Access:   ToolWriteAccess,
		Timeout:  120 * time.Second,
		Handler: func(ctx context.Context, args indexFileArgs, runtime HarnessRuntime) (string, error) {
			paths, err := collectIndexPaths(args.Path, args.Paths)
			if err != nil {
				return "", err
			}
			// Validate all paths before any index write so directory / missing
			// rejects do not partially index earlier paths in the batch.
			// One policy lookup + Stat per path; IndexFileResult reuses Stat.
			type job struct {
				path   string
				st     vfs.FileInfo
				policy string
			}
			jobs := make([]job, 0, len(paths))
			for _, p := range paths {
				policy := v.br.PolicyAt(p)
				if policy == vfsindex.PolicyNone {
					return "", fmt.Errorf("index_file: indexing disabled for mount of %s (IndexPolicy=none)", p)
				}
				st, err := v.br.Indexer.VFS.Stat(ctx, p)
				if err != nil {
					return "", fmt.Errorf("index_file: %s: %w", p, err)
				}
				if st.IsDir {
					return "", fmt.Errorf("index_file: path must be a file, not a directory: %s", p)
				}
				jobs = append(jobs, job{path: p, st: st, policy: policy})
			}
			runtime.EmitUpdate(fmt.Sprintf("Indexing %d path(s)…", len(jobs)))
			var b strings.Builder
			b.Grow(len(jobs) * 48)
			for i, j := range jobs {
				if i > 0 {
					b.WriteByte('\n')
				}
				res, err := v.br.Indexer.IndexFileResult(ctx, j.path, j.st)
				if err != nil {
					fmt.Fprintf(&b, "error path=%s: %v", j.path, err)
					continue
				}
				if j.policy == vfsindex.PolicySelective &&
					(res == vfsindex.PathIndexed || res == vfsindex.PathSkipped) {
					v.br.Track(j.path)
				}
				fmt.Fprintf(&b, "%s path=%s", res, j.path)
			}
			return b.String(), nil
		},
	})
}

func (v vfsIndexTools) newUnindex() *Tool {
	return NewTool(ToolConfig{
		Name:        "unindex",
		DisplayName: "Unindex {path}",
		Description: `Stop indexing a file so it no longer appears in search. Use when you indexed the wrong file. Does not delete the file. Requires an active plan.`,
		Category:    ToolCategoryDelete,
		Access:      ToolWriteAccess,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, args unindexArgs, runtime HarnessRuntime) (string, error) {
			p, err := vfs.CleanPath(args.Path)
			if err != nil {
				return "", err
			}
			runtime.EmitUpdate("Unindexing " + p)
			removed, err := v.br.Indexer.UnindexPath(ctx, p)
			if err != nil {
				return "", fmt.Errorf("unindex: %w", err)
			}
			v.br.Untrack(p)
			if !removed {
				return "noop path=" + p, nil
			}
			return "unindexed path=" + p, nil
		},
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
