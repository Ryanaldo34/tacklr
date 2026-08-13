package tacklr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// maxIndexFilePaths caps paths per index_file call (selective, not bulk).
const maxIndexFilePaths = 8

// maxFindContentResults default/max page size for find_content.
const maxFindContentResults = 20

// vfsIndexTools closes over the vfsindex.Bridge (indexer + policy/track).
type vfsIndexTools struct {
	br *vfsindex.Bridge
}

func newVFSIndexTools(br *vfsindex.Bridge) []*Tool {
	if br == nil || br.Indexer == nil {
		return nil
	}
	v := vfsIndexTools{br: br}
	return []*Tool{
		v.newIndexFile(),
		v.newUnindex(),
		v.newFindContent(),
	}
}

type indexFileArgs struct {
	Path  string   `json:"path,omitempty" desc:"Absolute virtual file path (e.g. /work/docs/API.md). Not a directory. One of path or paths required."`
	Paths []string `json:"paths,omitempty" desc:"Optional batch of absolute virtual file paths (max 8). Prefer key files for the current plan only."`
}

type unindexArgs struct {
	Path string `json:"path" desc:"Absolute virtual path whose brain mirror should be soft-deleted. Does not delete the VFS file."`
}

type findContentArgs struct {
	Query string `json:"query" desc:"Text to search in indexed file chunks (requires vfs_path on hits)."`
	Limit int    `json:"limit,omitempty" desc:"Max hits (default 10, max 20)."`
}

func (v vfsIndexTools) newIndexFile() *Tool {
	return NewTool(ToolConfig{
		Name:        "index_file",
		DisplayName: "Index {path}",
		Description: `Index one or more key virtual files into the knowledge brain as Document + Chunks with vfs_path (and line/block anchors). Enables later search / find_content instead of re-reading large files into context across planning handoffs.

WHEN TO USE
- You found a file that matters for THIS plan or later open todos (specs, README, API docs, behavior-defining config).
- Near the end of a research/discovery todo, before complete_todo, when the next handoff should not carry the full file body.
- To seed search for a path under selective index policy (index_file is the promote step).

WHEN NOT TO USE
- Do not index entire mounts, vendor trees, or "everything under /work".
- Do not index binaries, generated noise, or secret dumps.
- Do not index a one-off read for the current turn only — use read_lines.
- Prefer few paths (max 8 per call); select high-value files only.
- Under mount IndexPolicy=none, this tool errors (indexing disabled).

HOW TO USE
1) list / read_lines (or outline) to confirm the right file.
2) index_file with path or a short paths list.
3) Later: find_content or search; open live content with read_lines using vfs_path and start_line / block_id from hits.

Requires an active plan (writes unlock after create_plan). Returns compact status only — not file contents. Under selective policy, a successful index tracks the path so later persists reindex it. Under prefix/watch, AfterPersist already reindexes.`,
		Category: streaming.ToolCategoryExecute,
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
		Description: `Remove the brain mirror for a virtual path (soft-delete Document/Chunks for that vfs_path). Use when you indexed the wrong file or the path should no longer appear in search for this task.

Does not delete the real VFS file. Idempotent if nothing was indexed. Requires an active plan. Prefer unindex over leaving misleading chunks for later todos. Also drops selective track for the path.`,
		Category: streaming.ToolCategoryDelete,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args unindexArgs, runtime HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
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

func (v vfsIndexTools) newFindContent() *Tool {
	return NewTool(ToolConfig{
		Name:        "find_content",
		DisplayName: "Find content: {query}",
		Description: `Search indexed virtual files for a query (temporary thin tool until run_command + host rg).

Hits must have properties.vfs_path — non-file brain objects are omitted. Returns path, start_line/end_line/block_id, and a short snippet. Open live text with read_lines (not brain read) using those anchors.

Requires the index bridge (Brain + VFS + namespace). Under selective policy, index_file first. Under prefix/watch, files are indexed automatically after persist.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args findContentArgs, runtime HarnessRuntime) (string, error) {
			q := strings.TrimSpace(args.Query)
			if q == "" {
				return "", fmt.Errorf("find_content: query is required")
			}
			limit := args.Limit
			if limit <= 0 {
				limit = 10
			}
			if limit > maxFindContentResults {
				limit = maxFindContentResults
			}
			runtime.EmitUpdate("Finding content in indexed files…")
			// Over-fetch then filter to vfs_path-backed parents.
			page, err := v.br.Indexer.Brain.Search(ctx, v.br.Indexer.Scope, brain.SearchRequest{
				Query: q,
				Limit: limit * 2,
			}, brain.NewSearchContext())
			if err != nil {
				return "", fmt.Errorf("find_content: %w", err)
			}
			var b strings.Builder
			b.Grow(limit * 96)
			n := 0
			for _, obj := range page.Objects {
				vpath, _ := obj.Properties[vfsindex.PropVFSPath].(string)
				if strings.TrimSpace(vpath) == "" {
					continue
				}
				// Prefer evidence anchors when present.
				startLine, endLine, blockID, snippet := contentHitFromEvidence(obj)
				if snippet == "" {
					snippet = strings.TrimSpace(obj.Summary)
				}
				if n > 0 {
					b.WriteByte('\n')
				}
				fmt.Fprintf(&b, "path=%s", vpath)
				if startLine > 0 {
					fmt.Fprintf(&b, " start_line=%d", startLine)
				}
				if endLine > 0 {
					fmt.Fprintf(&b, " end_line=%d", endLine)
				}
				if blockID != "" {
					fmt.Fprintf(&b, " block_id=%s", blockID)
				}
				if snippet != "" {
					fmt.Fprintf(&b, " snippet=%q", truncateSnippet(snippet, 160))
				}
				n++
				if n >= limit {
					break
				}
			}
			if n == 0 {
				return "count=0", nil
			}
			return fmt.Sprintf("count=%d\n%s", n, b.String()), nil
		},
	})
}

func contentHitFromEvidence(obj brain.RichObject) (start, end int, blockID, snippet string) {
	if len(obj.Evidence) == 0 {
		return 0, 0, "", ""
	}
	ev := obj.Evidence[0]
	snippet = strings.TrimSpace(ev.Snippet)
	if ev.Properties != nil {
		if v, ok := numProp(ev.Properties[vfsindex.PropStartLine]); ok {
			start = v
		}
		if v, ok := numProp(ev.Properties[vfsindex.PropEndLine]); ok {
			end = v
		}
		if s, ok := ev.Properties[vfsindex.PropBlockID].(string); ok {
			blockID = s
		}
	}
	return start, end, blockID, snippet
}

func numProp(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	default:
		return 0, false
	}
}

func truncateSnippet(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if maxLen <= 0 {
		return s
	}
	n := 0
	for i := range s {
		if n == maxLen {
			return s[:i] + "…"
		}
		n++
	}
	return s
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
		abs, err := absVirtual(single)
		if err != nil {
			return nil, err
		}
		out = append(out, abs)
	}
	for _, p := range paths {
		abs, err := absVirtual(p)
		if err != nil {
			return nil, err
		}
		out = append(out, abs)
	}
	return out, nil
}
