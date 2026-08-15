package tacklr

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

// vfsTools closes over session mounts. Rev checks live here so MountSession
// stays a thin path/IR API (no high-level ReplaceLinesAt surface).
type vfsTools struct {
	ms *vfs.MountSession
}

func newVFSTools(ms *vfs.MountSession) []*Tool {
	v := vfsTools{ms: ms}
	return []*Tool{v.newRead(), v.newWrite()}
}

type readArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path to read."`
	Rev     string `json:"rev,omitempty" desc:"Optional expected content hash from a prior read or write. Mismatch returns stale content."`
	Start   int    `json:"start,omitempty" desc:"1-based start line (inclusive). Ignored when block_id is set."`
	End     int    `json:"end,omitempty" desc:"1-based end line (exclusive). Ignored when block_id is set."`
	BlockID string `json:"block_id,omitempty" desc:"Structured block id (e.g. heading path installation or api/errors). Media-agnostic."`
	Outline bool   `json:"outline,omitempty" desc:"If true, include structured block outline when available."`
	IR      bool   `json:"ir,omitempty" desc:"If true, include media_type/encoding/line_count. Full text= only when no window or block."`
}

type writeArgs struct {
	Path           string   `json:"path" desc:"Absolute virtual path to write."`
	Rev            string   `json:"rev,omitempty" desc:"Required when path exists: hash from the latest read. Omit only to create (full mode)."`
	Content        *string  `json:"content,omitempty" desc:"Full new file body (UTF-8). Creates or replaces the whole file. Empty creates or truncates."`
	IRText         *string  `json:"ir_text,omitempty" desc:"Full IR body. Same as content; if both are set they must match."`
	Start          *int     `json:"start,omitempty" desc:"1-based start line (inclusive). Lines mode."`
	End            *int     `json:"end,omitempty" desc:"1-based end line (exclusive). Required in lines mode."`
	Lines          []string `json:"lines,omitempty" desc:"Replacement lines (no embedded newlines). Empty deletes the span. Or use body."`
	Body           *string  `json:"body,omitempty" desc:"Replacement body as one string (split on newlines). Used if lines is empty."`
	Old            *string  `json:"old,omitempty" desc:"Exact substring to find. Must be unique unless replace_all."`
	New            *string  `json:"new,omitempty" desc:"Replacement text. Omitted treated as empty."`
	ReplaceAll     bool     `json:"replace_all,omitempty" desc:"Replace every occurrence of old."`
	BlockID        string   `json:"block_id,omitempty" desc:"Replace this structured block's body (or full span if include_heading)."`
	IncludeHeading bool     `json:"include_heading,omitempty" desc:"When block_id is a heading, replace the heading line too."`
}

func (v vfsTools) newRead() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read {path}",
		Description: `Read a virtual path: first page by default, or a line window / structured block.

Path only returns start=1 through 1+MaxLinesPerWindow plus rev. Use start/end for a half-open 1-based window, or block_id for a structured region. Set outline=true to list blocks. Optional rev must match or the tool returns stale content. ir=true adds media_type/encoding/line_count (and text= when there is no window or block). Pass rev to write. Live names/grep: run_command → ls / rg. Tree ops: run_command → mkdir / rm.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			rt.EmitUpdate("Reading " + p)

			explicitWindow := args.Start > 0 || args.End > 0
			if !explicitWindow && args.BlockID == "" && !args.Outline {
				args.Start = 1
				args.End = 1 + vfs.MaxLinesPerWindow
				explicitWindow = true
			}

			if args.BlockID != "" || args.Outline || args.IR {
				return v.readStructured(ctx, p, args, explicitWindow)
			}

			if args.Start < 1 || args.End < args.Start {
				return "", fmt.Errorf("invalid range start=%d end=%d (or set block_id / outline)", args.Start, args.End)
			}
			win, err := v.ms.ReadLines(ctx, p, args.Start, args.End)
			if err != nil {
				return "", err
			}
			rev := win.Rev
			if rev.Hash == "" {
				r, rerr := v.ms.ContentRev(ctx, p)
				if rerr != nil {
					if args.Rev != "" {
						return "", fmt.Errorf("read: %w", rerr)
					}
				} else {
					rev = r
				}
			}
			if args.Rev != "" && args.Rev != rev.Hash {
				return "", vfs.ErrStaleContent
			}
			var b strings.Builder
			growLineWindow(&b, 96+len(win.Path)+len(rev.Hash), win.Lines)
			fmt.Fprintf(&b, "path=%s rev=%s start=%d end=%d returned=%d eof=%v next_start=%d\n",
				win.Path, rev.Hash, win.Start, win.End, win.Returned, win.EOF, win.NextStart)
			for i, line := range win.Lines {
				fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
			}
			return b.String(), nil
		},
	})
}

func (v vfsTools) readStructured(ctx context.Context, p string, args readArgs, explicitWindow bool) (string, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return "", err
	}
	rev := vfs.ContentHash(doc.Text())
	if args.Rev != "" && args.Rev != rev {
		return "", vfs.ErrStaleContent
	}
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	var b strings.Builder
	b.Grow(128 + len(p) + len(rev) + len(doc.MediaType()))
	if args.IR {
		fmt.Fprintf(&b, "path=%s rev=%s media_type=%s encoding=%s line_count=%d\n",
			p, rev, doc.MediaType(), doc.Encoding(), doc.LineCount())
	} else {
		fmt.Fprintf(&b, "path=%s rev=%s media_type=%s line_count=%d\n",
			p, rev, doc.MediaType(), doc.LineCount())
	}
	if args.Outline && len(blocks) > 0 {
		b.WriteString("outline:\n")
		for _, bl := range blocks {
			fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
				bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
		}
	}
	start, end := args.Start, args.End
	if args.BlockID != "" {
		if len(blocks) == 0 {
			return "", fmt.Errorf("no structured blocks on this document")
		}
		bl, ok := vfs.FindBlock(blocks, args.BlockID)
		if !ok {
			return "", fmt.Errorf("unknown block_id %q", args.BlockID)
		}
		start, end = bl.Style.Span.StartLine, bl.Style.Span.EndLine
		fmt.Fprintf(&b, "block_id=%s\n", bl.ID)
	}
	if start > 0 && end >= start {
		win, err := lineWindowFromTextDoc(doc, start, end)
		if err != nil {
			return "", err
		}
		growLineWindow(&b, 64, win.Lines)
		fmt.Fprintf(&b, "start=%d end=%d returned=%d eof=%v next_start=%d\n",
			win.Start, win.End, win.Returned, win.EOF, win.NextStart)
		for i, line := range win.Lines {
			fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
		}
	}
	if args.IR && args.BlockID == "" && !explicitWindow {
		fmt.Fprintf(&b, "text=%s\n", doc.Text())
	}
	return b.String(), nil
}

func (v vfsTools) newWrite() *Tool {
	return NewTool(ToolConfig{
		Name:        "write",
		DisplayName: "Write {path}",
		Description: `Write a virtual file: full body, line span, substring, or structured block. Exactly one mode per call.

Pass rev from read when the path exists. Create only via content or ir_text (empty content creates or truncates). Modes are selected by which field is set: content|ir_text, old, block_id, or start. Persists immediately.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args writeArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			n, full := writeModeCount(args)
			switch {
			case n == 0:
				return "", fmt.Errorf("write: no mutation")
			case n > 1:
				return "", fmt.Errorf("write: exactly one of content|ir_text, old, block_id, start")
			}
			var fullBody string
			if full {
				fullBody, err = fullWriteBody(args)
				if err != nil {
					return "", err
				}
			}
			rt.EmitUpdate("Writing " + p)

			fi, err := v.ms.Stat(ctx, p)
			exists := err == nil
			if err != nil && !errors.Is(err, vfs.ErrNotExist) {
				return "", err
			}
			if exists {
				if strings.TrimSpace(args.Rev) == "" {
					return "", fmt.Errorf("write: rev required when path exists")
				}
			} else if !full {
				return "", vfs.ErrNotExist
			}

			switch {
			case full:
				return v.writeFull(ctx, p, exists, fi, args.Rev, fullBody)
			case args.Old != nil:
				return v.writeSubstring(ctx, p, args)
			case args.BlockID != "":
				return v.writeBlock(ctx, p, args)
			default:
				return v.writeLines(ctx, p, args)
			}
		},
	})
}

func writeModeCount(args writeArgs) (n int, full bool) {
	full = args.Content != nil || args.IRText != nil
	if full {
		n++
	}
	if args.Old != nil {
		n++
	}
	if args.BlockID != "" {
		n++
	}
	if args.Start != nil {
		n++
	}
	return n, full
}

func (v vfsTools) writeFull(ctx context.Context, p string, exists bool, fi vfs.FileInfo, rev, body string) (string, error) {
	if exists {
		cur, err := v.ms.ContentRev(ctx, p)
		if err != nil {
			return "", err
		}
		if cur.Hash != rev {
			return "", vfs.ErrStaleContent
		}
	}
	if len(body) > vfs.MaxReadFileBytes {
		return "", vfs.ErrTooLarge
	}
	mt := ""
	if exists {
		mt = fi.MediaType
	}
	if mt == "" {
		n := min(len(body), 512)
		mt = vfs.DetectMediaType(path.Base(p), []byte(body[:n]))
	}
	return v.stage(ctx, vfs.NewTextDocument(p, mt, "utf-8", body))
}

func fullWriteBody(args writeArgs) (string, error) {
	switch {
	case args.Content != nil && args.IRText != nil:
		if *args.Content != *args.IRText {
			return "", fmt.Errorf("write: content and ir_text disagree")
		}
		return *args.Content, nil
	case args.IRText != nil:
		return *args.IRText, nil
	default:
		return *args.Content, nil
	}
}

func (v vfsTools) writeSubstring(ctx context.Context, p string, args writeArgs) (string, error) {
	if *args.Old == "" {
		return "", fmt.Errorf("write: old is required")
	}
	doc, err := v.loadMatching(ctx, p, args.Rev)
	if err != nil {
		return "", err
	}
	repl := ""
	if args.New != nil {
		repl = *args.New
	}
	body := doc.Text()
	n := strings.Count(body, *args.Old)
	switch {
	case n == 0:
		return "", fmt.Errorf("write: old text not found")
	case !args.ReplaceAll && n != 1:
		return "", fmt.Errorf("write: old text occurs %d times (need unique match or replace_all)", n)
	}
	if args.ReplaceAll {
		doc.SetText(strings.ReplaceAll(body, *args.Old, repl))
	} else {
		doc.SetText(strings.Replace(body, *args.Old, repl, 1))
	}
	out, err := v.stage(ctx, doc)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s replacements=%d", out, n), nil
}

func (v vfsTools) writeBlock(ctx context.Context, p string, args writeArgs) (string, error) {
	doc, err := v.loadMatching(ctx, p, args.Rev)
	if err != nil {
		return "", err
	}
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	bl, ok := vfs.FindBlock(blocks, args.BlockID)
	if !ok {
		return "", fmt.Errorf("write: unknown block_id %q", args.BlockID)
	}
	start, end, err := vfs.BlockReplaceSpan(bl, args.IncludeHeading)
	if err != nil {
		return "", err
	}
	if err := doc.ReplaceLines(start, end, replacementLines(args.Lines, args.Body)); err != nil {
		return "", err
	}
	return v.stage(ctx, doc)
}

func (v vfsTools) writeLines(ctx context.Context, p string, args writeArgs) (string, error) {
	if args.End == nil || *args.Start < 1 || *args.End < *args.Start {
		return "", fmt.Errorf("write: invalid range start=%d end=%v", *args.Start, args.End)
	}
	doc, err := v.loadMatching(ctx, p, args.Rev)
	if err != nil {
		return "", err
	}
	if err := doc.ReplaceLines(*args.Start, *args.End, replacementLines(args.Lines, args.Body)); err != nil {
		return "", err
	}
	return v.stage(ctx, doc)
}

func replacementLines(lines []string, body *string) []string {
	if len(lines) > 0 || body == nil || *body == "" {
		return lines
	}
	out := strings.Split(*body, "\n")
	if strings.HasSuffix(*body, "\n") && len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

func lineWindowFromTextDoc(doc vfs.Textual, start, end int) (vfs.LineWindow, error) {
	n := doc.LineCount()
	if start < 1 || end < start {
		return vfs.LineWindow{}, fmt.Errorf("invalid range")
	}
	eof := false
	if end > n+1 {
		end = n + 1
		eof = true
	}
	if start > n+1 {
		return vfs.LineWindow{}, vfs.ErrLineOutOfRange
	}
	lines, err := doc.Lines(start, end)
	if err != nil {
		return vfs.LineWindow{}, err
	}
	return vfs.LineWindow{
		Start: start, End: end, Lines: lines,
		Returned: len(lines), EOF: eof, NextStart: start + len(lines),
	}, nil
}

func (v vfsTools) loadMatching(ctx context.Context, p, expected string) (vfs.Textual, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return nil, err
	}
	if vfs.ContentHash(doc.Text()) != expected {
		return nil, vfs.ErrStaleContent
	}
	return doc, nil
}

func (v vfsTools) stage(ctx context.Context, doc vfs.Textual) (string, error) {
	if err := v.ms.WriteDocument(ctx, doc); err != nil {
		return "", err
	}
	return fmt.Sprintf("path=%s rev=%s line_count=%d", doc.Path(), vfs.ContentHash(doc.Text()), doc.LineCount()), nil
}

func growLineWindow(b *strings.Builder, extra int, lines []string) {
	n := extra
	for _, line := range lines {
		n += 8 + len(line)
	}
	b.Grow(n)
}

func absVirtual(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" || !path.IsAbs(p) {
		return "", fmt.Errorf("path must be an absolute virtual path")
	}
	if strings.ContainsAny(p, "\\\x00") {
		return "", vfs.ErrInvalidPath
	}
	return path.Clean(p), nil
}
