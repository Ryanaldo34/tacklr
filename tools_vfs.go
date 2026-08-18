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
	ms                 *vfs.MountSession
	permissionRequired bool
}

func newVFSTools(ms *vfs.MountSession, permissionRequired bool) []*Tool {
	v := vfsTools{ms: ms, permissionRequired: permissionRequired}
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

type writeBlock struct {
	ID         string            `json:"id,omitempty"`
	Kind       string            `json:"kind"`
	Text       string            `json:"text"`
	Level      int               `json:"level,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type writeArgs struct {
	Path           string        `json:"path" desc:"Absolute virtual path to write."`
	Rev            string        `json:"rev,omitempty" desc:"Required when path exists: hash from the latest read. Omit only to create (full mode)."`
	Content        *string       `json:"content,omitempty" desc:"Full new file body (UTF-8). Creates or replaces the whole file. Empty creates or truncates. Create-only lift for Docs."`
	IRText         *string       `json:"ir_text,omitempty" desc:"Full IR body. Same as content; if both are set they must match."`
	Start          *int          `json:"start,omitempty" desc:"1-based start line (inclusive). Lines mode."`
	End            *int          `json:"end,omitempty" desc:"1-based end line (exclusive). Required in lines mode."`
	Lines          []string      `json:"lines,omitempty" desc:"Replacement lines (no embedded newlines). Empty deletes the span. Or use body."`
	Body           *string       `json:"body,omitempty" desc:"Replacement body as one string (split on newlines). Used if lines is empty."`
	Old            *string       `json:"old,omitempty" desc:"Exact substring to find. Must be unique unless replace_all."`
	New            *string       `json:"new,omitempty" desc:"Replacement text. Omitted treated as empty."`
	ReplaceAll     bool          `json:"replace_all,omitempty" desc:"Replace every occurrence of old."`
	BlockID        string        `json:"block_id,omitempty" desc:"Replace this structured block's body (or full span if include_heading)."`
	IncludeHeading bool          `json:"include_heading,omitempty" desc:"When block_id is a heading, replace the heading line too."`
	MediaType      string        `json:"media_type,omitempty" desc:"Create-as-Doc: application/vnd.google-apps.document. Ignored when the path exists. Foo.md is never a Doc."`
	Blocks         *[]writeBlock `json:"blocks,omitempty" desc:"Replace a tab body (SetBlocks) or create a Doc from IR."`
	TabID          string        `json:"tab_id,omitempty" desc:"Required for blocks when the Doc has more than one tab."`
}

func (v vfsTools) newRead() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read {path}",
		Description: `Read a virtual file (not a knowledge object). First page by default, or a line window / block.

Path only on ordinary files → start=1 through 1+MaxLinesPerWindow plus rev (pass rev to write).
Path only on Google Docs (projected) → outline. Use rg on the FUSE tree for HTML hits, then read({block_id}) for IR text.
start/end → half-open 1-based window (HTML lines on Docs). block_id → that region's IR text on Docs. outline=true → block list. ir=true → media_type/encoding (no HTML dump on Docs).
Knowledge objects with no file: read_object. Live names/grep: run_command → ls / rg.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			rt.EmitUpdate("Reading " + p)

			fi, err := v.ms.Stat(ctx, p)
			if err != nil {
				return "", err
			}
			projected := vfs.IsProjected(fi.MediaType)

			explicitWindow := args.Start > 0 || args.End > 0
			pathOnly := !explicitWindow && args.BlockID == "" && !args.Outline && !args.IR
			if pathOnly && projected {
				args.Outline = true
			} else if !explicitWindow && args.BlockID == "" && !args.Outline {
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
	rev := vfs.ContentToken(doc)
	if args.Rev != "" && args.Rev != rev {
		return "", vfs.ErrStaleContent
	}
	projected := vfs.IsProjected(doc.MediaType())
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
	if rd, ok := doc.(*vfs.RichDocument); ok && args.Outline {
		if tabs := rd.Tabs(); len(tabs) > 0 {
			b.WriteString("tabs:\n")
			for _, tb := range tabs {
				fmt.Fprintf(&b, "  %s title=%q index=%d\n", tb.ID, tb.Title, tb.Index)
			}
		}
	}
	if args.Outline && len(blocks) > 0 {
		b.WriteString("outline:\n")
		for _, bl := range blocks {
			tab := ""
			if bl.Style.Attributes != nil {
				tab = bl.Style.Attributes["tab_id"]
			}
			if tab != "" {
				fmt.Fprintf(&b, "  %s kind=%s level=%d tab=%s L%d-L%d %q\n",
					bl.ID, bl.Kind, bl.Style.Level, tab, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
			} else {
				fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
					bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
			}
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
		fmt.Fprintf(&b, "block_id=%s\n", bl.ID)
		if projected {
			fmt.Fprintf(&b, "text=%s\n", bl.Text)
			return b.String(), nil
		}
		start, end = bl.Style.Span.StartLine, bl.Style.Span.EndLine
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
	if args.IR && args.BlockID == "" && !explicitWindow && !projected {
		fmt.Fprintf(&b, "text=%s\n", doc.Text())
	}
	return b.String(), nil
}

func (v vfsTools) newWrite() *Tool {
	cfg := ToolConfig{
		Name:        "write",
		DisplayName: "Write {path}",
		Description: `Write a virtual file: full body, line span, substring, structured block, or Docs blocks. Exactly one mode per call.

Pass rev from read when the path exists. Create only via content or ir_text (empty content creates or truncates), or media_type+blocks for a Google Doc. Foo.md is never a Doc. Extensionless Spec without media_type is plaintext.
Projected Docs: use block_id or blocks. Line/HTML/SetText writes return an error. content lift is create-only. Persists immediately.`,
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
				return "", fmt.Errorf("write: exactly one of content|ir_text, old, block_id, start, blocks")
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
			} else if !full && args.Blocks == nil {
				return "", vfs.ErrNotExist
			}

			switch {
			case full:
				return v.writeFull(ctx, p, exists, fi, args, fullBody)
			case args.Blocks != nil:
				return v.writeBlocks(ctx, p, exists, fi, args)
			case args.Old != nil:
				return v.writeSubstring(ctx, p, args)
			case args.BlockID != "":
				return v.writeBlock(ctx, p, args)
			default:
				return v.writeLines(ctx, p, args)
			}
		},
	}
	if v.permissionRequired {
		cfg.OnCall = OnCalls(ToolPermissionOnCall)
	}
	return NewTool(cfg)
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
	if args.Blocks != nil {
		n++
	}
	return n, full
}

const mediaGoogleDocument = "application/vnd.google-apps.document"

func (v vfsTools) writeFull(ctx context.Context, p string, exists bool, fi vfs.FileInfo, args writeArgs, body string) (string, error) {
	if exists {
		if vfs.IsProjected(fi.MediaType) {
			return "", vfs.ErrProjected
		}
		cur, err := v.ms.ContentRev(ctx, p)
		if err != nil {
			return "", err
		}
		if cur.Hash != args.Rev {
			return "", vfs.ErrStaleContent
		}
	}
	if len(body) > vfs.MaxReadFileBytes {
		return "", vfs.ErrTooLarge
	}
	mt := ""
	if exists {
		mt = fi.MediaType
	} else if args.MediaType == mediaGoogleDocument && path.Ext(p) == "" {
		if looksLikeHTML(body) {
			return "", fmt.Errorf("write: HTML content is not accepted; use blocks")
		}
		return v.stage(ctx, vfs.NewRichDocument(p, mediaGoogleDocument, liftPlaintext(body)))
	} else {
		n := min(len(body), 512)
		mt = vfs.DetectMediaType(path.Base(p), []byte(body[:n]))
		if args.MediaType != "" && vfs.IsProjected(args.MediaType) {
			mt = args.MediaType
		}
		if vfs.IsProjected(mt) {
			return v.stage(ctx, vfs.NewRichDocument(p, mt, liftPlaintext(body)))
		}
	}
	return v.stage(ctx, vfs.NewTextDocument(p, mt, "utf-8", body))
}

func looksLikeHTML(s string) bool {
	t := strings.TrimSpace(s)
	return strings.HasPrefix(t, "<") && strings.Contains(t, ">")
}

func liftPlaintext(s string) []vfs.Block {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	var out []vfs.Block
	for _, para := range strings.Split(s, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		out = append(out, vfs.Block{Kind: vfs.BlockKindParagraph, Text: para})
	}
	if len(out) == 0 {
		out = append(out, vfs.Block{Kind: vfs.BlockKindParagraph, Text: s})
	}
	return out
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
	if vfs.IsProjected(doc.MediaType()) {
		return "", vfs.ErrProjected
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
		if err := doc.SetText(strings.ReplaceAll(body, *args.Old, repl)); err != nil {
			return "", err
		}
	} else if err := doc.SetText(strings.Replace(body, *args.Old, repl, 1)); err != nil {
		return "", err
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
	if args.TabID != "" && bl.Style.Attributes != nil {
		if got := bl.Style.Attributes["tab_id"]; got != "" && got != args.TabID {
			return "", fmt.Errorf("write: tab_id %q does not match block %q", args.TabID, got)
		}
	}
	if rd, ok := doc.(*vfs.RichDocument); ok {
		text := strings.Join(replacementLines(args.Lines, args.Body), "\n")
		if err := rd.ReplaceBlock(bl.ID, text, args.IncludeHeading); err != nil {
			return "", err
		}
		return v.stage(ctx, doc)
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

func (v vfsTools) writeBlocks(ctx context.Context, p string, exists bool, fi vfs.FileInfo, args writeArgs) (string, error) {
	next := make([]vfs.Block, 0, len(*args.Blocks))
	for _, wb := range *args.Blocks {
		attrs := map[string]string{}
		for k, v := range wb.Attributes {
			attrs[k] = v
		}
		if args.TabID != "" {
			attrs["tab_id"] = args.TabID
		}
		next = append(next, vfs.Block{
			ID: wb.ID, Kind: wb.Kind, Text: wb.Text,
			Style: vfs.StyleMeta{Level: wb.Level, Attributes: attrs},
		})
	}
	if !exists {
		mt := args.MediaType
		if mt == "" {
			mt = vfs.DetectMediaType(path.Base(p), nil)
		}
		if mt == mediaGoogleDocument {
			if path.Ext(p) != "" {
				return "", fmt.Errorf("write: blocks require media_type=%s on an extensionless path", mediaGoogleDocument)
			}
			return v.stage(ctx, vfs.NewRichDocument(p, mediaGoogleDocument, next))
		}
		if vfs.IsProjected(mt) {
			return v.stage(ctx, vfs.NewRichDocument(p, mt, next))
		}
		return "", fmt.Errorf("write: blocks require media_type=%s on an extensionless path", mediaGoogleDocument)
	}
	if !vfs.IsProjected(fi.MediaType) {
		return "", vfs.ErrProjected
	}
	if len(next) == 0 {
		return "", fmt.Errorf("write: refusing empty IR replace")
	}
	doc, err := v.loadMatching(ctx, p, args.Rev)
	if err != nil {
		return "", err
	}
	rd, ok := doc.(*vfs.RichDocument)
	if !ok {
		return "", vfs.ErrProjected
	}
	tabs := rd.Tabs()
	if len(tabs) > 1 && args.TabID == "" {
		return "", fmt.Errorf("write: tab_id required")
	}
	if args.TabID != "" && len(tabs) > 0 {
		var keep []vfs.Block
		for _, b := range rd.Blocks() {
			tab := ""
			if b.Style.Attributes != nil {
				tab = b.Style.Attributes["tab_id"]
			}
			if tab != args.TabID {
				keep = append(keep, b)
			}
		}
		next = append(next, keep...)
	}
	rd.SetBlocks(next)
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
	if vfs.IsProjected(doc.MediaType()) {
		return "", vfs.ErrProjected
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
	if vfs.ContentToken(doc) != expected {
		return nil, vfs.ErrStaleContent
	}
	return doc, nil
}

func (v vfsTools) stage(ctx context.Context, doc vfs.Textual) (string, error) {
	if err := v.ms.WriteDocument(ctx, doc); err != nil {
		if errors.Is(err, vfs.ErrConflict) {
			return "", vfs.ErrStaleContent
		}
		return "", err
	}
	return fmt.Sprintf("path=%s rev=%s line_count=%d", doc.Path(), vfs.ContentToken(doc), doc.LineCount()), nil
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
