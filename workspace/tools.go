package workspace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/vfs"
)

const smallFormatCells = 32

// vfsTools is the thin tool adapter over MountSession.Apply / ReadLines / ReadText.
type vfsTools struct {
	ms *vfs.MountSession
}

type readArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path."`
	Start   int    `json:"start,omitempty" desc:"1-based first line (inclusive). Sheets: first row of the sheet in block_id. Ignored when block_id is a cell or A1 range."`
	End     int    `json:"end,omitempty" desc:"1-based last line (exclusive). Sheets: last row (exclusive). Ignored when block_id is a cell or A1 range."`
	BlockID string `json:"block_id,omitempty" desc:"Markdown heading path, Docs/Word block id, sheet name, Sheet!A1, or Sheet!A1:C3."`
	Outline bool   `json:"outline,omitempty" desc:"List headings, Docs/Word blocks and tabs, or sheets and named ranges."`
}

type writeTextArgs struct {
	Path       string   `json:"path" desc:"Absolute virtual path."`
	Content    *string  `json:"content,omitempty" desc:"Full file body. Creates the path if missing."`
	Start      *int     `json:"start,omitempty" desc:"1-based first line to replace (inclusive). Use with end and lines or body."`
	End        *int     `json:"end,omitempty" desc:"1-based last line to replace (exclusive)."`
	Lines      []string `json:"lines,omitempty" desc:"Replacement lines with no embedded newlines. Empty deletes the span."`
	Body       *string  `json:"body,omitempty" desc:"Replacement text split on newlines when lines is empty."`
	Old        *string  `json:"old,omitempty" desc:"Exact substring that must still be in the current file. Must be unique unless replace_all."`
	New        *string  `json:"new,omitempty" desc:"Replacement for old. Omit to delete the substring."`
	ReplaceAll bool     `json:"replace_all,omitempty" desc:"Replace every old match. Default is one unique match."`
}

type writeDocumentArgs struct {
	Path    string   `json:"path" desc:"Absolute virtual path."`
	Content *string  `json:"content,omitempty" desc:"Full UTF-8 HTML body. Creates the path if missing. Extensionless HTML becomes a native Doc/Word on Drive/Graph."`
	Start   *int     `json:"start,omitempty" desc:"1-based first HTML line to replace (inclusive). Use with end and lines or body."`
	End     *int     `json:"end,omitempty" desc:"1-based last HTML line to replace (exclusive)."`
	Lines   []string `json:"lines,omitempty" desc:"Replacement HTML lines with no embedded newlines. Empty deletes the span."`
	Body    *string  `json:"body,omitempty" desc:"Replacement text split on newlines when lines is empty."`
	BlockID string   `json:"block_id,omitempty" desc:"Block id from read outline=true."`
	TabID   string   `json:"tab_id,omitempty" desc:"Required when the document has more than one tab."`
}

type writeSpreadsheetArgs struct {
	Path    string           `json:"path" desc:"Absolute virtual path."`
	BlockID string           `json:"block_id,omitempty" desc:"Existing spreadsheet: Sheet!A1 (one cell)."`
	Body    *string          `json:"body,omitempty" desc:"Cell value for block_id Sheet!A1."`
	Lines   []string         `json:"lines,omitempty" desc:"Cell value as lines when body is empty."`
	Format  *vfs.FormatPatch `json:"format,omitempty" desc:"Cell format on Sheet!A1. Omit a field to leave it. Value-only writes leave format; format-only writes leave the value."`
	Content *string          `json:"content,omitempty" desc:"Create a new spreadsheet only. TSV/CSV body on a path that does not exist yet."`
}

func (v vfsTools) newRead() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "read",
		DisplayName: "Read {path}",
		Description: `Read a virtual file. Call to inspect contents, headings, or a cell range before editing.

Path-only returns the first page of numbered lines (Docs/Word as HTML: one block per line) plus media_type, start, end, eof, and next_start. The N| prefix is display only — it is not in the file. Spreadsheets with no window default to outline.

Fails if the path is missing, the range is invalid, or block_id is unknown.`,
		Category: message.ToolCategoryRead,
		Access:   tacklr.ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler:  v.read,
	})
}

func (v vfsTools) readStructured(ctx context.Context, p string, args readArgs) (string, error) {
	doc, err := v.ms.Route(ctx, p).
		ReadText(ctx)
	if err != nil {
		return "", vfsToolErr("read", p, err)
	}
	projected := v.ms.Codecs().Projected(doc.MediaType())
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	var b strings.Builder
	b.Grow(128 + len(p) + len(doc.MediaType()))
	td, tabular := vfs.AsGrid(doc)
	if tabular {
		fmt.Fprintf(&b, "path=%s media_type=%s line_count=%d sheet_count=%d\n",
			p, doc.MediaType(), doc.LineCount(), len(td.Sheets()))
	} else {
		fmt.Fprintf(&b, "path=%s media_type=%s line_count=%d\n",
			p, doc.MediaType(), doc.LineCount())
	}
	if rd, ok := vfs.AsRich(doc); ok && args.Outline {
		if tabs := rd.Tabs(); len(tabs) > 0 {
			b.WriteString("tabs:\n")
			for _, tb := range tabs {
				fmt.Fprintf(&b, "  %s title=%q index=%d\n", tb.ID, tb.Title, tb.Index)
			}
		}
	}
	if tabular && args.Outline {
		if named := td.NamedRanges(); len(named) > 0 {
			b.WriteString("named_ranges:\n")
			for _, n := range named {
				fmt.Fprintf(&b, "  %s sheet_id=%s a1=%s\n", n.Name, n.SheetID, n.A1)
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
	if tabular {
		out, err := readTabular(td, args, &b)
		if err != nil {
			return "", vfsToolErr("read", p, err)
		}
		return out, nil
	}
	start, end := args.Start, args.End
	if args.BlockID != "" {
		if len(blocks) == 0 {
			return "", tacklr.Correctionf(vfs.ErrUnknownBlock, "read %s: this file has no structured blocks. Read the path without block_id (Docs/Word return numbered HTML lines)", p)
		}
		bl, ok := vfs.FindBlock(blocks, args.BlockID)
		if !ok {
			return "", vfsToolErr("read", p, fmt.Errorf("%w %q", vfs.ErrUnknownBlock, args.BlockID))
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
			return "", vfsToolErr("read", p, err)
		}
		growLineWindow(&b, 64, win.Lines)
		fmt.Fprintf(&b, "start=%d end=%d returned=%d eof=%v next_start=%d\n",
			win.Start, win.End, win.Returned, win.EOF, win.NextStart)
		for i, line := range win.Lines {
			fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
		}
	}
	return b.String(), nil
}

// readTabular reads a sheet window, a cell or an A1 range. read checked the
// window; A1 is parsed once, here.
func readTabular(td vfs.Grid, args readArgs, b *strings.Builder) (string, error) {
	if args.BlockID == "" {
		return b.String(), nil
	}
	sheetKey, a1 := vfs.SplitSheetAddr(args.BlockID)
	if a1 != "" {
		r1, c1, r2, c2, err := vfs.ParseA1(a1)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(b, "block_id=%s\n", args.BlockID)
		if r1 == r2 && c1 == c2 {
			cell, err := td.Cell(sheetKey, a1)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(b, "%s\n", formatToolCell(cell))
			return b.String(), nil
		}
		tsv, err := td.ReadRangeTSV(sheetKey, a1)
		if err != nil {
			return "", err
		}
		b.WriteString(tsv)
		if tsv != "" && !strings.HasSuffix(tsv, "\n") {
			b.WriteByte('\n')
		}
		if (r2-r1+1)*(c2-c1+1) <= smallFormatCells {
			writeRangeFormats(b, td, sheetKey, r1, c1, r2, c2)
		}
		return b.String(), nil
	}
	start, end := args.Start, args.End
	sh, lines, err := td.ReadRows(sheetKey, start, end)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(b, "sheet=%s rows=%d cols=%d start=%d\n", sh.Title, sh.Rows, sh.Cols, start)
	growLineWindow(b, 0, lines)
	for i, line := range lines {
		fmt.Fprintf(b, "%6d|%s\n", start+i, line)
	}
	return b.String(), nil
}

func formatToolCell(c vfs.Cell) string {
	s := "text=" + c.Display()
	if f := c.Format.String(); f != "" {
		s += " format=" + f
	}
	return s
}

// writeRangeFormats lists the formatted cells of a range the caller already
// read, so its sheet exists and each address is one valid cell.
func writeRangeFormats(b *strings.Builder, td vfs.Grid, sheet string, r1, c1, r2, c2 int) {
	for r := r1; r <= r2; r++ {
		for c := c1; c <= c2; c++ {
			cell, _ := td.Cell(sheet, vfs.FormatA1(r, c))
			if cell.Format.IsZero() {
				continue
			}
			fmt.Fprintf(b, "%s format=%s\n", vfs.FormatA1(r, c), cell.Format.String())
		}
	}
}

const (
	writeToolName            = "write"
	writeDocumentToolName    = "write_document"
	writeSpreadsheetToolName = "write_spreadsheet"
)

func (v vfsTools) newWrite() *tacklr.Tool {
	cfg := tacklr.ToolConfig{
		Name:        writeToolName,
		DisplayName: "Write {path}",
		Description: `Create or edit a plaintext or source file. Call when the path is new or its media_type is text or code. Pass exactly one change (content, old/new, or start/end). Do not copy the N| prefix from a prior read.

May pause for write permission. On success returns path, line_count, and replacements when a substitution ran. Fails if nothing to change, more than one change is passed, old is missing, or the existing file is a document or spreadsheet.`,
		Category: message.ToolCategoryEdit,
		Access:   tacklr.ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler:  v.write,
	}
	return v.withWritePermission(cfg)
}

func (v vfsTools) newWriteDocument() *tacklr.Tool {
	cfg := tacklr.ToolConfig{
		Name:        writeDocumentToolName,
		DisplayName: "Write document {path}",
		Description: `Create or edit a Google Doc or Word file. Call when the path is new or its media_type is a document. Pass exactly one change.

May pause for write permission. On success returns path, line_count, and an outline of blocks. Fails if nothing to change, more than one change is passed, or the existing file is plaintext or a spreadsheet.`,
		Category: message.ToolCategoryEdit,
		Access:   tacklr.ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler:  v.writeDocument,
	}
	return v.withWritePermission(cfg)
}

func (v vfsTools) newWriteSpreadsheet() *tacklr.Tool {
	cfg := tacklr.ToolConfig{
		Name:        writeSpreadsheetToolName,
		DisplayName: "Write spreadsheet {path}",
		Description: `Create a spreadsheet or edit one cell of an existing sheet.

May pause for write permission. On success returns path and line_count. Fails if content is passed for an existing sheet, block_id is missing on an existing sheet, or the existing file is not a spreadsheet.`,
		Category: message.ToolCategoryEdit,
		Access:   tacklr.ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler:  v.writeSpreadsheet,
	}
	return v.withWritePermission(cfg)
}

// withWritePermission gates a write tool on the user's permission
// (AgentOptions.AllowTools grants it).
func (v vfsTools) withWritePermission(cfg tacklr.ToolConfig) *tacklr.Tool {
	cfg.OnCall = []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}
	return tacklr.NewTool(cfg)
}

func (v vfsTools) applyWrite(ctx context.Context, tool, p string, mut vfs.Mutation, cc *tacklr.CallContext) (string, error) {
	cc.EmitUpdate("Writing " + p)
	res, err := v.ms.Apply(ctx, p, mut)
	if err != nil {
		return "", vfsToolErr(tool, p, err)
	}
	return formatWriteResult(res), nil
}

// requireWriteFamily allows create (path missing). An existing file must match tool.
func (v vfsTools) requireWriteFamily(ctx context.Context, p, tool string) (exists bool, err error) {
	fi, err := v.ms.Route(ctx, p).
		Stat(ctx)
	if errors.Is(err, vfs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, vfsToolErr(tool, p, err)
	}
	want := writeToolName
	switch {
	case isTabularMedia(fi.MediaType):
		want = writeSpreadsheetToolName
	case v.ms.Codecs().Projected(fi.MediaType):
		want = writeDocumentToolName
	}
	if want != tool {
		return true, tacklr.Correctionf(vfs.ErrProjected, "%s is %s. Use %s, not %s", p, fi.MediaType, want, tool)
	}
	return true, nil
}

// vfsToolErr maps usage mistakes at the VFS tool, not in the turn processor.
func vfsToolErr(name, path string, err error) error {
	switch {
	case errors.Is(err, vfs.ErrInvalidWrite), errors.Is(err, vfs.ErrConflict):
		return tacklr.Correctionf(vfs.ErrInvalidWrite, "%s was not saved. Some of the content may already be in the file. Read the file, then write the full HTML again", path)
	case errors.Is(err, vfs.ErrStaleContent):
		return tacklr.Correctionf(vfs.ErrStaleContent, "%s changed since you last read it. Read the file again, then write", path)
	case errors.Is(err, vfs.ErrUseHTML):
		return tacklr.Correctionf(vfs.ErrUseHTML, "%s is a document. Pass content as HTML, for example <h1>Title</h1> and <p>paragraphs</p>", path)
	case errors.Is(err, vfs.ErrEmptyReplace):
		return tacklr.Correctionf(vfs.ErrEmptyReplace, "%s: %s", path, vfs.ErrEmptyReplace.Error())
	case errors.Is(err, vfs.ErrTabIDRequired):
		return tacklr.Correctionf(vfs.ErrTabIDRequired, "%s: %s", path, vfs.ErrTabIDRequired.Error())
	case errors.Is(err, vfs.ErrProjected):
		return tacklr.Correctionf(vfs.ErrProjected, "%s: that write is not supported on this file type. Use write for plaintext, write_document for Docs/Word, write_spreadsheet for Sheet!A1", path)
	case errors.Is(err, vfs.ErrNotExist):
		return tacklr.Correctionf(vfs.ErrNotExist, "%s: that path does not exist. List the parent with run_command (ls) or read a known path, then retry", name)
	case errors.Is(err, vfs.ErrInvalidPath):
		return tacklr.Correctionf(vfs.ErrInvalidPath, "%s: that path is not a valid virtual path. Use an absolute path under a mount (for example /workspace/documents/Name)", name)
	case errors.Is(err, vfs.ErrIsDir):
		return tacklr.Correctionf(vfs.ErrIsDir, "%s: that path is a directory. Read a file inside it, or list it with run_command ls", name)
	case errors.Is(err, vfs.ErrNotDir):
		return tacklr.Correctionf(vfs.ErrNotDir, "%s: that path is a file, not a directory", name)
	case errors.Is(err, vfs.ErrAuthExpired):
		// The harness parks the call until the client refreshes them.
		return fmt.Errorf("%s: cloud credentials expired: %w: %w", name, tacklr.ErrAuthExpired, err)
	case errors.Is(err, vfs.ErrPermission):
		return tacklr.Correctionf(vfs.ErrPermission, "%s: permission denied on that path. Use a path the session is allowed to access", name)
	case errors.Is(err, vfs.ErrReadOnly):
		return tacklr.Correctionf(vfs.ErrReadOnly, "%s: that mount is read-only. Read the file, or write under a writable mount", name)
	case errors.Is(err, vfs.ErrUnknownBlock):
		return tacklr.Correctionf(vfs.ErrUnknownBlock, "%s: %v. Read the path with outline=true and use an id from that outline", name, err)
	case errors.Is(err, vfs.ErrInvalidA1):
		return tacklr.Correctionf(vfs.ErrInvalidA1, "%s: %v. Use Sheet!A1 or Sheet!A1:C3", name, err)
	case errors.Is(err, vfs.ErrLineOutOfRange):
		return tacklr.Correctionf(vfs.ErrLineOutOfRange, "%s: that line range is outside the file. Read the path without start/end to see line_count, then request a window that fits", name)
	case errors.Is(err, vfs.ErrInvalidLine):
		return tacklr.Correctionf(vfs.ErrInvalidLine, "%s: a replacement line contained a newline. Pass each line as its own array entry, with no embedded \\n", name)
	case errors.Is(err, vfs.ErrTooLarge):
		return tacklr.Correctionf(vfs.ErrTooLarge, "%s: that payload is too large. Write a smaller window or split the document", name)
	case errors.Is(err, vfs.ErrNotTextual), errors.Is(err, vfs.ErrNoCodec):
		return tacklr.Correctionf(vfs.ErrNotTextual, "%s: that file is not text. Use a different path, or write a text/HTML document", name)
	default:
		return err
	}
}

func formatWriteResult(res vfs.ApplyResult) string {
	s := fmt.Sprintf("path=%s line_count=%d", res.Path, res.LineCount)
	if res.Replacements > 0 {
		s += fmt.Sprintf(" replacements=%d", res.Replacements)
	}
	if len(res.Outline) == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 32*len(res.Outline))
	b.WriteString(s)
	b.WriteString("\noutline:\n")
	for _, bl := range res.Outline {
		fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
			bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
	}
	return b.String()
}

func isTabularMedia(mt string) bool {
	return strings.Contains(mt, "spreadsheet")
}

func lineWindowFromTextDoc(doc vfs.Textual, start, end int) (vfs.LineWindow, error) {
	n := doc.LineCount()
	eof := end > n+1
	end = min(end, n+1)
	// A start past the end is out of range.
	lines, err := doc.Lines(start, end)
	if err != nil {
		return vfs.LineWindow{}, err
	}
	return vfs.LineWindow{
		Start: start, End: end, Lines: lines,
		Returned: len(lines), EOF: eof, NextStart: start + len(lines),
	}, nil
}

func growLineWindow(b *strings.Builder, extra int, lines []string) {
	n := extra
	for _, line := range lines {
		n += 8 + len(line)
	}
	b.Grow(n)
}

func (v vfsTools) read(ctx context.Context, args readArgs, cc *tacklr.CallContext) (string, error) {
	p, err := vfs.CleanPath(args.Path)
	if err != nil {
		return "", vfsToolErr("read", args.Path, err)
	}
	cc.EmitUpdate("Reading " + p)

	fi, err := v.ms.Route(ctx, p).
		Stat(ctx)
	if err != nil {
		return "", vfsToolErr("read", p, err)
	}
	projected := v.ms.Codecs().Projected(fi.MediaType)

	// A window the model passed is checked once, here, for every
	// file kind; a read without one gets the first page.
	explicitWindow := args.Start > 0 || args.End > 0
	if explicitWindow && (args.Start < 1 || args.End < args.Start) {
		return "", tacklr.Correctionf(vfs.ErrLineOutOfRange, "read: invalid range start=%d end=%d. Use 1-based half-open start/end, or omit them to read the first page, or pass block_id / outline", args.Start, args.End)
	}
	pathOnly := !explicitWindow && args.BlockID == "" && !args.Outline
	if pathOnly && projected && isTabularMedia(fi.MediaType) {
		args.Outline = true
	} else if !explicitWindow && (args.BlockID != "" || !args.Outline) {
		args.Start = 1
		args.End = 1 + vfs.MaxLinesPerWindow
	}

	if args.BlockID != "" || args.Outline {
		return v.readStructured(ctx, p, args)
	}

	if projected && isTabularMedia(fi.MediaType) {
		return "", vfsToolErr("read", p, vfs.ErrProjected)
	}

	win, err := v.ms.Route(ctx, p).
		ReadLines(ctx, args.Start, args.End)
	if err != nil {
		return "", vfsToolErr("read", p, err)
	}
	var b strings.Builder
	growLineWindow(&b, 96+len(win.Path)+len(fi.MediaType), win.Lines)
	fmt.Fprintf(&b, "path=%s media_type=%s start=%d end=%d returned=%d eof=%v next_start=%d\n",
		win.Path, fi.MediaType, win.Start, win.End, win.Returned, win.EOF, win.NextStart)
	for i, line := range win.Lines {
		fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
	}
	return b.String(), nil
}

func (v vfsTools) write(ctx context.Context, args writeTextArgs, cc *tacklr.CallContext) (string, error) {
	p, err := vfs.CleanPath(args.Path)
	if err != nil {
		return "", vfsToolErr(writeToolName, args.Path, err)
	}
	if _, err := v.requireWriteFamily(ctx, p, writeToolName); err != nil {
		return "", err
	}
	n := 0
	if args.Content != nil {
		n++
	}
	if args.Old != nil {
		n++
	}
	if args.Start != nil {
		n++
	}
	switch {
	case n == 0:
		return "", fmt.Errorf("write %s: nothing to change. Pass content, or old and new, or start and end with lines", p)
	case n > 1:
		return "", fmt.Errorf("write %s: pass only one change: content, or old/new, or start/end lines", p)
	}
	return v.applyWrite(ctx, writeToolName, p, vfs.Mutation{
		Content:    args.Content,
		Old:        args.Old,
		New:        args.New,
		ReplaceAll: args.ReplaceAll,
		Start:      args.Start,
		End:        args.End,
		Lines:      args.Lines,
		Body:       args.Body,
	}, cc)
}

func (v vfsTools) writeDocument(ctx context.Context, args writeDocumentArgs, cc *tacklr.CallContext) (string, error) {
	p, err := vfs.CleanPath(args.Path)
	if err != nil {
		return "", vfsToolErr(writeDocumentToolName, args.Path, err)
	}
	if _, err := v.requireWriteFamily(ctx, p, writeDocumentToolName); err != nil {
		return "", err
	}
	n := 0
	if args.Content != nil {
		n++
	}
	if args.Start != nil {
		n++
	}
	if args.BlockID != "" {
		n++
	}
	switch {
	case n == 0:
		return "", fmt.Errorf("write_document %s: nothing to change. Pass content (HTML), or start and end with lines, or block_id", p)
	case n > 1:
		return "", fmt.Errorf("write_document %s: pass only one change: content, or start/end lines, or block_id", p)
	}
	return v.applyWrite(ctx, writeDocumentToolName, p, vfs.Mutation{
		Content: args.Content,
		Start:   args.Start,
		End:     args.End,
		Lines:   args.Lines,
		Body:    args.Body,
		BlockID: args.BlockID,
		TabID:   args.TabID,
	}, cc)
}

func (v vfsTools) writeSpreadsheet(ctx context.Context, args writeSpreadsheetArgs, cc *tacklr.CallContext) (string, error) {
	p, err := vfs.CleanPath(args.Path)
	if err != nil {
		return "", vfsToolErr(writeSpreadsheetToolName, args.Path, err)
	}
	exists, err := v.requireWriteFamily(ctx, p, writeSpreadsheetToolName)
	if err != nil {
		return "", err
	}
	hasContent := args.Content != nil
	hasCell := args.BlockID != ""
	switch {
	case hasContent && hasCell:
		return "", tacklr.Correctionf(tacklr.ErrInvalid, "write_spreadsheet %s: pass only one change: content on a new path, or block_id Sheet!A1 on an existing sheet", p)
	case !exists && !hasContent:
		return "", tacklr.Correctionf(tacklr.ErrInvalid, "write_spreadsheet %s: create a new spreadsheet with content, or write an existing sheet cell with block_id Sheet!A1", p)
	case exists && hasContent:
		return "", tacklr.Correctionf(vfs.ErrProjected, "%s already exists. write_spreadsheet edits one cell with block_id Sheet!A1; it cannot replace an existing sheet in place", p)
	case exists && !hasCell:
		return "", tacklr.Correctionf(tacklr.ErrInvalid, "write_spreadsheet %s: nothing to change. Pass block_id Sheet!A1 with body and/or format", p)
	}
	return v.applyWrite(ctx, writeSpreadsheetToolName, p, vfs.Mutation{
		Content: args.Content,
		BlockID: args.BlockID,
		Body:    args.Body,
		Lines:   args.Lines,
		Format:  args.Format,
	}, cc)
}
