package vfs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
)

// Mutation is one write against a virtual path. Exactly one mode must be set:
// Content, Old, BlockID, Start, or Blocks.
type Mutation struct {
	Rev            string
	Content        *string
	Old            *string
	New            *string
	ReplaceAll     bool
	Start, End     *int
	Lines          []string
	Body           *string
	BlockID        string
	IncludeHeading bool
	Blocks         []Block
	TabID          string
	MediaType      string
	Format         *FormatPatch
}

// ApplyResult is the post-write identity of the document.
type ApplyResult struct {
	Path         string
	Rev          string
	LineCount    int
	Replacements int
}

func (r ApplyResult) String() string {
	s := fmt.Sprintf("path=%s rev=%s line_count=%d", r.Path, r.Rev, r.LineCount)
	if r.Replacements > 0 {
		s += fmt.Sprintf(" replacements=%d", r.Replacements)
	}
	return s
}

func (m Mutation) modeCount() (n int, full bool) {
	full = m.Content != nil
	if full {
		n++
	}
	if m.Old != nil {
		n++
	}
	if m.BlockID != "" {
		n++
	}
	if m.Start != nil && m.BlockID == "" {
		n++
	}
	if m.Blocks != nil {
		n++
	}
	return n, full
}

// Apply persists one mutation. Rev is required when the path exists.
func (ms *MountSession) Apply(ctx context.Context, virtualPath string, mut Mutation) (ApplyResult, error) {
	p, err := CleanPath(virtualPath)
	if err != nil {
		return ApplyResult{}, err
	}
	n, full := mut.modeCount()
	switch {
	case n == 0:
		return ApplyResult{}, fmt.Errorf("vfs: no mutation")
	case n > 1:
		return ApplyResult{}, fmt.Errorf("vfs: exactly one mutation mode")
	}

	fi, err := ms.Stat(ctx, p)
	exists := err == nil
	if err != nil && !errors.Is(err, ErrNotExist) {
		return ApplyResult{}, err
	}
	if exists {
		if strings.TrimSpace(mut.Rev) == "" {
			return ApplyResult{}, fmt.Errorf("vfs: rev required when path exists")
		}
	} else if !full && mut.Blocks == nil {
		return ApplyResult{}, ErrNotExist
	}

	switch {
	case full:
		return ms.applyFull(ctx, p, exists, fi, mut)
	case mut.Blocks != nil:
		return ms.applyBlocks(ctx, p, exists, fi, mut)
	case mut.Old != nil:
		return ms.applySubstring(ctx, p, mut)
	case mut.BlockID != "":
		return ms.applyBlock(ctx, p, mut)
	default:
		return ms.applyLines(ctx, p, mut)
	}
}

func (ms *MountSession) applyFull(ctx context.Context, p string, exists bool, fi FileInfo, mut Mutation) (ApplyResult, error) {
	body := ""
	if mut.Content != nil {
		body = *mut.Content
	}
	if exists {
		if IsProjected(fi.MediaType) {
			return ApplyResult{}, ErrProjected
		}
		cur, err := ms.ContentRev(ctx, p)
		if err != nil {
			return ApplyResult{}, err
		}
		if cur.Hash != mut.Rev {
			return ApplyResult{}, ErrStaleContent
		}
	}
	if len(body) > MaxReadFileBytes {
		return ApplyResult{}, ErrTooLarge
	}
	if exists {
		return ms.stage(ctx, NewTextDocument(p, fi.MediaType, "utf-8", body))
	}
	n := min(len(body), 512)
	mt := createMediaType(p, mut.MediaType, []byte(body[:n]))
	doc, err := createDocument(p, mt, mut)
	if err != nil {
		return ApplyResult{}, err
	}
	text, ok := doc.(Textual)
	if !ok {
		return ApplyResult{}, ErrNotTextual
	}
	return ms.stage(ctx, text)
}

func createMediaType(p, requested string, sample []byte) string {
	if path.Ext(p) != "" {
		return DetectMediaType(path.Base(p), sample)
	}
	if requested != "" {
		return requested
	}
	return DetectMediaType(path.Base(p), sample)
}

func looksLikeHTML(s string) bool {
	t := strings.TrimSpace(s)
	return strings.HasPrefix(t, "<") && strings.Contains(t, ">")
}

func (ms *MountSession) applySubstring(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	if *mut.Old == "" {
		return ApplyResult{}, fmt.Errorf("vfs: old is required")
	}
	doc, err := ms.loadMatching(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if IsProjected(doc.MediaType()) {
		return ApplyResult{}, ErrProjected
	}
	repl := ""
	if mut.New != nil {
		repl = *mut.New
	}
	body := doc.Text()
	n := strings.Count(body, *mut.Old)
	switch {
	case n == 0:
		return ApplyResult{}, fmt.Errorf("vfs: old text not found")
	case !mut.ReplaceAll && n != 1:
		return ApplyResult{}, fmt.Errorf("vfs: old text occurs %d times (need unique match or replace_all)", n)
	}
	if mut.ReplaceAll {
		if err := doc.SetText(strings.ReplaceAll(body, *mut.Old, repl)); err != nil {
			return ApplyResult{}, err
		}
	} else if err := doc.SetText(strings.Replace(body, *mut.Old, repl, 1)); err != nil {
		return ApplyResult{}, err
	}
	out, err := ms.stage(ctx, doc)
	if err != nil {
		return ApplyResult{}, err
	}
	out.Replacements = n
	return out, nil
}

func (ms *MountSession) applyBlock(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	doc, err := ms.loadMatching(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if g, ok := asGridBody(doc); ok {
		d, _ := asIR(doc)
		return ms.applyTabularBlock(ctx, d, g, mut)
	}
	var blocks []Block
	if s, ok := doc.(Structured); ok {
		blocks = s.Blocks()
	}
	bl, ok := FindBlock(blocks, mut.BlockID)
	if !ok {
		return ApplyResult{}, fmt.Errorf("vfs: unknown block_id %q", mut.BlockID)
	}
	if mut.TabID != "" && bl.Style.Attributes != nil {
		if got := bl.Style.Attributes["tab_id"]; got != "" && got != mut.TabID {
			return ApplyResult{}, fmt.Errorf("vfs: tab_id %q does not match block %q", mut.TabID, got)
		}
	}
	if rd, ok := AsRich(doc); ok {
		text := strings.Join(replacementLines(mut.Lines, mut.Body), "\n")
		if err := rd.ReplaceBlock(bl.ID, text, mut.IncludeHeading); err != nil {
			return ApplyResult{}, err
		}
		return ms.stage(ctx, doc)
	}
	start, end, err := BlockReplaceSpan(bl, mut.IncludeHeading)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := doc.ReplaceLines(start, end, replacementLines(mut.Lines, mut.Body)); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, doc)
}

func (ms *MountSession) applyTabularBlock(ctx context.Context, td *IR, g *gridBody, mut Mutation) (ApplyResult, error) {
	sheetKey, a1 := SplitSheetAddr(mut.BlockID)
	idx, ok := g.findSheet(sheetKey)
	if !ok {
		return ApplyResult{}, fmt.Errorf("vfs: unknown block_id %q", mut.BlockID)
	}
	if mut.TabID != "" && g.sheets[idx].ID != "" && g.sheets[idx].ID != mut.TabID {
		return ApplyResult{}, fmt.Errorf("vfs: tab_id %q does not match block %q", mut.TabID, g.sheets[idx].ID)
	}
	hasValue := mut.Body != nil || len(mut.Lines) > 0
	var format *FormatPatch
	if !mut.Format.empty() {
		format = mut.Format
	}
	if !hasValue && format == nil {
		return ApplyResult{}, fmt.Errorf("vfs: no mutation")
	}
	if a1 == "" {
		return ApplyResult{}, fmt.Errorf("%w: sheet write requires Sheet!A1", ErrNotSupported)
	}
	r1, c1, r2, c2, err := parseA1(a1)
	if err != nil {
		return ApplyResult{}, err
	}
	if r1 != r2 || c1 != c2 {
		return ApplyResult{}, fmt.Errorf("%w: sheet write is one cell (Sheet!A1)", ErrNotSupported)
	}
	var input *string
	if hasValue {
		text := strings.Join(replacementLines(mut.Lines, mut.Body), "\n")
		input = &text
	}
	if err := g.overlayCell(idx, r1, c1, input, format); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, td)
}

func (ms *MountSession) applyBlocks(ctx context.Context, p string, exists bool, fi FileInfo, mut Mutation) (ApplyResult, error) {
	next := make([]Block, 0, len(mut.Blocks))
	for _, b := range mut.Blocks {
		attrs := map[string]string{}
		for k, v := range b.Style.Attributes {
			attrs[k] = v
		}
		if mut.TabID != "" {
			attrs["tab_id"] = mut.TabID
		}
		b.Style.Attributes = attrs
		next = append(next, b)
	}
	if !exists {
		mt := createMediaType(p, mut.MediaType, nil)
		created := mut
		created.Blocks = next
		doc, err := createDocument(p, mt, created)
		if err != nil {
			return ApplyResult{}, err
		}
		text, ok := doc.(Textual)
		if !ok {
			return ApplyResult{}, ErrNotTextual
		}
		return ms.stage(ctx, text)
	}
	if !IsProjected(fi.MediaType) {
		return ApplyResult{}, ErrProjected
	}
	if len(next) == 0 {
		return ApplyResult{}, fmt.Errorf("vfs: refusing empty IR replace")
	}
	doc, err := ms.loadMatching(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if _, ok := asGridBody(doc); ok {
		return ApplyResult{}, fmt.Errorf("%w: sheet replace uses create (content or blocks on a new path)", ErrNotSupported)
	}
	rd, ok := AsRich(doc)
	if !ok {
		return ApplyResult{}, ErrProjected
	}
	d, _ := asIR(doc)
	tabs := rd.Tabs()
	if len(tabs) > 1 && mut.TabID == "" {
		return ApplyResult{}, fmt.Errorf("vfs: tab_id required")
	}
	if mut.TabID != "" && len(tabs) > 0 {
		var keep []Block
		for _, b := range rd.Blocks() {
			tab := ""
			if b.Style.Attributes != nil {
				tab = b.Style.Attributes["tab_id"]
			}
			if tab != mut.TabID {
				keep = append(keep, b)
			}
		}
		next = append(next, keep...)
	}
	rd.SetBlocks(next)
	return ms.stage(ctx, d)
}

func (ms *MountSession) applyLines(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	if mut.End == nil || *mut.Start < 1 || *mut.End < *mut.Start {
		return ApplyResult{}, fmt.Errorf("vfs: invalid range start=%d end=%v", *mut.Start, mut.End)
	}
	doc, err := ms.loadMatching(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if IsProjected(doc.MediaType()) {
		return ApplyResult{}, ErrProjected
	}
	if err := doc.ReplaceLines(*mut.Start, *mut.End, replacementLines(mut.Lines, mut.Body)); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, doc)
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

func (ms *MountSession) loadMatching(ctx context.Context, p, expected string) (Textual, error) {
	doc, err := ms.ReadText(ctx, p)
	if err != nil {
		return nil, err
	}
	if ContentToken(doc) != expected {
		return nil, ErrStaleContent
	}
	return doc, nil
}

func (ms *MountSession) stage(ctx context.Context, doc Textual) (ApplyResult, error) {
	if err := ms.WriteDocument(ctx, doc); err != nil {
		if errors.Is(err, ErrConflict) {
			return ApplyResult{}, ErrStaleContent
		}
		return ApplyResult{}, err
	}
	return ApplyResult{Path: doc.Path(), Rev: ContentToken(doc), LineCount: doc.LineCount()}, nil
}
