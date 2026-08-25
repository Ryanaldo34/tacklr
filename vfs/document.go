package vfs

import (
	"context"
	"strings"
)

// Document is the general content IR. Codecs produce Document values that are
// independent of storage backends (local, S3, Drive, …).
//
// Path is always a virtual path — never a host filesystem path or bucket key.
type Document interface {
	Path() string
	MediaType() string
}

// Textual is document content that has a plaintext form (source, Markdown,
// Engrams, later Word/Docs/PDF extracts). Images and other binaries do not
// implement it — callers use a comma-ok assert.
//
// Text() is that plaintext (FUSE / encode). Line numbers are
// 1-based. Lines(start, end) is half-open [start, end).
// SetText / SetLine / ReplaceLines mutate this value; persist with WriteDocument.
// SetText on Docs/Word applies HTML then SetBlocks; spreadsheets return ErrProjected.
type Textual interface {
	Document
	Encoding() string
	Text() string
	LineCount() int
	Line(n int) (string, error)
	Lines(start, end int) ([]string, error)
	SetText(text string) error
	SetLine(n int, line string) error
	ReplaceLines(start, end int, replacement []string) error
}

// Structured is optional for documents with a block tree (Markdown headings,
// Word, Google Docs, …). Callers use Blocks(); empty means no structure.
type Structured interface {
	Document
	Blocks() []Block
}

// Block kind vocabulary (stable for tools and brain props). Grow carefully.
const (
	BlockKindPreamble  = "preamble"
	BlockKindHeading   = "heading"
	BlockKindParagraph = "paragraph"
	BlockKindListItem  = "list_item"
	BlockKindTable     = "table"
	BlockKindImage     = "image"
	BlockKindSheet     = "sheet"
)

// StyleMeta is optional presentation/structure for rich documents.
type StyleMeta struct {
	Kind       string
	Level      int
	Span       Span
	Attributes map[string]string
}

// Span is a line-based content address (1-based half-open).
type Span struct {
	StartLine int
	EndLine   int
}

// Block is a structural unit (heading region, paragraph, …).
// For Markdown, blocks are a projected view over the textual body (not a second body).
//
// Text is what tools show the agent. On RichDocument, inline marks in Text are
// **bold**, _italic_ (also *italic* on input), ~~strike~~, and [label](url).
// kind/level carry structure — do not put # or - lists in Text.
// Runs is the decoded form; callers set Text and leave Runs empty.
type Block struct {
	ID    string
	Kind  string
	Text  string
	Runs  []Run
	Style StyleMeta
}

// FindBlock looks up by ID or Style.Attributes["heading_path"] (exact).
func FindBlock(blocks []Block, idOrPath string) (Block, bool) {
	idOrPath = strings.TrimSpace(idOrPath)
	if idOrPath == "" {
		return Block{}, false
	}
	for _, b := range blocks {
		if b.ID == idOrPath {
			return b, true
		}
		if b.Style.Attributes != nil && b.Style.Attributes["heading_path"] == idOrPath {
			return b, true
		}
	}
	return Block{}, false
}

// BlockReplaceSpan returns half-open 1-based lines to replace for a block.
// For headings, includeHeading false skips the heading line (body only).
func BlockReplaceSpan(b Block, includeHeading bool) (start, end int, err error) {
	start, end = b.Style.Span.StartLine, b.Style.Span.EndLine
	if start < 1 || end < start {
		return 0, 0, ErrLineOutOfRange
	}
	if b.Kind == BlockKindHeading && !includeHeading {
		start++
		if start > end {
			return start, end, nil // empty body
		}
	}
	return start, end, nil
}

// ir is the one document checkout. Codecs pick a body (text, blocks, or grid).
// MountSession and tools see Path, MediaType, Textual, Structured, and optional
// AsGrid / AsRich — not backends or concrete file types.
type IR struct {
	path, mediaType, encoding string
	body                      body
	hint                      persistHint
}

type body interface {
	text() string
	lineStarts() []int
	setText(string) error
	setLine(int, string) error
	replaceLines(int, int, []string) error
	blocks(mediaType string) []Block
	fingerprint() string
}

type textBody struct {
	payload string
	starts  []int
}

// NewTextDocument builds a plaintext checkout (identity body).
func NewTextDocument(path, mediaType, encoding, text string) *IR {
	if encoding == "" {
		encoding = "utf-8"
	}
	if mediaType == "" {
		mediaType = "text/plain"
	}
	d := &IR{path: path, mediaType: mediaType, encoding: encoding, body: &textBody{}}
	_ = d.SetText(text)
	return d
}

func newIR(path, mediaType, encoding string, b body) *IR {
	if encoding == "" {
		encoding = "utf-8"
	}
	if mediaType == "" {
		mediaType = "text/plain"
	}
	return &IR{path: path, mediaType: mediaType, encoding: encoding, body: b}
}

func asIR(doc Document) (*IR, bool) {
	d, ok := doc.(*IR)
	return d, ok
}

// Grid is the spreadsheet representation of a Document (Sheets / Excel).
type Grid interface {
	Sheets() []Sheet
	NamedRanges() []NamedRange
	Cell(key, a1 string) (Cell, error)
	ReadRows(key string, start, end int) (Sheet, []string, error)
	ReadCell(key, a1 string) (string, error)
	ReadRangeTSV(key, a1 string) (string, error)
}

// AsGrid reports whether doc is represented as a spreadsheet grid.
func AsGrid(doc Document) (Grid, bool) {
	d, ok := asIR(doc)
	if !ok {
		return nil, false
	}
	g, ok := d.body.(*gridBody)
	if !ok {
		return nil, false
	}
	return g, true
}

func asGridBody(doc Document) (*gridBody, bool) {
	d, ok := asIR(doc)
	if !ok {
		return nil, false
	}
	g, ok := d.body.(*gridBody)
	return g, ok
}

// Rich is the block-tree representation of a Document (Docs / Word).
type Rich interface {
	Tabs() []DocTab
	Blocks() []Block
	ReplaceBlock(id, text string, includeHeading bool) error
	SetBlocks(blocks []Block)
}

// AsRich reports whether doc is represented as a block tree (Docs / Word).
func AsRich(doc Document) (Rich, bool) {
	r, ok := asRichBody(doc)
	if !ok {
		return nil, false
	}
	return r, true
}

func asRichBody(doc Document) (*richBody, bool) {
	d, ok := asIR(doc)
	if !ok {
		return nil, false
	}
	r, ok := d.body.(*richBody)
	return r, ok
}

func EncodeDocument(ctx context.Context, doc Document) ([]byte, error) {
	if doc == nil {
		return nil, ErrNotTextual
	}
	if c, ok := defaultContentRegistry.codec(normalizeMediaType(doc.MediaType())); ok {
		if enc, ok := c.(Encoder); ok {
			return enc.Encode(ctx, doc)
		}
	}
	text, ok := doc.(Textual)
	if !ok {
		return nil, ErrNotTextual
	}
	return EncodeTextual(text)
}

func (d *IR) Path() string      { return d.path }
func (d *IR) MediaType() string { return d.mediaType }
func (d *IR) Encoding() string  { return d.encoding }
func (d *IR) Text() string      { return d.body.text() }
func (d *IR) LineCount() int    { return len(d.body.lineStarts()) }

func (d *IR) Blocks() []Block {
	return d.body.blocks(d.mediaType)
}

func (d *IR) Line(n int) (string, error) {
	starts := d.body.lineStarts()
	if n < 1 || n > len(starts) {
		return "", ErrLineOutOfRange
	}
	return lineAt(d.body.text(), starts, n-1), nil
}

func (d *IR) Lines(start, end int) ([]string, error) {
	starts := d.body.lineStarts()
	n := len(starts)
	if start < 1 || end < start || end > n+1 {
		return nil, ErrLineOutOfRange
	}
	if start == end {
		return []string{}, nil
	}
	out := make([]string, end-start)
	text := d.body.text()
	for i := range out {
		out[i] = lineAt(text, starts, start-1+i)
	}
	return out, nil
}

func (d *IR) SetText(text string) error {
	return d.body.setText(text)
}

func (d *IR) SetLine(n int, line string) error {
	return d.body.setLine(n, line)
}

func (d *IR) ReplaceLines(start, end int, replacement []string) error {
	return d.body.replaceLines(start, end, replacement)
}

func (d *IR) ContentFingerprint() string { return d.body.fingerprint() }

func (d *IR) bindPath(virtual string) { d.path = virtual }

func lineAt(text string, starts []int, i int) string {
	start := starts[i]
	if i+1 < len(starts) {
		return text[start : starts[i+1]-1]
	}
	return text[start:]
}

func (b *textBody) text() string        { return b.payload }
func (b *textBody) lineStarts() []int   { return b.starts }
func (b *textBody) fingerprint() string { return ContentHash(b.payload) }
func (b *textBody) blocks(mediaType string) []Block {
	return structureFor(mediaType, b.payload, b.starts)
}

func (b *textBody) setText(text string) error {
	b.payload = text
	b.starts = lineStartsOf(text)
	return nil
}

func (b *textBody) setLine(n int, line string) error {
	return b.replaceLines(n, n+1, []string{line})
}

func (b *textBody) replaceLines(start, end int, replacement []string) error {
	s, err := spliceLines(b.payload, b.starts, start, end, replacement)
	if err != nil {
		return err
	}
	return b.setText(s)
}

func spliceLines(text string, starts []int, start, end int, replacement []string) (string, error) {
	n := len(starts)
	if start < 1 || end < start || end > n+1 {
		return "", ErrLineOutOfRange
	}
	for _, line := range replacement {
		if strings.Contains(line, "\n") {
			return "", ErrInvalidLine
		}
	}
	if n == 0 {
		return strings.Join(replacement, "\n"), nil
	}

	prefixLines := start - 1
	suffixLines := n - (end - 1)
	moreAfterPrefix := len(replacement) > 0 || suffixLines > 0

	need := len(text) + 1
	for _, line := range replacement {
		need += len(line) + 1
	}
	var buf strings.Builder
	buf.Grow(need)

	if prefixLines > 0 {
		if prefixLines == n {
			buf.WriteString(text)
			if moreAfterPrefix {
				buf.WriteByte('\n')
			}
		} else if moreAfterPrefix {
			buf.WriteString(text[:starts[prefixLines]])
		} else {
			buf.WriteString(text[:starts[prefixLines]-1])
		}
	}
	for i, line := range replacement {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)
	}
	if suffixLines > 0 {
		if len(replacement) > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(text[starts[end-1]:])
	}
	return buf.String(), nil
}

var (
	_ Document   = (*IR)(nil)
	_ Textual    = (*IR)(nil)
	_ Structured = (*IR)(nil)
)
