package vfs

// Document is the general content IR. Codecs produce Document values that are
// independent of storage backends (local, S3, Drive, …).
//
// Path is always a virtual path — never a host filesystem path or bucket key.
type Document interface {
	Path() string
	MediaType() string
}

// Textual is document content that has a plaintext form (source, Markdown,
// Engrams, Word, Google Docs). Images and other binaries do not
// implement it — callers use a comma-ok assert.
//
// Text() is that plaintext (FUSE / encode). Line numbers are
// 1-based. Lines(start, end) is half-open [start, end).
type Textual interface {
	Document
	Encoding() string
	Text() string
	LineCount() int
	Line(n int) (string, error)
	Lines(start, end int) ([]string, error)
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

// ir is the one document checkout. Codecs pick a body (text, blocks, or grid).
// MountSession and tools see Path, MediaType, Textual, Structured, and optional
// AsGrid / AsRich — not backends or concrete file types.
type IR struct {
	path, mediaType, encoding string
	body                      body
}

type body interface {
	text() string
	lineStarts() []int
	blocks(mediaType string) []Block
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
	return &IR{path: path, mediaType: mediaType, encoding: encoding, body: &textBody{payload: text, starts: lineStartsOf(text)}}
}

// newIR is a UTF-8 checkout of body b.
func newIR(path, mediaType string, b body) *IR {
	if mediaType == "" {
		mediaType = "text/plain"
	}
	return &IR{path: path, mediaType: mediaType, encoding: "utf-8", body: b}
}

func asIR(doc Document) (*IR, bool) {
	d, ok := doc.(*IR)
	return d, ok
}

// Grid is the spreadsheet representation of a Document (Sheets / Excel).
type Grid interface {
	Sheets() []Sheet
	NamedRanges() []NamedRange
}

// AsGrid reports whether doc is represented as a spreadsheet grid.
func AsGrid(doc Document) (Grid, bool) {
	g, ok := asGridBody(doc)
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

func (d *IR) bindPath(virtual string) { d.path = virtual }

func lineAt(text string, starts []int, i int) string {
	start := starts[i]
	if i+1 < len(starts) {
		return text[start : starts[i+1]-1]
	}
	return text[start:]
}

func (b *textBody) text() string      { return b.payload }
func (b *textBody) lineStarts() []int { return b.starts }
func (b *textBody) blocks(mediaType string) []Block {
	return structureFor(mediaType, b.payload, b.starts)
}
