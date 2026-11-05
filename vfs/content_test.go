package vfs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// typesCodec is a host codec that claims the media types it lists. The
// registry checks the types when the host registers the codec.
type typesCodec []string

func (c typesCodec) MediaTypes() []string { return c }
func (typesCodec) Decode(context.Context, string, string, []byte) (vfs.Document, error) {
	return nil, errors.New("typesCodec decodes nothing")
}

// imageDocument is a document with no text form, as a host image codec
// returns it.
type imageDocument struct{ path string }

func (d imageDocument) Path() string    { return d.path }
func (imageDocument) MediaType() string { return "image/png" }

// The built-in codecs own the text types as identity (the raw bytes are the
// text). An empty or unregistered type is neither identity nor projected,
// and the text codec decodes an empty or octet-stream type as plain text.
// Markdown has structure and plain text has none. A document with no text
// and no codec does not encode, and a new file of blocks needs a codec with
// structure.
func TestContentRegistry_builtinsOwnTextTypes(t *testing.T) {
	t.Parallel()
	r, err := vfs.NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, mt := range []string{
		"text/plain", "text/markdown", "text/x-go", "application/json",
		"application/yaml", "text/plain; charset=utf-8",
	} {
		if !r.Identity(mt) {
			t.Fatalf("Identity(%q) = false, want plaintext writable", mt)
		}
	}
	for _, mt := range []string{
		"", "application/octet-stream", "image/png", adapters.DOCXMediaType,
		"application/vnd.google-apps.document", "application/vnd.notion.page",
	} {
		if r.Identity(mt) || r.Projected(mt) {
			t.Fatalf("Identity or Projected(%q) = true, want neither", mt)
		}
	}
	if c, ok := r.Lookup("text/plain"); !ok || c == nil {
		t.Fatal("no codec for text/plain")
	}
	doc, err := r.Decode(t.Context(), "/x.go", "text/x-go; charset=utf-8", []byte("package x\n"))
	if err != nil || doc.MediaType() != "text/x-go" {
		t.Fatalf("decoded %+v err=%v, want text/x-go", doc, err)
	}
	for _, mt := range []string{"", "application/octet-stream"} {
		doc, err := (vfs.TextCodec{}).Decode(t.Context(), "/x", mt, []byte("hi"))
		if err != nil || doc.MediaType() != "text/plain" {
			t.Fatalf("decode of %q = %+v err=%v, want text/plain", mt, doc, err)
		}
	}
	if !r.HasStructure("text/markdown") || r.HasStructure("text/plain") {
		t.Fatal("HasStructure: want Markdown only")
	}
	if _, err := r.Encode(t.Context(), imageDocument{path: "/x.png"}); !errors.Is(err, vfs.ErrNotTextual) {
		t.Fatalf("encode of an image = %v, want ErrNotTextual", err)
	}
	ms, err := vfs.Tree(vfs.At("w", vfs.Local(t.TempDir())))(t.Context(), t.Name(), vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Apply(t.Context(), "/workspace/w/new.txt", vfs.Mutation{Blocks: []vfs.Block{{Kind: "paragraph", Text: "x"}}}); err == nil || !strings.Contains(err.Error(), "structured codec") {
		t.Fatalf("new text file of blocks = %v, want a structured codec error", err)
	}
}

// DetectMediaType sniffs at most 512 bytes of a file with no known
// extension, and UTF-8 text with control bytes is still plain text.
func TestDetectMediaType_sniffsTextWithNoExtension(t *testing.T) {
	t.Parallel()
	for name, sample := range map[string][]byte{
		"a long sample":            []byte(strings.Repeat("a", 600) + "\x00"),
		"text with a control byte": []byte("hello\x01world"),
	} {
		if got := vfs.DetectMediaType("README", sample); got != "text/plain" {
			t.Errorf("%s = %q, want text/plain", name, got)
		}
	}
}

// A host codec with no codec value, no types or a blank type is refused. A type with no codec and a decode with a canceled
// context fail.
func TestNewContentRegistry_refusesBadCodecsAndUnknownTypes(t *testing.T) {
	t.Parallel()
	for name, codec := range map[string]vfs.Codec{
		"nil":      nil,
		"no types": typesCodec(nil),
		"blank":    typesCodec{""},
	} {
		if _, err := vfs.NewContentRegistry(codec); err == nil {
			t.Fatalf("%s codec registered", name)
		}
	}
	r, err := vfs.NewContentRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Decode(t.Context(), "/x.bin", "application/x-unknown", []byte(`{}`)); !errors.Is(err, vfs.ErrNoCodec) {
		t.Fatalf("unregistered decode = %v, want ErrNoCodec", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (vfs.TextCodec{}).Decode(canceled, "/x", "text/plain", []byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled decode = %v", err)
	}
}

// A BlockCodec reports its types and creates documents of its first type. It
// fails without a normalizer, when the normalizer cannot decode the bytes,
// and when it encodes a document that is not rich.
func TestBlockCodec_createsItsTypeAndFailsWithoutAUsableNormalizer(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	empty := vfs.BlockCodec{Types: []string{adapters.DOCXMediaType}}
	docx := vfs.BlockCodec{Types: []string{adapters.DOCXMediaType}, Normalizer: adapters.DOCX{}}

	created, createErr := empty.Create("/x", "", vfs.Mutation{})
	_, emptyDecodeErr := empty.Decode(ctx, "/x", adapters.DOCXMediaType, nil)
	_, emptyEncodeErr := empty.Encode(ctx, vfs.NewRichDocument("/x", adapters.DOCXMediaType, nil))
	_, badZipErr := docx.Decode(ctx, "/x", adapters.DOCXMediaType, []byte("not a zip"))
	_, textEncodeErr := docx.Encode(ctx, vfs.NewTextDocument("/t", "text/plain", "utf-8", "plain"))
	_, imageEncodeErr := docx.Encode(ctx, imageDocument{path: "/x"})

	if got := docx.MediaTypes(); len(got) != 1 || got[0] != adapters.DOCXMediaType {
		t.Fatalf("MediaTypes = %v", got)
	}
	if createErr != nil || created.MediaType() != adapters.DOCXMediaType {
		t.Fatalf("Create = %+v err=%v, want a %s document", created, createErr, adapters.DOCXMediaType)
	}
	if emptyDecodeErr == nil || emptyEncodeErr == nil || badZipErr == nil || textEncodeErr == nil {
		t.Fatalf("errors: decode without normalizer=%v, encode without normalizer=%v, bad zip=%v, encode text=%v",
			emptyDecodeErr, emptyEncodeErr, badZipErr, textEncodeErr)
	}
	if !errors.Is(imageEncodeErr, vfs.ErrNotTextual) {
		t.Fatalf("encode of an image = %v, want ErrNotTextual", imageEncodeErr)
	}
}
