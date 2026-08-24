package vfs

import (
	"context"
	"errors"
	"testing"
)

func TestKernelWritable(t *testing.T) {
	for _, mt := range []string{
		"text/plain", "text/markdown", "text/x-go", "application/json",
		"application/yaml", "text/plain; charset=utf-8",
	} {
		if !kernelWritable(mt) {
			t.Fatalf("kernelWritable(%q) = false, want plaintext writable", mt)
		}
	}
	for _, mt := range []string{
		"", "application/octet-stream", "image/png",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.google-apps.document", "application/vnd.google-apps.spreadsheet",
		"application/vnd.notion.page",
	} {
		if kernelWritable(mt) {
			t.Fatalf("kernelWritable(%q) = true, want EROFS", mt)
		}
	}
	if err := DefaultContentRegistry().Register(kernelProjectedCodec{}); err != nil {
		t.Fatal(err)
	}
	if kernelWritable("application/x-test-projected") {
		t.Fatal("registered non-identity codec must not be kernel-writable")
	}
	if !kernelWritableFile(FileInfo{Name: "a.go", MediaType: "text/x-go"}) {
		t.Fatal("kernelWritableFile go")
	}
	if kernelWritableFile(FileInfo{Name: "pic.png", MediaType: "image/png"}) {
		t.Fatal("kernelWritableFile png")
	}
	if kernelWritableFile(FileInfo{Name: "a.go"}) {
		t.Fatal("kernelWritableFile empty media type")
	}
	doc, err := DefaultContentRegistry().Decode(context.Background(), "/x.go", "text/x-go; charset=utf-8", []byte("package x\n"))
	if err != nil {
		t.Fatalf("default registry must own extension-map types: %v", err)
	}
	if doc.MediaType() != "text/x-go" {
		t.Fatalf("decoded media type %q", doc.MediaType())
	}
	if !kernelCreateOK("README") || !kernelCreateOK("note.txt") {
		t.Fatal("kernelCreateOK plaintext")
	}
}

func TestContentRegistry_registerDoesNotReplace(t *testing.T) {
	r := NewContentRegistry()
	if err := r.Register(TextCodec{}); err != nil {
		t.Fatal(err)
	}
	err := r.Register(stealPlainCodec{})
	if !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("overwrite: %v", err)
	}
	doc, err := r.Decode(context.Background(), "/a.txt", "text/plain", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	txt, ok := doc.(Textual)
	if !ok || txt.Text() != "hello" {
		t.Fatalf("text codec still owns text/plain: %T %v", doc, err)
	}
	if _, ok := r.Lookup("text/plain"); !ok {
		t.Fatal("lookup text/plain")
	}
}

type stealPlainCodec struct{}

func (stealPlainCodec) MediaTypes() []string { return []string{"text/plain"} }
func (stealPlainCodec) Decode(_ context.Context, _, _ string, _ []byte) (Document, error) {
	return nil, errors.New("stolen")
}

type kernelProjectedCodec struct{}

func (kernelProjectedCodec) MediaTypes() []string { return []string{"application/x-test-projected"} }
func (kernelProjectedCodec) Decode(_ context.Context, p, mt string, data []byte) (Document, error) {
	return NewTextDocument(p, mt, "utf-8", string(data)), nil
}
