package vfs

import (
	"context"
	"strings"
)

// OpenDocument loads a virtual path and decodes it into a Document IR.
//
// reg may be nil, in which case DefaultContentRegistry() is used.
// Prefer ReadLines when tools only need a line window (no full IR).
func (m *MountSession) OpenDocument(ctx context.Context, virtualPath string, reg *ContentRegistry) (Document, error) {
	if reg == nil {
		reg = DefaultContentRegistry()
	}
	data, err := m.ReadFile(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	sample := data
	if len(sample) > 512 {
		sample = sample[:512]
	}
	return reg.Decode(ctx, virtualPath, DetectMediaType(virtualPath, sample), data)
}

// ReadText opens a virtual path as a *TextDocument (full body + line index).
func (m *MountSession) ReadText(ctx context.Context, virtualPath string) (*TextDocument, error) {
	doc, err := m.OpenDocument(ctx, virtualPath, nil)
	if err != nil {
		return nil, err
	}
	td, ok := doc.(*TextDocument)
	if !ok {
		return nil, ErrNotTextual
	}
	return td, nil
}

// WriteDocument encodes a textual Document and writes it to doc.Path().
//
// Streams the body via strings.NewReader (no intermediate full []byte when the
// provider supports PutFile). Size capped at MaxReadFileBytes.
//
//	text, _ := ms.ReadText(ctx, path)
//	_ = text.SetLine(2, "changed")
//	_ = ms.WriteDocument(ctx, text)
func (m *MountSession) WriteDocument(ctx context.Context, doc Document) error {
	t, err := AsTextual(doc)
	if err != nil {
		return err
	}
	body := t.Text()
	return m.writeContents(ctx, t.Path(), strings.NewReader(body), int64(len(body)))
}
