package vfs

import (
	"context"
)

// documentBackend is optional: providers that can translate Document IR.
// MountSession routes OpenDocument / WriteDocument here. A backend that
// does not implement it returns ErrNotSupported — the session never encodes.
type documentBackend interface {
	OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error)
	WriteDocument(ctx context.Context, name string, doc Document) error
}

// EncodeTextual returns UTF-8 bytes for a Textual document.
// Backend providers call this when their native form is a file or object.
// MountSession does not encode.
func EncodeTextual(t Textual) ([]byte, error) {
	body, err := textualPayload(t)
	if err != nil {
		return nil, err
	}
	return []byte(body), nil
}

func textualPayload(t Textual) (string, error) {
	body := t.Text()
	if len(body) > MaxReadFileBytes {
		return "", errFileExceeds(MaxReadFileBytes)
	}
	return body, nil
}

func decodeProviderDocument(ctx context.Context, name string, fi FileInfo, data []byte, reg *ContentRegistry) (Document, error) {
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	if reg == nil {
		reg = DefaultContentRegistry()
	}
	mt := normalizeMediaType(fi.MediaType)
	if mt == "" {
		return nil, ErrNoCodec
	}
	return reg.Decode(ctx, name, mt, data)
}

// OpenDocument loads a virtual path into a Document IR via the mount's provider.
// The returned Textual is a fresh decode (safe to edit). reg nil uses DefaultContentRegistry().
func (m *MountSession) OpenDocument(ctx context.Context, virtualPath string, reg *ContentRegistry) (Document, error) {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return nil, err
	}
	p, rel, err := m.at(ctx, cleaned, false)
	if err != nil {
		return nil, err
	}
	db, ok := p.(documentBackend)
	if !ok {
		return nil, ErrNotSupported
	}
	if reg == nil {
		reg = DefaultContentRegistry()
	}
	doc, err := db.OpenDocument(ctx, rel, reg)
	if err != nil {
		return nil, err
	}
	return bindDocument(doc, cleaned), nil
}

// ReadText opens a virtual path as Textual IR (clone; safe to edit).
func (m *MountSession) ReadText(ctx context.Context, virtualPath string) (Textual, error) {
	doc, err := m.OpenDocument(ctx, virtualPath, nil)
	if err != nil {
		return nil, err
	}
	t, ok := doc.(Textual)
	if !ok {
		return nil, ErrNotTextual
	}
	return t, nil
}

// WriteDocument asks the mount's provider to translate IR and persist now.
func (m *MountSession) WriteDocument(ctx context.Context, doc Document) error {
	t, ok := doc.(Textual)
	if !ok {
		return ErrNotTextual
	}
	cleaned, err := cleanVirtualPath(t.Path())
	if err != nil {
		return err
	}
	p, rel, err := m.at(ctx, cleaned, true)
	if err != nil {
		return err
	}
	db, ok := p.(documentBackend)
	if !ok {
		return ErrNotSupported
	}
	if err := db.WriteDocument(ctx, rel, doc); err != nil {
		return err
	}
	return m.fireAfterPersist(ctx, cleaned)
}

func bindDocument(doc Document, virtual string) Document {
	switch d := doc.(type) {
	case *TextDocument:
		d.path = virtual
	case *RichDocument:
		d.path = virtual
	case *TabularDocument:
		d.path = virtual
	}
	return doc
}

func encodeDocument(_ context.Context, doc Document, _ *ContentRegistry) (data []byte, persistType string, err error) {
	t, ok := doc.(Textual)
	if !ok {
		return nil, "", ErrNotTextual
	}
	body, err := textualPayload(t)
	return []byte(body), t.MediaType(), err
}
