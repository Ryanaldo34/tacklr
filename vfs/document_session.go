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

// WriteDocument asks the provider for the document path to translate IR and persist.
func (m *MountSession) WriteDocument(ctx context.Context, doc Document) error {
	t, ok := doc.(Textual)
	if !ok {
		return ErrNotTextual
	}
	return m.Route(ctx, t.Path()).WriteDocument(ctx, doc)
}

func bindDocument(doc Document, virtual string) Document {
	if d, ok := asIR(doc); ok {
		d.bindPath(virtual)
	}
	return doc
}
