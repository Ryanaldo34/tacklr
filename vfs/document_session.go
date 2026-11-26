package vfs

import (
	"context"
)

// documentBackend is a provider that decodes its files into Document IR.
// MountSession routes OpenDocument here; a backend that does not implement
// it returns ErrNotSupported.
type documentBackend interface {
	OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error)
}

func decodeProviderDocument(ctx context.Context, name string, fi FileInfo, data []byte, reg *ContentRegistry) (Document, error) {
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	mt := normalizeMediaType(fi.MediaType)
	if mt == "" {
		return nil, ErrNoCodec
	}
	return reg.Decode(ctx, name, mt, data)
}

func bindDocument(doc Document, virtual string) Document {
	if d, ok := asIR(doc); ok {
		d.bindPath(virtual)
	}
	return doc
}
