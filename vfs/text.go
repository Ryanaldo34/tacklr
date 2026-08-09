package vfs

import (
	"context"
	"unicode/utf8"
	"unsafe"
)

// TextCodec decodes UTF-8 text and text-like media types into *TextDocument.
type TextCodec struct{}

// MediaTypes lists types this codec registers (derived from the extension map).
// Lookup also falls back to TextCodec for any unregistered text-like type.
func (TextCodec) MediaTypes() []string {
	return mediaTypesFromExtMap()
}

// Decode builds a TextDocument from data. mediaType is the caller's detection result.
//
// data is taken over as the document body (no extra content copy). Callers must
// not mutate data after Decode returns.
func (TextCodec) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, ErrInvalidUTF8
	}
	if mediaType == "" || mediaType == "application/octet-stream" {
		mediaType = "text/plain"
	}
	return NewTextDocument(path, mediaType, "utf-8", bytesToStringOwned(data)), nil
}

// bytesToStringOwned reinterprets b as a string without copying. The Document
// becomes the sole owner; b must not be written again.
func bytesToStringOwned(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
