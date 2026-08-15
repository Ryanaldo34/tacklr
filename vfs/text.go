package vfs

import (
	"context"
	"unicode/utf8"
	"unsafe"
)

// TextCodec decodes UTF-8 text and text-like media types into *TextDocument.
// Identity: persist form is the UTF-8 payload (no container).
type TextCodec struct{}

// Identity marks TextCodec as an IdentityCodec. FUSE may accept kernel writes.
func (TextCodec) Identity() {}

// MediaTypes is the set of extension-map types this codec claims at registration.
// Unregistered text-like types still fall back to TextCodec in ContentRegistry.Decode.
func (TextCodec) MediaTypes() []string {
	return textMediaTypes
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
	return NewTextDocument(path, mediaType, "utf-8", unsafe.String(unsafe.SliceData(data), len(data))), nil
}
