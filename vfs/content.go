package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path"
	"strings"
	"unicode/utf8"
)

// Codec decodes raw bytes into a Document IR.
// mediaType is chosen by the caller (typically DetectMediaType); codecs must not re-sniff.
type Codec interface {
	MediaTypes() []string
	Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error)
}

// Encoder is the optional write side of a Codec. Rich document codecs use it
// to encode the edited canonical projection back to source bytes.
type Encoder interface {
	Encode(ctx context.Context, doc Document) ([]byte, error)
}

// Creator builds a new document from a create-mode Mutation.
// Codecs that own a representation (blocks, grid) implement this.
// MountSession looks up the codec by media type; it does not switch on vendors.
type Creator interface {
	Create(path, mediaType string, mut Mutation) (Document, error)
}

func (r *ContentRegistry) create(path, mediaType string, mut Mutation) (Document, error) {
	if c, ok := r.codec(mediaType); ok {
		if cr, ok := c.(Creator); ok {
			return cr.Create(path, mediaType, mut)
		}
	}
	if mut.Blocks != nil {
		return nil, fmt.Errorf("vfs: blocks require a structured codec")
	}
	body := ""
	if mut.Content != nil {
		body = *mut.Content
	}
	return NewTextDocument(path, mediaType, "utf-8", body), nil
}

// IdentityCodec is a Codec whose persist form is the UTF-8 payload itself
// (no container). TextCodec implements it. Office/cloud codecs (Word, Notion,
// Google Docs) must not — FUSE then returns EROFS and the write tool uses
// WriteDocument. Register those codecs under their native media types; do not
// steal text/markdown.
type IdentityCodec interface {
	Codec
	Identity()
}

// ContentRegistry maps media types to the codecs a MountSession reads and
// writes documents with. It is immutable: NewContentRegistry builds one, and
// a host adds codecs to its tree with OpenVFS.WithCodecs.
type ContentRegistry struct {
	codecs map[string]Codec
}

// builtinCodecs is TextCodec for the text types, which every registry has.
// A host adds the rest with OpenVFS.WithCodecs (package vfs/adapters has
// Word and Excel); a mounted backend adds the codecs of its own native
// types (codecSource).
var builtinCodecs = textRegistry(textMediaTypes)

// textRegistry takes the text types as an argument so they are initialized
// first.
func textRegistry(textTypes []string) *ContentRegistry {
	r := &ContentRegistry{codecs: make(map[string]Codec, len(textTypes))}
	for _, mt := range textTypes {
		r.codecs[mt] = TextCodec{}
	}
	return r
}

// codecSource is a backend with native file types of its own (Drive's
// Google Docs and Sheets). Mounting it adds those codecs to the session.
type codecSource interface {
	codecs() []Codec
}

// adding returns r plus the codecs of types r does not have yet, so two
// mounts of one backend add its codecs once.
func (r *ContentRegistry) adding(codecs []Codec) *ContentRegistry {
	out := &ContentRegistry{codecs: maps.Clone(r.codecs)}
	for _, c := range codecs {
		for _, mt := range c.MediaTypes() {
			if _, ok := out.codecs[mt]; !ok {
				out.codecs[mt] = c
			}
		}
	}
	return out
}

// NewContentRegistry returns TextCodec for the text types plus codecs. Two
// codecs for one media type is ErrAlreadyRegistered.
func NewContentRegistry(codecs ...Codec) (*ContentRegistry, error) {
	return builtinCodecs.with(codecs...)
}

// with returns r plus codecs, leaving r as it is.
func (r *ContentRegistry) with(codecs ...Codec) (*ContentRegistry, error) {
	out := &ContentRegistry{codecs: maps.Clone(r.codecs)}
	for _, c := range codecs {
		if c == nil {
			return nil, errors.New("vfs: codec required")
		}
		types := c.MediaTypes()
		if len(types) == 0 {
			return nil, errors.New("vfs: codec media types required")
		}
		for _, mt := range types {
			mt = normalizeMediaType(mt)
			if mt == "" {
				return nil, errors.New("vfs: codec media type required")
			}
			if _, exists := out.codecs[mt]; exists {
				return nil, fmt.Errorf("%w: %s", ErrAlreadyRegistered, mt)
			}
			out.codecs[mt] = c
		}
	}
	return out, nil
}

// kind is one lookup: identity is true for IdentityCodec owners. Empty,
// octet-stream, and unregistered types are neither identity nor projected.
func (r *ContentRegistry) kind(mediaType string) (identity, registered bool) {
	mediaType = normalizeMediaType(mediaType)
	if mediaType == "" || mediaType == "application/octet-stream" {
		return false, false
	}
	c, ok := r.codecs[mediaType]
	if !ok {
		return false, false
	}
	_, id := c.(IdentityCodec)
	return id, true
}

// Projected reports whether mediaType has a rich or grid codec. Projected
// bodies are not raw-byte writes (FUSE, shell); identity/text bodies are.
func (r *ContentRegistry) Projected(mediaType string) bool {
	id, ok := r.kind(mediaType)
	return ok && !id
}

// Identity reports whether mediaType's stored bytes are its text: an
// IdentityCodec owns it, so raw byte writes (FUSE, shell) are safe.
func (r *ContentRegistry) Identity(mediaType string) bool {
	id, _ := r.kind(mediaType)
	return id
}

// HasStructure reports whether documents of mediaType expose a block tree
// (Structured.Blocks): Markdown headings, or a projected rich or grid codec.
func (r *ContentRegistry) HasStructure(mediaType string) bool {
	return normalizeMediaType(mediaType) == "text/markdown" || r.Projected(mediaType)
}

// Encode is doc as the bytes its codec stores, or its text when the codec
// does not encode.
func (r *ContentRegistry) Encode(ctx context.Context, doc Document) ([]byte, error) {
	if doc == nil {
		return nil, ErrNotTextual
	}
	if c, ok := r.codec(doc.MediaType()); ok {
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

// Lookup returns the codec bound to mediaType, if any.
func (r *ContentRegistry) Lookup(mediaType string) (Codec, bool) {
	return r.codec(mediaType)
}

func normalizeMediaType(mediaType string) string {
	if i := strings.IndexByte(mediaType, ';'); i >= 0 {
		mediaType = mediaType[:i]
	}
	return strings.ToLower(strings.TrimSpace(mediaType))
}

func (r *ContentRegistry) codec(mediaType string) (Codec, bool) {
	c, ok := r.codecs[normalizeMediaType(mediaType)]
	return c, ok
}

// Decode looks up a codec for mediaType and decodes data.
func (r *ContentRegistry) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	mediaType = normalizeMediaType(mediaType)
	c, ok := r.codec(mediaType)
	if !ok {
		return nil, ErrNoCodec
	}
	return c.Decode(ctx, path, mediaType, data)
}

// DetectMediaType is a helper for providers filling FileInfo.MediaType.
// OpenDocument does not call this — it trusts the provider.
//
// Order:
//  1. Well-known extension map (source code and text formats)
//  2. If sample is non-empty: http.DetectContentType (+ UTF-8 text fallback)
//  3. application/octet-stream
func DetectMediaType(virtualPath string, sample []byte) string {
	if ext := path.Ext(virtualPath); ext != "" {
		if mt, ok := extMediaTypes[strings.ToLower(ext)]; ok {
			return mt
		}
	}
	if len(sample) > 0 {
		return sniffBytes(sample)
	}
	return "application/octet-stream"
}

var extMediaTypes = map[string]string{
	".txt": "text/plain", ".md": "text/markdown", ".markdown": "text/markdown",
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".go":   "text/x-go", ".py": "text/x-python",
	".js": "text/javascript", ".mjs": "text/javascript", ".cjs": "text/javascript", ".jsx": "text/javascript",
	".ts": "text/x.typescript", ".tsx": "text/x.typescript",
	".json": "application/json", ".yaml": "application/yaml", ".yml": "application/yaml", ".toml": "application/toml",
	".rs": "text/x-rust", ".java": "text/x-java-source",
	".c": "text/x-c", ".h": "text/x-c", ".cpp": "text/x-c", ".cc": "text/x-c", ".cs": "text/x-csharp",
	".rb": "text/x-ruby", ".php": "text/x-php",
	".sh": "text/x-shellscript", ".bash": "text/x-shellscript",
	".css": "text/css", ".html": "text/html", ".htm": "text/html",
	".xml": "application/xml", ".sql": "application/sql", ".csv": "text/csv",
}

func sniffBytes(sample []byte) string {
	if len(sample) > 512 {
		sample = sample[:512]
	}
	mt := http.DetectContentType(sample)
	if i := strings.IndexByte(mt, ';'); i >= 0 {
		mt = strings.TrimSpace(mt[:i])
	}
	if mt != "" && mt != "application/octet-stream" {
		if strings.HasPrefix(mt, "text/") && !utf8.Valid(sample) {
			return "application/octet-stream"
		}
		return mt
	}
	// DetectContentType often returns octet-stream for plain source without BOM.
	if utf8.Valid(sample) && bytes.IndexByte(sample, 0) < 0 {
		return "text/plain"
	}
	return "application/octet-stream"
}

// IsTextLike reports whether mediaType is treated as text for IR/index routing.
func IsTextLike(mediaType string) bool {
	if strings.HasPrefix(mediaType, "text/") {
		return true
	}
	switch mediaType {
	case "application/json", "application/yaml", "application/toml",
		"application/xml", "application/sql":
		return true
	default:
		return false
	}
}

// textMediaTypes is the unique set of extension-map media types TextCodec
// owns.
var textMediaTypes = uniqueMediaTypes(extMediaTypes)

func uniqueMediaTypes(m map[string]string) []string {
	seen := make(map[string]struct{}, len(m))
	out := make([]string, 0, len(m))
	for _, mt := range m {
		if !IsTextLike(mt) {
			continue
		}
		if _, ok := seen[mt]; ok {
			continue
		}
		seen[mt] = struct{}{}
		out = append(out, mt)
	}
	return out
}

func errFileExceeds(limit int) error {
	return fmt.Errorf("%w (max %d bytes)", ErrTooLarge, limit)
}
