package vfs

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
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

func createDocument(path, mediaType string, mut Mutation) (Document, error) {
	if c, ok := defaultContentRegistry.codec(mediaType); ok {
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

// codecKind is one registry lookup. identity is true for IdentityCodec owners.
// Empty, octet-stream, and unregistered types are neither identity nor projected.
func codecKind(mediaType string) (identity, registered bool) {
	mediaType = normalizeMediaType(mediaType)
	if mediaType == "" || mediaType == "application/octet-stream" {
		return false, false
	}
	c, ok := defaultContentRegistry.codec(mediaType)
	if !ok {
		return false, false
	}
	_, id := c.(IdentityCodec)
	return id, true
}

func identityBody(mediaType string) bool {
	id, _ := codecKind(mediaType)
	return id
}

// IsProjected reports whether mediaType is a registered rich/grid codec.
// Projected bodies are not FUSE raw-byte writes; identity/text bodies are.
func IsProjected(mediaType string) bool {
	id, ok := codecKind(mediaType)
	return ok && !id
}

func kernelWritable(mediaType string) bool { return identityBody(mediaType) }

func kernelWritableFile(st FileInfo) bool {
	return identityBody(st.MediaType)
}

// KernelCreateOK reports whether a new name may be created via FUSE.
// Unknown types (temps, README) are allowed; projected codecs are not.
func kernelCreateOK(name string) bool {
	id, ok := codecKind(DetectMediaType(name, nil))
	return !ok || id
}

// ContentRegistry maps media type → Codec (process-scoped, like BackendRegistry).
type ContentRegistry struct {
	mu     sync.RWMutex
	codecs map[string]Codec
}

// NewContentRegistry returns an empty content registry.
func NewContentRegistry() *ContentRegistry {
	return &ContentRegistry{codecs: make(map[string]Codec)}
}

// Register binds c for each of c.MediaTypes(). It does not replace an existing
// binding; first registration wins.
func (r *ContentRegistry) Register(c Codec) error {
	if c == nil {
		return fmt.Errorf("vfs: codec required")
	}
	types := c.MediaTypes()
	if len(types) == 0 {
		return fmt.Errorf("vfs: codec media types required")
	}
	normalized := make([]string, 0, len(types))
	for _, mt := range types {
		mt = normalizeMediaType(mt)
		if mt == "" {
			return fmt.Errorf("vfs: codec media type required")
		}
		normalized = append(normalized, mt)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mt := range normalized {
		if _, exists := r.codecs[mt]; exists {
			return fmt.Errorf("%w: %s", ErrAlreadyRegistered, mt)
		}
	}
	for _, mt := range normalized {
		r.codecs[mt] = c
	}
	return nil
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
	if r == nil {
		return nil, false
	}
	mediaType = normalizeMediaType(mediaType)
	r.mu.RLock()
	c, ok := r.codecs[mediaType]
	r.mu.RUnlock()
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

// DefaultContentRegistry returns the process-wide registry with TextCodec registered.
func DefaultContentRegistry() *ContentRegistry {
	return defaultContentRegistry
}

var defaultContentRegistry = mustDefaultContentRegistry(textMediaTypes)

func mustDefaultContentRegistry(types []string) *ContentRegistry {
	if len(types) == 0 {
		panic("vfs: text media types required before default registry init")
	}
	r := NewContentRegistry()
	if err := r.Register(TextCodec{}); err != nil {
		panic(err)
	}
	if err := r.Register(DocsCodec{}); err != nil {
		panic(err)
	}
	if err := r.Register(SheetsCodec{}); err != nil {
		panic(err)
	}
	return r
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

// textMediaTypes is the unique set of extension-map media types for TextCodec
// registration. Computed as a package var (not init) so DefaultContentRegistry
// can register TextCodec during variable initialization.
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
