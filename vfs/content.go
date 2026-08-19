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

// IdentityCodec is a Codec whose persist form is the UTF-8 payload itself
// (no container). TextCodec implements it. Office/cloud codecs (Word, Notion,
// Google Docs) must not — FUSE then returns EROFS and the write tool uses
// WriteDocument. Register those codecs under their native media types; do not
// steal text/markdown.
type IdentityCodec interface {
	Codec
	Identity()
}

// IsProjected reports whether mediaType is owned by a registered non-identity
// codec (DocsCodec today). PDF and unregistered types are not projected.
func IsProjected(mediaType string) bool {
	mediaType = normalizeMediaType(mediaType)
	if mediaType == "" || mediaType == "application/octet-stream" {
		return false
	}
	c, ok := defaultContentRegistry.codec(mediaType)
	if !ok {
		return false
	}
	_, id := c.(IdentityCodec)
	return !id
}

// KernelWritable reports whether FUSE/host writes may persist raw bytes for
// mediaType. True only when a registered IdentityCodec owns the type.
// Providers must set FileInfo.MediaType; unregistered types are EROFS.
func kernelWritable(mediaType string) bool {
	mediaType = normalizeMediaType(mediaType)
	if mediaType == "" || mediaType == "application/octet-stream" {
		return false
	}
	c, ok := defaultContentRegistry.codec(mediaType)
	if !ok {
		return false
	}
	_, ok = c.(IdentityCodec)
	return ok
}

// KernelWritableFile reports whether an existing file may take kernel writes.
// Empty MediaType is not writable — providers classify at Stat.
func kernelWritableFile(st FileInfo) bool {
	return kernelWritable(st.MediaType)
}

// KernelCreateOK reports whether a new name may be created via FUSE.
// Unknown types (temps, README) are allowed; a registered non-identity codec is not.
func kernelCreateOK(name string) bool {
	mt := DetectMediaType(name, nil)
	c, ok := defaultContentRegistry.codec(mt)
	if !ok {
		return true
	}
	_, id := c.(IdentityCodec)
	return id
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

// Register adds or replaces codec bindings for each of c.MediaTypes().
func (r *ContentRegistry) Register(c Codec) error {
	if c == nil {
		return fmt.Errorf("vfs: codec required")
	}
	types := c.MediaTypes()
	if len(types) == 0 {
		return fmt.Errorf("vfs: codec media types required")
	}
	for _, mt := range types {
		if mt == "" {
			return fmt.Errorf("vfs: codec media type required")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mt := range types {
		r.codecs[mt] = c
	}
	return nil
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
