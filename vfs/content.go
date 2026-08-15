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

// IdentityCodec is a Codec whose persist form is the UTF-8 payload itself
// (no container). TextCodec implements it. Office/cloud codecs (Word, Notion,
// Google Docs) must not — FUSE then returns EROFS and the write tool uses
// WriteDocument. Register those codecs under their native media types; do not
// steal text/markdown.
type IdentityCodec interface {
	Codec
	Identity()
}

// KernelWritable reports whether FUSE/host writes may persist raw bytes for
// mediaType. True only when Decode would use an IdentityCodec (TextCodec, or
// the unregistered text-like fallback). Projected documents stay EROFS.
func KernelWritable(mediaType string) bool {
	if i := strings.IndexByte(mediaType, ';'); i >= 0 {
		mediaType = strings.TrimSpace(mediaType[:i])
	}
	if mediaType == "" || mediaType == "application/octet-stream" {
		return false
	}
	c, ok := defaultContentRegistry.codec(mediaType)
	if !ok {
		return IsTextLike(mediaType)
	}
	_, ok = c.(IdentityCodec)
	return ok
}

// KernelWritableFile reports whether an existing file may take kernel writes.
func KernelWritableFile(st FileInfo) bool {
	mt := st.MediaType
	if mt == "" {
		mt = DetectMediaType(st.Name, nil)
	}
	return KernelWritable(mt)
}

// KernelCreateOK reports whether a new name may be created via FUSE.
// Unknown types (temps, README) are allowed; a registered non-identity codec is not.
func KernelCreateOK(name string) bool {
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

func (r *ContentRegistry) codec(mediaType string) (Codec, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	c, ok := r.codecs[mediaType]
	r.mu.RUnlock()
	return c, ok
}

// Decode looks up a codec for mediaType and decodes data.
func (r *ContentRegistry) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	c, ok := r.codec(mediaType)
	if !ok {
		if !IsTextLike(mediaType) {
			return nil, ErrNoCodec
		}
		c = TextCodec{}
	}
	return c.Decode(ctx, path, mediaType, data)
}

// DefaultContentRegistry returns the process-wide registry with TextCodec registered.
func DefaultContentRegistry() *ContentRegistry {
	return defaultContentRegistry
}

var defaultContentRegistry = func() *ContentRegistry {
	r := NewContentRegistry()
	_ = r.Register(TextCodec{})
	return r
}()

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
	".go": "text/x-go", ".py": "text/x-python",
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

// textMediaTypes is the unique set of extension-map media types for TextCodec registration.
var textMediaTypes []string

func init() {
	seen := make(map[string]struct{}, len(extMediaTypes))
	textMediaTypes = make([]string, 0, len(extMediaTypes))
	for _, mt := range extMediaTypes {
		if _, ok := seen[mt]; ok {
			continue
		}
		seen[mt] = struct{}{}
		textMediaTypes = append(textMediaTypes, mt)
	}
}

func errFileExceeds(limit int) error {
	return fmt.Errorf("%w (max %d bytes)", ErrTooLarge, limit)
}
