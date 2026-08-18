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

// Encoder is the optional write side of a Codec. A codec that decodes a rich
// document into the canonical text projection can implement Encoder so the
// projection is encoded again during Sync.
type Encoder interface {
	Encode(ctx context.Context, doc Document) ([]byte, error)
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

// Decode looks up a codec for mediaType and decodes data.
func (r *ContentRegistry) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	r.mu.RLock()
	c, ok := r.codecs[mediaType]
	r.mu.RUnlock()
	if !ok {
		if !IsTextLike(mediaType) {
			return nil, ErrNoCodec
		}
		c = TextCodec{}
	}
	return c.Decode(ctx, path, mediaType, data)
}

// DefaultContentRegistry returns the process-wide registry with TextCodec
// registered. The top-level Tacklr harness additionally registers common rich
// text adapters during package initialization.
func DefaultContentRegistry() *ContentRegistry {
	return defaultContentRegistry
}

var defaultContentRegistry = func() *ContentRegistry {
	r := NewContentRegistry()
	_ = r.Register(TextCodec{})
	return r
}()

// DetectMediaType returns a best-effort media type for IR codec routing.
//
// Order:
//  1. Well-known extension map (source code and text formats)
//  2. If sample is non-empty: http.DetectContentType (+ UTF-8 text fallback)
//  3. application/octet-stream
//
// OpenDocument calls this after reading bytes; raw ReadFile does not.
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
