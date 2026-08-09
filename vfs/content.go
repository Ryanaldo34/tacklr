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
	if r == nil || c == nil {
		return fmt.Errorf("vfs: register requires registry and codec")
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

// Lookup returns the codec for mediaType, or TextCodec for text-like types, or ErrNoCodec.
func (r *ContentRegistry) Lookup(mediaType string) (Codec, error) {
	if r == nil {
		return nil, fmt.Errorf("vfs: content registry required")
	}
	r.mu.RLock()
	c, ok := r.codecs[mediaType]
	r.mu.RUnlock()
	if ok {
		return c, nil
	}
	if isTextLike(mediaType) {
		return TextCodec{}, nil
	}
	return nil, ErrNoCodec
}

// Decode looks up a codec for mediaType and decodes data.
func (r *ContentRegistry) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := r.Lookup(mediaType)
	if err != nil {
		return nil, err
	}
	return c.Decode(ctx, path, mediaType, data)
}

// DefaultContentRegistry returns the process-wide registry with TextCodec registered.
// Safe for concurrent use; do not mutate after first use unless the host owns that policy.
func DefaultContentRegistry() *ContentRegistry {
	return defaultContentRegistry()
}

var defaultContentRegistry = sync.OnceValue(func() *ContentRegistry {
	r := NewContentRegistry()
	_ = r.Register(TextCodec{})
	return r
})

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
	if utf8.Valid(sample) && !bytesContainNUL(sample) {
		return "text/plain"
	}
	return "application/octet-stream"
}

func bytesContainNUL(b []byte) bool {
	return bytes.IndexByte(b, 0) >= 0
}

// isTextLike reports whether an unregistered media type may still use TextCodec.
func isTextLike(mediaType string) bool {
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

// mediaTypesFromExtMap returns unique media types from the extension map (cached).
func mediaTypesFromExtMap() []string {
	return textCodecMediaTypes()
}

var textCodecMediaTypes = sync.OnceValue(func() []string {
	seen := make(map[string]struct{}, len(extMediaTypes))
	out := make([]string, 0, len(extMediaTypes))
	for _, mt := range extMediaTypes {
		if _, ok := seen[mt]; ok {
			continue
		}
		seen[mt] = struct{}{}
		out = append(out, mt)
	}
	return out
})

func errFileExceeds(limit int) error {
	return fmt.Errorf("%w (max %d bytes)", ErrTooLarge, limit)
}
