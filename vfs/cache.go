package vfs

import (
	"strings"
	"sync"
	"time"
)

// ponytail: fixed caps; tune if sessions thrash network backends.
const (
	maxCacheEntries = 32
	maxCacheBytes   = 64 << 20 // 64 MiB
)

// contentCache is session-local textual IR with write-back dirty tracking.
type contentCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	bytes   int64
}

type cacheEntry struct {
	doc     *TextDocument
	size    int64
	modTime time.Time
	dirty   bool
}

func newContentCache() *contentCache {
	return &contentCache{entries: make(map[string]*cacheEntry)}
}

func (c *contentCache) get(path string) (doc *TextDocument, size int64, mod time.Time, dirty bool, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[path]
	if !ok {
		return nil, 0, time.Time{}, false, false
	}
	return e.doc, e.size, e.modTime, e.dirty, true
}

func (c *contentCache) getDirty(path string) (*TextDocument, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[path]
	if !ok || !e.dirty {
		return nil, false
	}
	return e.doc, true
}

func (c *contentCache) put(path string, doc *TextDocument, size int64, mod time.Time, dirty bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[path]; ok {
		c.bytes -= old.size
	}
	stored := doc.clone()
	if size <= 0 {
		size = int64(len(stored.text))
	}
	c.entries[path] = &cacheEntry{doc: stored, size: size, modTime: mod, dirty: dirty}
	c.bytes += size
	for len(c.entries) > maxCacheEntries || c.bytes > maxCacheBytes {
		var victim string
		for p, e := range c.entries {
			if !e.dirty {
				victim = p
				break
			}
		}
		if victim == "" {
			break
		}
		c.bytes -= c.entries[victim].size
		delete(c.entries, victim)
	}
}

func (c *contentCache) markClean(path string, size int64, mod time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[path]
	if !ok {
		return
	}
	c.bytes -= e.size
	e.dirty, e.size, e.modTime = false, size, mod
	c.bytes += size
}

func (c *contentCache) remove(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[path]; ok {
		c.bytes -= e.size
		delete(c.entries, path)
	}
}

func (c *contentCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*cacheEntry)
	c.bytes = 0
}

func (c *contentCache) removePrefix(point string) {
	cleaned, err := cleanVirtualPath(point)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for p, e := range c.entries {
		if p == cleaned || strings.HasPrefix(p, cleaned+"/") {
			c.bytes -= e.size
			delete(c.entries, p)
		}
	}
}

func (c *contentCache) dirtyDocs() []*TextDocument {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*TextDocument
	for _, e := range c.entries {
		if e.dirty {
			out = append(out, e.doc.clone())
		}
	}
	return out
}
