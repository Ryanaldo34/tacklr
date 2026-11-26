package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"
)

// fifo is a mutexed insertion-order cache (not a promoting LRU).
type fifo[T any] struct {
	mu    sync.Mutex
	cap   int
	keys  []string
	items map[string]T
}

func newFIFO[T any](n int) *fifo[T] {
	return &fifo[T]{cap: n, items: make(map[string]T)}
}

func (c *fifo[T]) get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[key]
	return v, ok
}

// put keeps val for key. A new key evicts the oldest key when c is full.
func (c *fifo[T]) put(key string, val T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		if len(c.keys) >= c.cap {
			delete(c.items, c.keys[0])
			c.keys = c.keys[1:]
		}
		c.keys = append(c.keys, key)
	}
	c.items[key] = val
}

// load is the value for key, from c or else from fetch, which c then keeps.
func (c *fifo[T]) load(key string, fetch func() (T, error)) (T, error) {
	if v, ok := c.get(key); ok {
		return v, nil
	}
	v, err := fetch()
	if err != nil {
		return v, err
	}
	c.put(key, v)
	return v, nil
}

func (c *fifo[T]) dropFile(fileID string) {
	prefix := fileID + "\x00"
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.keys[:0]
	for _, k := range c.keys {
		if k == fileID || strings.HasPrefix(k, prefix) {
			delete(c.items, k)
			continue
		}
		kept = append(kept, k)
	}
	c.keys = kept
}

func cacheKey(fileID string, mod time.Time) string {
	return fileID + "\x00" + mod.UTC().Format(time.RFC3339Nano)
}

func (p *driveProvider) invalidate(fileID string) {
	p.zipCache.dropFile(fileID)
	p.getCache.dropFile(fileID)
	p.sheetCache.dropFile(fileID)
}

func driveRevision(m DriveMeta) string {
	if m.Version != "" {
		return m.Version
	}
	if !m.ModTime.IsZero() {
		return m.ModTime.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func (p *driveProvider) docsGet(ctx context.Context, id string) (DocsSnapshot, error) {
	var snap DocsSnapshot
	err := p.call(ctx, func() error {
		var err error
		snap, err = p.docs.Get(ctx, id)
		return err
	})
	return snap, err
}

func (p *driveProvider) sheetsGet(ctx context.Context, id string) (SheetsSnapshot, error) {
	var snap SheetsSnapshot
	err := p.call(ctx, func() error {
		var err error
		snap, err = p.sheets.Get(ctx, id)
		return err
	})
	return snap, err
}

// parentAndLeaf makes the missing folders above rel, a clean path that is
// not the root, and returns the ID of its parent and its last name.
func (p *driveProvider) parentAndLeaf(ctx context.Context, rel string) (parentID, leaf string, err error) {
	dir, leaf := path.Split(rel)
	parent, err := p.ensureDir(ctx, strings.TrimSuffix(dir, "/"))
	return parent.ID, leaf, err
}

func (p *driveProvider) ensureDir(ctx context.Context, rel string) (DriveMeta, error) {
	root, err := p.root(ctx)
	if err != nil {
		return DriveMeta{}, err
	}
	if rel == "" {
		return root, nil
	}
	cur := root
	for part := range strings.SplitSeq(rel, "/") {
		child, err := p.lookupChild(ctx, cur.ID, part, true)
		if err == nil {
			if !child.dir() {
				return DriveMeta{}, fmt.Errorf("%w: %q", ErrNotDir, part)
			}
			cur = child
			continue
		}
		if !isNotExist(err) {
			return DriveMeta{}, err
		}
		parentID := cur.ID
		err = p.call(ctx, func() error {
			var e error
			cur, e = p.api.Mkdir(ctx, parentID, part)
			return e
		})
		if err != nil {
			return DriveMeta{}, err
		}
	}
	return cur, nil
}

func isNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

// openGoogleDoc reads a Google Doc from the Docs API on a writable mount,
// and from its HTML export on a read-only one. Both are kept per
// modification time.
func (p *driveProvider) openGoogleDoc(ctx context.Context, name string, meta DriveMeta) (Document, error) {
	key := cacheKey(meta.ID, meta.ModTime)
	if !p.writable || p.docs == nil {
		html, err := p.zipCache.load(key, func() ([]byte, error) { return p.exportHTML(ctx, meta.ID) })
		if err != nil {
			return nil, err
		}
		return DocsCodec{}.Decode(ctx, name, mimeGoogleDocument, html)
	}
	snap, err := p.getCache.load(key, func() (DocsSnapshot, error) { return p.docsGet(ctx, meta.ID) })
	if err != nil {
		return nil, err
	}
	return snapshotToRich(name, snap), nil
}

// openGoogleSheet reads a Google Sheet from the Sheets API on a writable
// mount, and from its HTML export on a read-only one. Both are kept per
// modification time.
func (p *driveProvider) openGoogleSheet(ctx context.Context, name string, meta DriveMeta) (Document, error) {
	key := cacheKey(meta.ID, meta.ModTime)
	if !p.writable || p.sheets == nil {
		raw, err := p.zipCache.load(key, func() ([]byte, error) { return p.exportBytes(ctx, meta.ID) })
		if err != nil {
			return nil, err
		}
		return SheetsCodec{}.Decode(ctx, name, mimeGoogleSpreadsheet, raw)
	}
	snap, err := p.sheetCache.load(key, func() (SheetsSnapshot, error) { return p.sheetsGet(ctx, meta.ID) })
	if err != nil {
		return nil, err
	}
	return NewTabularDocument(name, mimeGoogleSpreadsheet, snap.Sheets, snap.Named)
}

func (p *driveProvider) exportHTML(ctx context.Context, fileID string) ([]byte, error) {
	data, err := p.exportBytes(ctx, fileID)
	if err != nil {
		return nil, err
	}
	html, err := unzipHTML(data)
	if err != nil {
		return nil, err
	}
	if len(html) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return html, nil
}

func (p *driveProvider) exportBytes(ctx context.Context, fileID string) ([]byte, error) {
	var body io.ReadCloser
	var size int64
	err := p.call(ctx, func() error {
		var e error
		body, size, e = p.api.Export(ctx, fileID, mimeExportHTMLZip)
		return e
	})
	if err != nil {
		return nil, err
	}
	data, err := readCapped(body, MaxDocsExportBytes, size)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	if len(data) > MaxDocsExportBytes {
		return nil, errFileExceeds(MaxDocsExportBytes)
	}
	return data, nil
}

// PutFile implements filePutter. Native Google files are rejected.
func (p *driveProvider) PutFile(ctx context.Context, rel string, body []byte) error {
	meta, err := p.resolve(ctx, rel)
	if isNotExist(err) {
		return p.createFile(ctx, rel, body)
	}
	if err != nil {
		return err
	}
	if isGoogleNativeFile(meta.MimeType) {
		return ErrNotSupported
	}
	media := mediaTypeOf(meta.MimeType, meta.Name, nil)
	err = p.call(ctx, func() error {
		_, e := p.api.PutMedia(ctx, meta.ID, media, bytes.NewReader(body), int64(len(body)))
		return e
	})
	if err != nil {
		return err
	}
	p.invalidate(meta.ID)
	return nil
}

// createFile uploads body as a new file at rel and makes its missing
// folders.
func (p *driveProvider) createFile(ctx context.Context, rel string, body []byte) error {
	parentID, leaf, err := p.parentAndLeaf(ctx, rel)
	if err != nil {
		return err
	}
	media := DetectMediaType(leaf, nil)
	return p.call(ctx, func() error {
		_, e := p.api.Create(ctx, parentID, leaf, media, media, bytes.NewReader(body), int64(len(body)))
		return e
	})
}

func readCapped(r io.Reader, limit int, hint int64) ([]byte, error) {
	var buf bytes.Buffer
	if hint > 0 && hint <= int64(limit) {
		buf.Grow(int(hint))
	}
	_, err := buf.ReadFrom(io.LimitReader(r, int64(limit)+1))
	return buf.Bytes(), err
}

func zipSizeHint(n uint64, limit int) int64 {
	if limit <= 0 || n == 0 || n > uint64(limit) {
		return 0
	}
	return int64(n) //nolint:gosec // G115: n is capped at limit (≤ MaxReadFileBytes)
}
