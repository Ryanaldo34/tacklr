package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
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
	var zero T
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[key]
	return v, ok
}

func (c *fifo[T]) put(key string, val T) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		c.items[key] = val
		return
	}
	if len(c.keys) >= c.cap {
		old := c.keys[0]
		c.keys = c.keys[1:]
		delete(c.items, old)
	}
	c.keys = append(c.keys, key)
	c.items[key] = val
}

func (c *fifo[T]) dropFile(fileID string) {
	if c == nil {
		return
	}
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

func (p *driveProvider) docsBatch(ctx context.Context, id string, req DocsBatch) (DocsBatchResult, error) {
	var res DocsBatchResult
	err := p.call(ctx, func() error {
		var err error
		res, err = p.docs.BatchUpdate(ctx, id, req)
		return err
	})
	return res, err
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

func (p *driveProvider) sheetsBatchValues(ctx context.Context, id string, req SheetsValuesBatch) error {
	return p.call(ctx, func() error {
		return p.sheets.BatchUpdateValues(ctx, id, req)
	})
}

// resolveLeaf walks the path but does not follow the last component if it is a shortcut.
func (p *driveProvider) resolveLeaf(ctx context.Context, name string) (DriveMeta, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return DriveMeta{}, err
	}
	if rel == "" {
		return DriveMeta{}, ErrInvalidPath
	}
	root, err := p.getMeta(ctx, p.rootID)
	if err != nil {
		return DriveMeta{}, err
	}
	root, err = p.follow(ctx, root, 4)
	if err != nil {
		return DriveMeta{}, err
	}
	parts := strings.Split(rel, "/")
	parentID := root.ID
	for i, part := range parts {
		if i < len(parts)-1 {
			child, err := p.child(ctx, parentID, part)
			if err != nil {
				return DriveMeta{}, err
			}
			if !child.dir() {
				return DriveMeta{}, ErrNotExist
			}
			parentID = child.ID
			continue
		}
		return p.childRaw(ctx, parentID, part)
	}
	return DriveMeta{}, ErrNotExist
}

func (p *driveProvider) parentAndLeaf(ctx context.Context, name string) (parentID, leaf string, parentMeta DriveMeta, err error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", "", DriveMeta{}, err
	}
	if rel == "" {
		return "", "", DriveMeta{}, ErrInvalidPath
	}
	root, err := p.getMeta(ctx, p.rootID)
	if err != nil {
		return "", "", DriveMeta{}, err
	}
	root, err = p.follow(ctx, root, 4)
	if err != nil {
		return "", "", DriveMeta{}, err
	}
	dir, leaf := path.Split(rel)
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		return root.ID, leaf, root, nil
	}
	parent, err := p.ensureDir(ctx, dir)
	if err != nil {
		return "", "", DriveMeta{}, err
	}
	return parent.ID, leaf, parent, nil
}

func (p *driveProvider) ensureDir(ctx context.Context, rel string) (DriveMeta, error) {
	root, err := p.getMeta(ctx, p.rootID)
	if err != nil {
		return DriveMeta{}, err
	}
	root, err = p.follow(ctx, root, 4)
	if err != nil {
		return DriveMeta{}, err
	}
	if rel == "" {
		return root, nil
	}
	parentID := root.ID
	var cur DriveMeta
	for _, part := range strings.Split(rel, "/") {
		child, err := p.child(ctx, parentID, part)
		if err == nil {
			if !child.dir() {
				return DriveMeta{}, fmt.Errorf("%w: %q is not a folder", ErrNotSupported, part)
			}
			cur = child
			parentID = child.ID
			continue
		}
		if !isNotExist(err) {
			return DriveMeta{}, err
		}
		var created DriveMeta
		cerr := p.call(ctx, func() error {
			var e error
			created, e = p.api.Mkdir(ctx, parentID, part)
			return e
		})
		if cerr != nil {
			return DriveMeta{}, cerr
		}
		cur = created
		parentID = created.ID
	}
	return cur, nil
}

func isNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

func (p *driveProvider) openGoogleDoc(ctx context.Context, name string, meta DriveMeta) (Document, error) {
	key := cacheKey(meta.ID, meta.ModTime)
	if p.writable && p.docs != nil {
		if snap, ok := p.getCache.get(key); ok {
			return snapshotToRich(name, snap), nil
		}
		snap, err := p.docsGet(ctx, meta.ID)
		if err != nil {
			return nil, err
		}
		p.getCache.put(key, snap)
		return snapshotToRich(name, snap), nil
	}
	if html, ok := p.zipCache.get(key); ok {
		return DocsCodec{}.Decode(ctx, name, mimeGoogleDocument, html)
	}
	html, err := p.exportHTML(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	p.zipCache.put(key, html)
	return DocsCodec{}.Decode(ctx, name, mimeGoogleDocument, html)
}

func (p *driveProvider) openGoogleSheet(ctx context.Context, name string, meta DriveMeta) (Document, error) {
	key := cacheKey(meta.ID, meta.ModTime)
	if p.writable && p.sheets != nil {
		if snap, ok := p.sheetCache.get(key); ok {
			td, err := snapshotToTabular(name, snap)
			if err != nil {
				return nil, err
			}
			td.hint.fileID = meta.ID
			td.hint.revisionID = snap.RevisionID
			return td, nil
		}
		snap, err := p.sheetsGet(ctx, meta.ID)
		if err != nil {
			return nil, err
		}
		snap.RevisionID = driveRevision(meta)
		p.sheetCache.put(key, snap)
		td, err := snapshotToTabular(name, snap)
		if err != nil {
			return nil, err
		}
		td.hint.fileID = meta.ID
		td.hint.revisionID = snap.RevisionID
		return td, nil
	}
	if html, ok := p.zipCache.get(key); ok {
		return SheetsCodec{}.Decode(ctx, name, mimeGoogleSpreadsheet, html)
	}
	raw, err := p.exportZip(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	p.zipCache.put(key, raw)
	return SheetsCodec{}.Decode(ctx, name, mimeGoogleSpreadsheet, raw)
}

func (p *driveProvider) exportZip(ctx context.Context, fileID string) ([]byte, error) {
	return p.exportBytes(ctx, fileID)
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
func (p *driveProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	rel, err := cleanRel(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return ErrInvalidPath
	}
	meta, err := p.resolve(ctx, name)
	if err == nil {
		if isGoogleNativeFile(meta.MimeType) {
			return ErrNotSupported
		}
		media := s3KnownType(meta.MimeType)
		if media == "" {
			media = DetectMediaType(meta.Name, nil)
		}
		if media == "" || media == "application/octet-stream" {
			media = "application/octet-stream"
		}
		body, err := bufferUpload(r, size)
		if err != nil {
			return err
		}
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
	if !isNotExist(err) {
		return err
	}
	parentID, leaf, _, err := p.parentAndLeaf(ctx, name)
	if err != nil {
		return err
	}
	media := DetectMediaType(leaf, nil)
	body, err := bufferUpload(r, size)
	if err != nil {
		return err
	}
	var created DriveMeta
	err = p.call(ctx, func() error {
		var e error
		created, e = p.api.Create(ctx, parentID, leaf, media, media, bytes.NewReader(body), int64(len(body)))
		return e
	})
	if err != nil {
		return err
	}
	p.invalidate(created.ID)
	return nil
}

// WriteDocument implements documentBackend.
func (p *driveProvider) WriteDocument(ctx context.Context, name string, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	if rd, ok := doc.(*RichDocument); ok && normalizeMediaType(rd.MediaType()) == mimeGoogleDocument {
		return p.writeGoogleDoc(ctx, name, rd)
	}
	if td, ok := doc.(*TabularDocument); ok && isSpreadsheet(td.MediaType()) {
		return p.writeGoogleSheet(ctx, name, td)
	}
	meta, err := p.resolve(ctx, name)
	if err == nil && isGoogleNativeFile(meta.MimeType) {
		return ErrNotSupported
	}
	if err != nil && !isNotExist(err) {
		return err
	}
	data, _, err := encodeDocument(ctx, doc, DefaultContentRegistry())
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}

func bufferUpload(r io.Reader, size int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	if size > 0 {
		return readCapped(r, int(size), size)
	}
	return io.ReadAll(r)
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

func (p *driveProvider) writeGoogleSheet(ctx context.Context, name string, td *TabularDocument) error {
	if p.sheets == nil {
		return ErrNotSupported
	}
	meta, err := p.resolve(ctx, name)
	if isNotExist(err) {
		return p.createGoogleSheet(ctx, name, td)
	}
	if err != nil {
		return err
	}
	if meta.MimeType != mimeGoogleSpreadsheet {
		return ErrNotSupported
	}
	if td.hint.revisionID == "" {
		return ErrConflict
	}
	batch, err := tabularOverlayBatch(td)
	if err != nil {
		return err
	}
	if len(batch.Data) > 0 {
		if err := p.sheetsBatchValues(ctx, meta.ID, batch); err != nil {
			return err
		}
	}
	p.invalidate(meta.ID)
	return p.refreshSheetHint(ctx, meta.ID, td)
}

func (p *driveProvider) createGoogleSheet(ctx context.Context, name string, td *TabularDocument) error {
	parentID, leaf, _, err := p.parentAndLeaf(ctx, name)
	if err != nil {
		return err
	}
	var created DriveMeta
	err = p.call(ctx, func() error {
		var e error
		created, e = p.api.Create(ctx, parentID, leaf, mimeGoogleSpreadsheet, "", nil, 0)
		return e
	})
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		snap, err := p.sheetsGet(ctx, created.ID)
		if err != nil {
			return err
		}
		if len(td.sheets) == 0 || td.sheets[0].Rows == 0 {
			last = nil
			break
		}
		overlay := *td
		if len(snap.Sheets) > 0 {
			sh := overlay.sheets[0]
			sh.Title = snap.Sheets[0].Title
			sh.ID = snap.Sheets[0].ID
			overlay.sheets = []Sheet{sh}
		}
		batch, err := tabularOverlayBatch(&overlay)
		if err != nil {
			return err
		}
		if len(batch.Data) == 0 {
			last = nil
			break
		}
		last = p.sheetsBatchValues(ctx, created.ID, batch)
		if last == nil {
			break
		}
		if !errors.Is(last, ErrConflict) {
			return last
		}
	}
	if last != nil {
		return last
	}
	p.invalidate(created.ID)
	return p.refreshSheetHint(ctx, created.ID, td)
}

func (p *driveProvider) refreshSheetHint(ctx context.Context, id string, td *TabularDocument) error {
	meta, merr := p.getMeta(ctx, id)
	snap, err := p.sheetsGet(ctx, id)
	if err == nil {
		fresh, ferr := adoptTabularDocument(td.path, mimeGoogleSpreadsheet, snap.Sheets, snap.Named)
		if ferr == nil {
			td.sheets = fresh.sheets
			td.named = fresh.named
			td.html = fresh.html
			td.starts = fresh.starts
			td.hint.fileID = id
			if merr == nil {
				td.hint.revisionID = driveRevision(meta)
			}
		}
	}
	return nil
}

func (p *driveProvider) writeGoogleDoc(ctx context.Context, name string, rd *RichDocument) error {
	if p.docs == nil {
		return ErrNotSupported
	}
	meta, err := p.resolve(ctx, name)
	if isNotExist(err) {
		return p.createGoogleDoc(ctx, name, rd)
	}
	if err != nil {
		return err
	}
	if isGoogleNativeFile(meta.MimeType) && meta.MimeType != mimeGoogleDocument {
		return ErrNotSupported
	}
	if meta.MimeType != mimeGoogleDocument {
		return ErrNotSupported
	}
	if rd.hint.revisionID == "" {
		return ErrConflict
	}
	if rd.mut == richSet {
		if err := p.setBlocksDoc(ctx, meta.ID, rd); err != nil {
			return err
		}
	} else {
		if err := p.replaceBlocksDoc(ctx, meta.ID, rd); err != nil {
			return err
		}
	}
	p.invalidate(meta.ID)
	return p.refreshHint(ctx, meta.ID, rd)
}

func (p *driveProvider) replaceBlocksDoc(ctx context.Context, id string, rd *RichDocument) error {
	reqs, err := mapReplaceBlocks(rd.hint, rd.blocks)
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		return nil
	}
	_, err = p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: rd.hint.revisionID, Requests: reqs})
	return err
}

func (p *driveProvider) setBlocksDoc(ctx context.Context, id string, rd *RichDocument) error {
	if len(rd.blocks) == 0 {
		return fmt.Errorf("write: refusing empty IR replace")
	}
	tabID := ""
	tabs := uniqueTabIDs(rd.blocks, rd.tabs)
	if len(rd.tabs) > 1 || len(tabs) > 1 {
		tabID = blockAttr(rd.blocks[0], "tab_id")
		if tabID == "" {
			return fmt.Errorf("write: tab_id required")
		}
	} else if len(rd.tabs) == 1 {
		tabID = rd.tabs[0].ID
	} else if len(tabs) == 1 {
		tabID = tabs[0]
	}
	tabBlocks := make([]Block, 0, len(rd.blocks))
	for _, b := range rd.blocks {
		if tabID == "" || blockAttr(b, "tab_id") == tabID {
			tabBlocks = append(tabBlocks, b)
		}
	}
	if len(tabBlocks) == 0 {
		return fmt.Errorf("write: refusing empty IR replace")
	}
	keep := keepImageObjectIDs(tabBlocks)
	dels := mapSetBlocksDeletes(rd.hint, tabID, keep)
	cas := rd.hint.revisionID
	if len(dels) > 0 {
		res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: cas, TabID: tabID, Requests: dels})
		if err != nil {
			return err
		}
		if res.RevisionID != "" {
			cas = res.RevisionID
		}
	}
	return p.insertBlocks(ctx, id, tabID, cas, tabBlocks)
}

func (p *driveProvider) createGoogleDoc(ctx context.Context, name string, rd *RichDocument) error {
	parentID, leaf, _, err := p.parentAndLeaf(ctx, name)
	if err != nil {
		return err
	}
	var created DriveMeta
	err = p.call(ctx, func() error {
		var e error
		created, e = p.api.Create(ctx, parentID, leaf, mimeGoogleDocument, "", nil, 0)
		return e
	})
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		snap, err := p.docsGet(ctx, created.ID)
		if err != nil {
			return err
		}
		tabID := ""
		if len(snap.Tabs) > 0 {
			tabID = snap.Tabs[0].ID
		}
		last = p.insertBlocks(ctx, created.ID, tabID, snap.RevisionID, rd.blocks)
		if last == nil {
			break
		}
		if !errors.Is(last, ErrConflict) {
			return last
		}
	}
	if last != nil {
		return last
	}
	p.invalidate(created.ID)
	return p.refreshHint(ctx, created.ID, rd)
}

func (p *driveProvider) insertBlocks(ctx context.Context, id, tabID, cas string, blocks []Block) error {
	chunks, _, err := mapInsertBlocks(blocks, 1, tabID)
	if err != nil {
		return err
	}
	for _, ch := range chunks {
		if len(ch.reqs) > 0 {
			body, styles := splitTextStyles(ch.reqs)
			if len(body) > 0 {
				res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: cas, TabID: tabID, Requests: body})
				if err != nil {
					return err
				}
				if res.RevisionID != "" {
					cas = res.RevisionID
				}
			}
			if len(styles) > 0 {
				res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: cas, TabID: tabID, Requests: styles})
				if err != nil {
					return err
				}
				if res.RevisionID != "" {
					cas = res.RevisionID
				}
			}
		}
		if !ch.tableFill {
			continue
		}
		snap, err := p.docsGet(ctx, id)
		if err != nil {
			return err
		}
		if snap.RevisionID != "" {
			cas = snap.RevisionID
		}
		var table *DocsSpan
		for i := range snap.Body {
			if snap.Body[i].Kind == "table" && (tabID == "" || snap.Body[i].TabID == tabID) {
				sp := snap.Body[i]
				table = &sp
			}
		}
		if table == nil {
			return fmt.Errorf("%w: inserted table not found", ErrNotSupported)
		}
		fill := Block{
			Kind:  BlockKindTable,
			Text:  encodeTSV(ch.grid),
			Style: StyleMeta{Attributes: map[string]string{"rows": strconv.Itoa(ch.rows), "cols": strconv.Itoa(ch.cols)}},
		}
		reqs, err := mapReplaceTable(spanToLocation(*table), fill)
		if err != nil {
			return err
		}
		if len(reqs) == 0 {
			continue
		}
		res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: cas, TabID: tabID, Requests: reqs})
		if err != nil {
			return err
		}
		if res.RevisionID != "" {
			cas = res.RevisionID
		}
	}
	return nil
}

func (p *driveProvider) refreshHint(ctx context.Context, id string, rd *RichDocument) error {
	snap, err := p.docsGet(ctx, id)
	if err == nil {
		fresh := snapshotToRich(rd.path, snap)
		rd.hint = fresh.hint
		rd.tabs = fresh.tabs
		rd.blocks = fresh.blocks
		rd.reproject()
		rd.mut = richClean
		return nil
	}
	rd.hint.locations = nil
	rd.hint.structural = nil
	return nil
}

func uniqueTabIDs(blocks []Block, tabs []DocTab) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, t := range tabs {
		if t.ID == "" {
			continue
		}
		if _, ok := seen[t.ID]; ok {
			continue
		}
		seen[t.ID] = struct{}{}
		out = append(out, t.ID)
	}
	for _, b := range blocks {
		id := blockAttr(b, "tab_id")
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
