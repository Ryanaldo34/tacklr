package vfs

import (
	"bytes"
	"cmp"
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

// persistNative writes a rich Google Doc or a grid Google Sheet through
// the Docs or Sheets API. It reports false for any other document.
func (p *driveProvider) persistNative(ctx context.Context, name string, doc Document) (bool, error) {
	d, ok := asIR(doc)
	if !ok {
		return false, nil
	}
	mt := normalizeMediaType(d.MediaType())
	switch body := d.body.(type) {
	case *richBody:
		if mt == mimeGoogleDocument {
			return true, p.writeGoogleDoc(ctx, name, d, body)
		}
	case *gridBody:
		if mt == mimeGoogleSpreadsheet {
			return true, p.writeGoogleSheet(ctx, name, d, body)
		}
	}
	return false, nil
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

func (p *driveProvider) sheetsBatchUpdate(ctx context.Context, id string, req SheetsBatch) error {
	return p.call(ctx, func() error {
		return p.sheets.BatchUpdate(ctx, id, req)
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
	dir, leaf := path.Split(rel)
	parentID := root.ID
	if dir != "" {
		for part := range strings.SplitSeq(strings.TrimSuffix(dir, "/"), "/") {
			child, err := p.child(ctx, parentID, part)
			if err != nil {
				return DriveMeta{}, err
			}
			if !child.dir() {
				return DriveMeta{}, ErrNotExist
			}
			parentID = child.ID
		}
	}
	return p.childRaw(ctx, parentID, leaf)
}

// parentAndLeaf makes the missing folders above rel, a clean path that is
// not the root, and returns the ID of its parent and its last name.
func (p *driveProvider) parentAndLeaf(ctx context.Context, rel string) (parentID, leaf string, err error) {
	dir, leaf := path.Split(rel)
	parent, err := p.ensureDir(ctx, strings.TrimSuffix(dir, "/"))
	return parent.ID, leaf, err
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
	cur := root
	for part := range strings.SplitSeq(rel, "/") {
		child, err := p.child(ctx, cur.ID, part)
		if err == nil {
			if !child.dir() {
				return DriveMeta{}, fmt.Errorf("%w: %q is not a folder", ErrNotSupported, part)
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
	snap, err := p.sheetCache.load(key, func() (SheetsSnapshot, error) {
		snap, err := p.sheetsGet(ctx, meta.ID)
		snap.RevisionID = driveRevision(meta)
		return snap, err
	})
	if err != nil {
		return nil, err
	}
	td, err := snapshotToTabular(name, snap)
	if err != nil {
		return nil, err
	}
	td.hint.fileID = meta.ID
	td.hint.revisionID = snap.RevisionID
	return td, nil
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
	body, err := bufferUpload(r, size)
	if err != nil {
		return err
	}
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
	media := cmp.Or(s3KnownType(meta.MimeType), DetectMediaType(meta.Name, nil))
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

// WriteDocument implements documentBackend.
func (p *driveProvider) WriteDocument(ctx context.Context, name string, doc Document, reg *ContentRegistry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	if handled, err := p.persistNative(ctx, name, doc); handled {
		return err
	}
	meta, err := p.resolve(ctx, name)
	if err == nil && isGoogleNativeFile(meta.MimeType) {
		return ErrNotSupported
	}
	if err != nil && !isNotExist(err) {
		return err
	}
	data, err := reg.Encode(ctx, doc)
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// bufferUpload reads the whole upload, so that a call can send it again
// after a token refresh.
func bufferUpload(r io.Reader, size int64) ([]byte, error) {
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

// writeGoogleSheet writes the values and formats of g over the Google
// Sheet at name, or creates the Sheet.
func (p *driveProvider) writeGoogleSheet(ctx context.Context, name string, td *IR, g *gridBody) error {
	if p.sheets == nil {
		return ErrNotSupported
	}
	meta, err := p.resolve(ctx, name)
	if isNotExist(err) {
		return p.createGoogleSheet(ctx, name, td, g)
	}
	if err != nil {
		return err
	}
	if meta.MimeType != mimeGoogleSpreadsheet {
		return ErrNotSupported
	}
	if err := p.writeSheetCells(ctx, meta.ID, g); err != nil {
		return err
	}
	p.invalidate(meta.ID)
	return p.refreshSheetHint(ctx, meta.ID, td)
}

// writeSheetCells writes the values and then the formats of g.
func (p *driveProvider) writeSheetCells(ctx context.Context, id string, g *gridBody) error {
	if batch := tabularOverlayBatch(g); len(batch.Data) > 0 {
		if err := p.sheetsBatchValues(ctx, id, batch); err != nil {
			return err
		}
	}
	if formats := tabularFormatBatch(g); len(formats.Requests) > 0 {
		return p.sheetsBatchUpdate(ctx, id, formats)
	}
	return nil
}

// createGoogleSheet creates a Google Sheet at name and writes the first
// sheet of g into the sheet that Sheets makes for it.
func (p *driveProvider) createGoogleSheet(ctx context.Context, name string, td *IR, g *gridBody) error {
	rel, _ := cleanRel(name) // resolve accepted name
	parentID, leaf, err := p.parentAndLeaf(ctx, rel)
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
	if len(g.sheets) > 0 {
		snap, err := p.sheetsGet(ctx, created.ID)
		if err != nil {
			return err
		}
		sh := g.sheets[0]
		if len(snap.Sheets) > 0 {
			sh.Title, sh.ID = snap.Sheets[0].Title, snap.Sheets[0].ID
		}
		first := *g
		first.sheets = []Sheet{sh}
		if err := p.writeSheetCells(ctx, created.ID, &first); err != nil {
			return err
		}
	}
	return p.refreshSheetHint(ctx, created.ID, td)
}

func (p *driveProvider) refreshSheetHint(ctx context.Context, id string, td *IR) error {
	meta, merr := p.getMeta(ctx, id)
	snap, err := p.sheetsGet(ctx, id)
	if err != nil {
		return err
	}
	fresh, ferr := adoptTabularDocument(td.path, mimeGoogleSpreadsheet, snap.Sheets, snap.Named)
	if ferr != nil {
		return ferr
	}
	td.body = fresh.body
	td.hint.fileID = id
	if merr == nil {
		td.hint.revisionID = driveRevision(meta)
	}
	return nil
}

// writeGoogleDoc writes rb over the Google Doc at name, or creates the
// Doc.
func (p *driveProvider) writeGoogleDoc(ctx context.Context, name string, rd *IR, rb *richBody) error {
	if p.docs == nil {
		return ErrNotSupported
	}
	meta, err := p.resolve(ctx, name)
	if isNotExist(err) {
		return p.createGoogleDoc(ctx, name, rd, rb)
	}
	if err != nil {
		return err
	}
	if meta.MimeType != mimeGoogleDocument {
		return ErrNotSupported
	}
	if rd.hint.revisionID == "" {
		return ErrConflict
	}
	if rb.mut == richSet {
		err = p.setBlocksDoc(ctx, meta.ID, rd, rb)
	} else {
		err = p.replaceBlocksDoc(ctx, meta.ID, rd, rb)
	}
	p.invalidate(meta.ID)
	if err != nil {
		return err
	}
	return p.refreshHint(ctx, meta.ID, rd)
}

func (p *driveProvider) replaceBlocksDoc(ctx context.Context, id string, rd *IR, rb *richBody) error {
	reqs, err := mapReplaceBlocks(rd.hint, rb.tree)
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		return nil
	}
	_, err = p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: rd.hint.revisionID, Requests: reqs})
	return err
}

func (p *driveProvider) setBlocksDoc(ctx context.Context, id string, rd *IR, rb *richBody) error {
	if len(rb.tree) == 0 {
		return ErrEmptyReplace
	}
	tabID := ""
	tabs := uniqueTabIDs(rb.tree, rb.tabs)
	if len(rb.tabs) > 1 || len(tabs) > 1 {
		tabID = blockAttr(rb.tree[0], "tab_id")
		if tabID == "" {
			return ErrTabIDRequired
		}
	} else if len(rb.tabs) == 1 {
		tabID = rb.tabs[0].ID
	} else if len(tabs) == 1 {
		tabID = tabs[0]
	}
	// tabBlocks always holds rb.tree[0]: its tab is tabID, or it has none.
	tabBlocks := make([]Block, 0, len(rb.tree))
	for _, b := range rb.tree {
		id := blockAttr(b, "tab_id")
		// HTML writes omit tab_id. A single-tab Doc still owns those blocks.
		if tabID == "" || id == "" || id == tabID {
			tabBlocks = append(tabBlocks, b)
		}
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

// createGoogleDoc creates a Google Doc at name and inserts rb into its
// first tab. A write that meets a revision conflict starts again from a
// new read, at most four times.
func (p *driveProvider) createGoogleDoc(ctx context.Context, name string, rd *IR, rb *richBody) error {
	rel, _ := cleanRel(name) // resolve accepted name
	parentID, leaf, err := p.parentAndLeaf(ctx, rel)
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
	for attempt := range 4 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		snap, gerr := p.docsGet(ctx, created.ID)
		if gerr != nil {
			return gerr
		}
		tabID := ""
		if len(snap.Tabs) > 0 {
			tabID = snap.Tabs[0].ID
		}
		if err = p.insertAt(ctx, created.ID, tabID, snap.RevisionID, rb.tree, snap); !errors.Is(err, ErrConflict) {
			break
		}
	}
	if err != nil {
		return err
	}
	return p.refreshHint(ctx, created.ID, rd)
}

func (p *driveProvider) insertBlocks(ctx context.Context, id, tabID, cas string, blocks []Block) error {
	snap, err := p.docsGet(ctx, id)
	if err != nil {
		return err
	}
	return p.insertAt(ctx, id, tabID, cas, blocks, snap)
}

func (p *driveProvider) insertAt(ctx context.Context, id, tabID, cas string, blocks []Block, snap DocsSnapshot) error {
	if snap.RevisionID != "" {
		cas = snap.RevisionID
	}
	idx := paragraphInsertIndex(snap.Body, tabID)
	remaining := blocks
	for len(remaining) > 0 {
		head, rest := splitInsertHead(remaining)
		chunks, endIdx, err := mapInsertBlocks(head, idx, tabID)
		if err != nil {
			return err
		}
		remaining = rest
		table := false
		for _, ch := range chunks {
			if err := p.applyInsertChunk(ctx, id, tabID, &cas, ch); err != nil {
				return err
			}
			if ch.tableFill {
				table = true
			}
		}
		if table {
			got, err := p.docsGet(ctx, id)
			if err != nil {
				return err
			}
			snap = got
			if snap.RevisionID != "" {
				cas = snap.RevisionID
			}
			idx = paragraphAppendIndex(snap.Body, tabID)
			continue
		}
		idx = endIdx
	}
	return nil
}

func splitInsertHead(blocks []Block) (head, rest []Block) {
	for i, b := range blocks {
		if b.Kind != BlockKindTable {
			continue
		}
		if i == 0 {
			return blocks[:1], blocks[1:]
		}
		return blocks[:i], blocks[i:]
	}
	return blocks, nil
}

func (p *driveProvider) applyInsertChunk(ctx context.Context, id, tabID string, cas *string, ch insertChunk) error {
	if len(ch.reqs) > 0 {
		body, styles := splitTextStyles(ch.reqs)
		if len(body) > 0 {
			res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: *cas, TabID: tabID, Requests: body})
			if err != nil {
				return err
			}
			if res.RevisionID != "" {
				*cas = res.RevisionID
			}
		}
		if len(styles) > 0 {
			res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: *cas, TabID: tabID, Requests: styles})
			if err != nil {
				return err
			}
			if res.RevisionID != "" {
				*cas = res.RevisionID
			}
		}
	}
	if !ch.tableFill {
		return nil
	}
	snap, err := p.docsGet(ctx, id)
	if err != nil {
		return err
	}
	if snap.RevisionID != "" {
		*cas = snap.RevisionID
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
		return nil
	}
	res, err := p.docsBatch(ctx, id, DocsBatch{RequiredRevisionID: *cas, TabID: tabID, Requests: reqs})
	if err != nil {
		return err
	}
	if res.RevisionID != "" {
		*cas = res.RevisionID
	}
	return nil
}

func (p *driveProvider) refreshHint(ctx context.Context, id string, rd *IR) error {
	snap, err := p.docsGet(ctx, id)
	if err != nil {
		return err
	}
	fresh := snapshotToRich(rd.path, snap)
	rd.body = fresh.body
	rd.hint = fresh.hint
	return nil
}

// uniqueTabIDs is the tab IDs of tabs and then of blocks, each once.
func uniqueTabIDs(blocks []Block, tabs []DocTab) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(id string) {
		if _, ok := seen[id]; ok || id == "" {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, t := range tabs {
		add(t.ID)
	}
	for _, b := range blocks {
		add(blockAttr(b, "tab_id"))
	}
	return out
}
