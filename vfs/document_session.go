package vfs

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// OpenDocument loads a virtual path into a Document IR.
//
// Textual results use the session write-back cache; the returned value is always
// a clone (safe to edit). Binary / unregistered types are uncached.
// reg nil uses DefaultContentRegistry().
func (m *MountSession) OpenDocument(ctx context.Context, virtualPath string, reg *ContentRegistry) (Document, error) {
	if reg == nil {
		reg = DefaultContentRegistry()
	}
	if doc, ok := m.cachedText(ctx, virtualPath); ok {
		return doc, nil
	}

	var mt string
	size, mod := int64(-1), time.Time{}
	if fi, err := m.Stat(ctx, virtualPath); err == nil {
		if fi.IsDir {
			return nil, fmt.Errorf("vfs: %s is a directory", virtualPath)
		}
		mt, size, mod = fi.MediaType, fi.Size, fi.ModTime
	}

	raw, err := m.ReadFile(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	if mt == "" {
		mt = "application/octet-stream"
	}
	decoded, err := reg.Decode(ctx, virtualPath, mt, raw)
	if err != nil {
		return nil, err
	}
	td, ok := decoded.(*TextDocument)
	if !ok {
		return decoded, nil
	}
	if size < 0 {
		size = int64(len(td.Text()))
	}
	m.cache.put(virtualPath, td, size, mod, false)
	return td.clone(), nil
}

// ReadText opens a virtual path as Textual IR (clone; safe to edit).
func (m *MountSession) ReadText(ctx context.Context, virtualPath string) (Textual, error) {
	doc, err := m.OpenDocument(ctx, virtualPath, nil)
	if err != nil {
		return nil, err
	}
	t, ok := doc.(Textual)
	if !ok {
		return nil, ErrNotTextual
	}
	return t, nil
}

// WriteDocument stages Textual IR in the session cache as dirty.
// Backend updates happen on Sync / SyncAll (checkpoint calls SyncAll).
func (m *MountSession) WriteDocument(ctx context.Context, doc Document) error {
	t, ok := doc.(Textual)
	if !ok {
		return ErrNotTextual
	}
	td, ok := t.(*TextDocument)
	if !ok {
		td = NewTextDocument(t.Path(), t.MediaType(), t.Encoding(), t.Text())
	}
	cleaned, err := cleanVirtualPath(td.Path())
	if err != nil {
		return err
	}
	if _, _, err := m.at(ctx, cleaned, true); err != nil {
		return err
	}
	if len(td.Text()) > MaxReadFileBytes {
		return errFileExceeds(MaxReadFileBytes)
	}
	// Keep cache key aligned with cleaned path (td.Path may already be clean).
	if cleaned != td.Path() {
		td = td.clone()
		td.path = cleaned
	}
	m.cache.put(cleaned, td, int64(len(td.Text())), time.Now().UTC(), true)
	return nil
}

// Sync flushes one dirty path to the backend (no-op if clean or uncached).
func (m *MountSession) Sync(ctx context.Context, virtualPath string) error {
	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return err
	}
	doc, dirty := m.cache.getDirty(cleaned)
	if !dirty {
		return nil
	}
	body := doc.Text()
	if err := m.writeContents(ctx, cleaned, strings.NewReader(body), int64(len(body))); err != nil {
		return err
	}
	// Mark clean before Stat so overlay no longer masks backend mtime.
	m.cache.markClean(cleaned, int64(len(body)), time.Time{})
	if fi, err := m.Stat(ctx, cleaned); err == nil {
		m.cache.markClean(cleaned, int64(len(body)), fi.ModTime)
	}
	m.fireAfterPersist(ctx, cleaned)
	return nil
}

// SyncAll flushes every dirty path. Harness checkpoint calls this before saving Specs.
func (m *MountSession) SyncAll(ctx context.Context) error {
	for _, doc := range m.cache.dirtyDocs() {
		if err := m.Sync(ctx, doc.Path()); err != nil {
			return err
		}
	}
	return nil
}

func (m *MountSession) cachedText(ctx context.Context, virtualPath string) (*TextDocument, bool) {
	doc, size, mod, dirty, ok := m.cache.get(virtualPath)
	if !ok {
		return nil, false
	}
	if !dirty {
		if fi, err := m.Stat(ctx, virtualPath); err == nil {
			stale := (size >= 0 && fi.Size != size) || (!mod.IsZero() && !fi.ModTime.Equal(mod))
			if stale {
				m.cache.remove(virtualPath)
				return nil, false
			}
		}
	}
	return doc.clone(), true
}
