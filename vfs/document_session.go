package vfs

import (
	"context"
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

	raw, err := m.ReadFile(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	sample := raw
	if len(sample) > 512 {
		sample = sample[:512]
	}
	decoded, err := reg.Decode(ctx, virtualPath, DetectMediaType(virtualPath, sample), raw)
	if err != nil {
		return nil, err
	}
	td, ok := decoded.(*TextDocument)
	if !ok {
		return decoded, nil
	}
	size, mod := int64(-1), time.Time{}
	if fi, err := m.Stat(ctx, virtualPath); err == nil {
		size, mod = fi.Size, fi.ModTime
	}
	m.cache.put(virtualPath, td, size, mod, false)
	return td.clone(), nil
}

// ReadText opens a virtual path as a *TextDocument (clone; safe to edit).
func (m *MountSession) ReadText(ctx context.Context, virtualPath string) (*TextDocument, error) {
	doc, err := m.OpenDocument(ctx, virtualPath, nil)
	if err != nil {
		return nil, err
	}
	td, ok := doc.(*TextDocument)
	if !ok {
		return nil, ErrNotTextual
	}
	return td, nil
}

// WriteDocument stages *TextDocument in the session cache as dirty.
// Backend updates happen on Sync / SyncAll (checkpoint calls SyncAll).
func (m *MountSession) WriteDocument(ctx context.Context, doc Document) error {
	td, ok := doc.(*TextDocument)
	if !ok {
		return ErrNotTextual
	}
	if _, _, err := m.at(ctx, td.Path(), true); err != nil {
		return err
	}
	if len(td.Text()) > MaxReadFileBytes {
		return errFileExceeds(MaxReadFileBytes)
	}
	m.cache.put(td.Path(), td, int64(len(td.Text())), time.Time{}, true)
	return nil
}

// Sync flushes one dirty path to the backend (no-op if clean or uncached).
func (m *MountSession) Sync(ctx context.Context, virtualPath string) error {
	doc, dirty := m.cache.getDirty(virtualPath)
	if !dirty {
		return nil
	}
	body := doc.Text()
	if err := m.writeContents(ctx, virtualPath, strings.NewReader(body), int64(len(body))); err != nil {
		return err
	}
	mod := time.Time{}
	if fi, err := m.Stat(ctx, virtualPath); err == nil {
		mod = fi.ModTime
	}
	m.cache.markClean(virtualPath, int64(len(body)), mod)
	m.fireAfterPersist(ctx, virtualPath)
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
