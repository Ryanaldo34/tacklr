package vfs_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

type memNode struct {
	meta   vfs.DriveMeta
	parent string
	body   []byte
}

// memDrive is an in-process DriveAPI (no network).
type memDrive struct {
	mu    sync.Mutex
	nodes map[string]*memNode
	fail  map[string]error
	once  map[string]error
}

func newMemDrive() *memDrive {
	return &memDrive{nodes: make(map[string]*memNode), fail: make(map[string]error), once: make(map[string]error)}
}

func (m *memDrive) add(parent string, meta vfs.DriveMeta, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if meta.ModTime.IsZero() {
		meta.ModTime = time.Now().UTC()
	}
	meta.IsDir = meta.MimeType == "application/vnd.google-apps.folder"
	m.nodes[meta.ID] = &memNode{meta: meta, parent: parent, body: append([]byte(nil), body...)}
}

func (m *memDrive) check(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.once[op]; ok {
		delete(m.once, op)
		return err
	}
	return m.fail[op]
}

func (m *memDrive) GetMeta(ctx context.Context, fileID string) (vfs.DriveMeta, error) {
	if err := m.check(ctx, "GetMeta"); err != nil {
		return vfs.DriveMeta{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[fileID]
	if !ok {
		return vfs.DriveMeta{}, vfs.ErrNotExist
	}
	return n.meta, nil
}

func (m *memDrive) GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error) {
	if err := m.check(ctx, "GetMedia"); err != nil {
		return nil, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[fileID]
	if !ok {
		return nil, 0, vfs.ErrNotExist
	}
	cp := append([]byte(nil), n.body...)
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), nil
}

func (m *memDrive) List(ctx context.Context, folderID string) ([]vfs.DriveMeta, error) {
	if err := m.check(ctx, "List"); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[folderID]; !ok {
		return nil, vfs.ErrNotExist
	}
	var out []vfs.DriveMeta
	for _, n := range m.nodes {
		if n.parent == folderID {
			out = append(out, n.meta)
		}
	}
	return out, nil
}
