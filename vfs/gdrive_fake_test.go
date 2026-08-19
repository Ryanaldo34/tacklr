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
	meta    vfs.DriveMeta
	parent  string
	body    []byte
	export  []byte
	trashed bool
}

// memDrive is an in-process DriveAPI (no network).
type memDrive struct {
	mu      sync.Mutex
	nodes   map[string]*memNode
	fail    map[string]error
	once    map[string]error
	exports int
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

func (m *memDrive) Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error) {
	if err := m.check(ctx, "Export"); err != nil {
		return nil, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.exports++
	n, ok := m.nodes[fileID]
	if !ok {
		return nil, 0, vfs.ErrNotExist
	}
	body := n.export
	if len(body) == 0 {
		body = n.body
	}
	_ = mimeType
	cp := append([]byte(nil), body...)
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), nil
}

func (m *memDrive) PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, size int64) (vfs.DriveMeta, error) {
	if err := m.check(ctx, "PutMedia"); err != nil {
		return vfs.DriveMeta{}, err
	}
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return vfs.DriveMeta{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[fileID]
	if !ok {
		return vfs.DriveMeta{}, vfs.ErrNotExist
	}
	n.body = data
	if mediaMIME != "" {
		n.meta.MimeType = mediaMIME
	}
	n.meta.Size = int64(len(data))
	n.meta.ModTime = time.Now().UTC()
	return n.meta, nil
}

func (m *memDrive) Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (vfs.DriveMeta, error) {
	if err := m.check(ctx, "Create"); err != nil {
		return vfs.DriveMeta{}, err
	}
	var data []byte
	if r != nil && mediaMIME != "" {
		var err error
		data, err = io.ReadAll(io.LimitReader(r, size+1))
		if err != nil {
			return vfs.DriveMeta{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := "id-" + name + "-" + time.Now().UTC().Format("150405.000000")
	meta := vfs.DriveMeta{
		ID: id, Name: name, MimeType: metadataMIME, Size: int64(len(data)),
		ModTime: time.Now().UTC(),
		IsDir:   metadataMIME == "application/vnd.google-apps.folder",
	}
	m.nodes[id] = &memNode{meta: meta, parent: parentID, body: data}
	_ = mediaMIME
	return meta, nil
}

func (m *memDrive) Trash(ctx context.Context, fileID string) error {
	if err := m.check(ctx, "Trash"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[fileID]
	if !ok {
		return vfs.ErrNotExist
	}
	n.trashed = true
	delete(m.nodes, fileID)
	return nil
}

func (m *memDrive) Mkdir(ctx context.Context, parentID, name string) (vfs.DriveMeta, error) {
	return m.Create(ctx, parentID, name, "application/vnd.google-apps.folder", "", nil, 0)
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
