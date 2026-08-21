package vfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

type memGraphNode struct {
	meta    vfs.GraphItem
	parent  string
	body    []byte
	trashed bool
}

type memGraph struct {
	mu    sync.Mutex
	nodes map[string]*memGraphNode
	fail  map[string]error
	once  map[string]error
}

func newMemGraph(rootID, rootName string) *memGraph {
	g := &memGraph{nodes: map[string]*memGraphNode{}, fail: map[string]error{}, once: map[string]error{}}
	g.nodes[rootID] = &memGraphNode{
		meta: vfs.GraphItem{ID: rootID, Name: rootName, IsDir: true},
	}
	return g
}

func (g *memGraph) add(parent string, item vfs.GraphItem, body []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	item.ParentID = parent
	g.nodes[item.ID] = &memGraphNode{meta: item, parent: parent, body: append([]byte(nil), body...)}
}

func (g *memGraph) check(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err, ok := g.once[op]; ok {
		delete(g.once, op)
		return err
	}
	return g.fail[op]
}

func (g *memGraph) lookup(itemID string) (*memGraphNode, bool) {
	if itemID == "" {
		for _, n := range g.nodes {
			if n.parent == "" {
				return n, true
			}
		}
		return nil, false
	}
	n, ok := g.nodes[itemID]
	return n, ok
}

func (g *memGraph) GetItem(ctx context.Context, driveID, itemID string) (vfs.GraphItem, error) {
	if err := g.check(ctx, "GetItem"); err != nil {
		return vfs.GraphItem{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.lookup(itemID)
	if !ok {
		return vfs.GraphItem{}, vfs.ErrNotExist
	}
	return n.meta, nil
}

func (g *memGraph) GetByPath(ctx context.Context, driveID, itemID, rel string) (vfs.GraphItem, error) {
	if err := g.check(ctx, "GetByPath"); err != nil {
		return vfs.GraphItem{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.lookup(itemID)
	if !ok {
		return vfs.GraphItem{}, vfs.ErrNotExist
	}
	if rel == "" {
		return n.meta, nil
	}
	parentID := n.meta.ID
	var cur vfs.GraphItem
	parts := strings.Split(rel, "/")
	for i, part := range parts {
		found, hits := vfs.GraphItem{}, 0
		for _, c := range g.nodes {
			if c.parent == parentID && c.meta.Name == part {
				found = c.meta
				hits++
				if hits > 1 {
					return vfs.GraphItem{}, fmt.Errorf("%w: %q", vfs.ErrAmbiguous, part)
				}
			}
		}
		if hits == 0 {
			return vfs.GraphItem{}, vfs.ErrNotExist
		}
		if i < len(parts)-1 && !found.IsDir {
			return vfs.GraphItem{}, vfs.ErrNotExist
		}
		cur = found
		parentID = found.ID
	}
	return cur, nil
}

func (g *memGraph) ListChildren(ctx context.Context, driveID, itemID string) ([]vfs.GraphItem, error) {
	if err := g.check(ctx, "ListChildren"); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.nodes[itemID]; !ok {
		return nil, vfs.ErrNotExist
	}
	var out []vfs.GraphItem
	for _, n := range g.nodes {
		if n.parent == itemID {
			out = append(out, n.meta)
		}
	}
	return out, nil
}

func (g *memGraph) GetContent(ctx context.Context, driveID, itemID string) (io.ReadCloser, int64, error) {
	if err := g.check(ctx, "GetContent"); err != nil {
		return nil, 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[itemID]
	if !ok {
		return nil, 0, vfs.ErrNotExist
	}
	cp := append([]byte(nil), n.body...)
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), nil
}

func (g *memGraph) PutContent(ctx context.Context, driveID, itemID, name, parentID string, r io.Reader, size int64) (vfs.GraphItem, error) {
	if err := g.check(ctx, "PutContent"); err != nil {
		return vfs.GraphItem{}, err
	}
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return vfs.GraphItem{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if itemID != "" {
		n, ok := g.nodes[itemID]
		if !ok {
			return vfs.GraphItem{}, vfs.ErrNotExist
		}
		n.body = data
		n.meta.Size = int64(len(data))
		return n.meta, nil
	}
	id := "id-" + name + "-" + time.Now().UTC().Format("150405.000000")
	meta := vfs.GraphItem{ID: id, Name: name, ParentID: parentID, Size: int64(len(data)), Mime: vfs.DetectMediaType(name, nil)}
	g.nodes[id] = &memGraphNode{meta: meta, parent: parentID, body: data}
	return meta, nil
}

func (g *memGraph) CreateFolder(ctx context.Context, driveID, parentID, name string) (vfs.GraphItem, error) {
	if err := g.check(ctx, "CreateFolder"); err != nil {
		return vfs.GraphItem{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	id := "dir-" + name + "-" + time.Now().UTC().Format("150405.000000")
	meta := vfs.GraphItem{ID: id, Name: name, ParentID: parentID, IsDir: true}
	g.nodes[id] = &memGraphNode{meta: meta, parent: parentID}
	return meta, nil
}

func (g *memGraph) Delete(ctx context.Context, driveID, itemID string) error {
	if err := g.check(ctx, "Delete"); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[itemID]
	if !ok {
		return vfs.ErrNotExist
	}
	n.trashed = true
	delete(g.nodes, itemID)
	return nil
}
