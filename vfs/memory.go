package vfs

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

// MemoryFactory provides an ephemeral, session-scoped backend. It is intended
// for attachments and generated context, not durable user files.
type MemoryFactory struct {
	ID        string
	mu        sync.Mutex
	bySession map[string]*memoryProvider
}

func (f *MemoryFactory) Profile() string { return f.ID }

func (f *MemoryFactory) Open(_ context.Context, sessionID string, _ MountSpec) (Provider, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bySession == nil {
		f.bySession = make(map[string]*memoryProvider)
	}
	if p := f.bySession[sessionID]; p != nil {
		return p, nil
	}
	p := &memoryProvider{files: map[string][]byte{"": nil}, dirs: map[string]bool{"": true}}
	f.bySession[sessionID] = p
	return p, nil
}

type memoryProvider struct {
	mu    sync.RWMutex
	files map[string][]byte
	dirs  map[string]bool
}

func (*memoryProvider) Validate(context.Context) error { return nil }
func (p *memoryProvider) Stat(_ context.Context, name string) (FileInfo, error) {
	name = cleanMemoryPath(name)
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.dirs[name] {
		return FileInfo{Name: path.Base(name), Mode: fs.ModeDir | 0o755, IsDir: true}, nil
	}
	b, ok := p.files[name]
	if !ok {
		return FileInfo{}, ErrNotExist
	}
	return FileInfo{Name: path.Base(name), Size: int64(len(b)), Mode: 0o644, ModTime: time.Time{}}, nil
}

func (p *memoryProvider) OpenFile(_ context.Context, name string, flag int, _ fs.FileMode) (File, error) {
	name = cleanMemoryPath(name)
	p.mu.Lock()
	if p.dirs[name] {
		p.mu.Unlock()
		return nil, ErrIsDir
	}
	b, ok := p.files[name]
	if !ok && flag&os.O_CREATE == 0 {
		p.mu.Unlock()
		return nil, ErrNotExist
	}
	if !ok {
		b = nil
		p.files[name] = nil
		p.ensureParents(name)
	}
	write := flag&(os.O_WRONLY|os.O_RDWR) != 0
	if flag&os.O_TRUNC != 0 {
		b = nil
		p.files[name] = nil
	}
	p.mu.Unlock()
	return &memoryFile{provider: p, name: name, reader: bytes.NewReader(b), write: write, data: append([]byte(nil), b...)}, nil
}

func (p *memoryProvider) ReadDir(_ context.Context, name string) ([]DirEntry, error) {
	name = cleanMemoryPath(name)
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.dirs[name] {
		return nil, ErrNotDir
	}
	prefix := name + "/"
	if name == "" {
		prefix = ""
	}
	seen := map[string]bool{}
	for d := range p.dirs {
		if strings.HasPrefix(d, prefix) {
			rest := strings.TrimPrefix(d, prefix)
			if rest != "" && !strings.Contains(rest, "/") {
				seen[rest] = true
			}
		}
	}
	for f := range p.files {
		if strings.HasPrefix(f, prefix) {
			rest := strings.TrimPrefix(f, prefix)
			if rest != "" && !strings.Contains(rest, "/") {
				seen[rest] = false
			}
		}
	}
	out := make([]DirEntry, 0, len(seen))
	for n, dir := range seen {
		out = append(out, DirEntry{Name: n, IsDir: dir})
	}
	return out, nil
}

func (p *memoryProvider) Remove(_ context.Context, name string) error {
	name = cleanMemoryPath(name)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.files[name]; ok {
		delete(p.files, name)
		return nil
	}
	if p.dirs[name] {
		delete(p.dirs, name)
		return nil
	}
	return ErrNotExist
}

func (p *memoryProvider) MkdirAll(_ context.Context, name string, _ fs.FileMode) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	name = cleanMemoryPath(name)
	p.dirs[name] = true
	p.ensureParents(name)
	return nil
}
func (p *memoryProvider) ensureParents(name string) {
	name = path.Dir(name)
	for name != "." && name != "" {
		p.dirs[name] = true
		name = path.Dir(name)
	}
}
func cleanMemoryPath(name string) string {
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	if name == "." {
		return ""
	}
	return name
}

type memoryFile struct {
	provider *memoryProvider
	name     string
	reader   *bytes.Reader
	data     []byte
	write    bool
	closed   bool
}

func (f *memoryFile) Read(b []byte) (int, error) {
	if f.write {
		return 0, io.ErrClosedPipe
	}
	return f.reader.Read(b)
}
func (f *memoryFile) Write(b []byte) (int, error) {
	if !f.write {
		return 0, io.ErrClosedPipe
	}
	f.data = append(f.data, b...)
	return len(b), nil
}
func (f *memoryFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if f.write {
		f.provider.mu.Lock()
		f.provider.files[f.name] = append([]byte(nil), f.data...)
		f.provider.ensureParents(f.name)
		f.provider.mu.Unlock()
	}
	return nil
}
func (f *memoryFile) Stat() (FileInfo, error) { return f.provider.Stat(context.Background(), f.name) }
