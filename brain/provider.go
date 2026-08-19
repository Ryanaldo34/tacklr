package brain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/vfs"
)

// Mount layout and factory defaults.
const (
	DefaultProfile    = "brain"
	DefaultMountPoint = "/engram"
	ModePrefix        = "prefix"
	ModeRoots         = "roots"
	// MaxEngramReadDir caps Provider ReadDir / ListByKind listings (paginate later).
	MaxEngramReadDir = 500
)

// MountForKind selects the roots mount for kind, then a prefix mount, then any
// brain mount. Harness tool adapters use this canonical layout resolver.
func MountForKind(specs []vfs.MountSpec, kind string) (vfs.MountSpec, bool) {
	var prefix vfs.MountSpec
	var hasPrefix bool
	for _, spec := range specs {
		if spec.Profile != DefaultProfile {
			continue
		}
		mode := spec.Params["mode"]
		if mode == ModeRoots && kind != "" && spec.Params["kind"] == kind {
			return spec, true
		}
		if mode != ModeRoots && !hasPrefix {
			prefix, hasPrefix = spec, true
		}
	}
	if hasPrefix {
		return prefix, true
	}
	for _, spec := range specs {
		if spec.Profile == DefaultProfile {
			return spec, true
		}
	}
	return vfs.MountSpec{}, false
}

// BrainFactory opens a vfs.Provider over Engine objects (Engrams as Markdown files).
// Profile() is ID or DefaultProfile ("brain"). vfs stays brain-free.
type BrainFactory struct {
	ID     string
	Engine *Engine
	Scope  Scope
	Mode   string   // roots | prefix; MountSpec.Params["mode"] wins; default prefix
	Kinds  []string // optional allow-list; Params["kinds"] wins
}

// Profile implements vfs.ProviderFactory.
func (f BrainFactory) Profile() string {
	if id := strings.TrimSpace(f.ID); id != "" {
		return id
	}
	return DefaultProfile
}

// Open implements vfs.ProviderFactory.
func (f BrainFactory) Open(_ context.Context, _ string, spec vfs.MountSpec) (vfs.Provider, error) {
	return newEngramProvider(f, spec)
}

type engramProvider struct {
	eng   *Engine
	scope Scope
	mode  string
	point string
	kind  string   // roots: single kind name
	allow []string // allow-list (canonical kind names); empty = catalog parents / open
}

func newEngramProvider(f BrainFactory, spec vfs.MountSpec) (*engramProvider, error) {
	if f.Engine == nil {
		return nil, fmt.Errorf("brain: engine is required")
	}
	if f.Scope.Namespace == nil || *f.Scope.Namespace == uuid.Nil {
		return nil, fmt.Errorf("brain: namespace is required")
	}
	ns := *f.Scope.Namespace
	p := &engramProvider{
		eng:   f.Engine,
		scope: Scope{Namespace: &ns},
		point: spec.Point,
		mode:  strings.ToLower(strings.TrimSpace(paramOr(spec.Params, "mode", f.Mode))),
	}
	if p.point == "" {
		p.point = DefaultMountPoint
	}
	if p.mode == "" {
		p.mode = ModePrefix
	}
	if p.mode != ModePrefix && p.mode != ModeRoots {
		return nil, fmt.Errorf("brain: mode must be %s or %s", ModePrefix, ModeRoots)
	}
	if kinds := paramOr(spec.Params, "kinds", strings.Join(f.Kinds, ",")); kinds != "" {
		for _, k := range strings.Split(kinds, ",") {
			k = strings.TrimSpace(k)
			if k != "" {
				p.allow = append(p.allow, k)
			}
		}
	}
	if p.mode == ModeRoots {
		p.kind = strings.TrimSpace(paramOr(spec.Params, "kind", ""))
		if p.kind == "" && len(p.allow) == 1 {
			p.kind = p.allow[0]
		}
		if p.kind == "" {
			return nil, fmt.Errorf("brain: roots mode requires kind=")
		}
	}
	return p, nil
}

func paramOr(params map[string]string, key, fallback string) string {
	if params != nil {
		if v := strings.TrimSpace(params[key]); v != "" {
			return v
		}
	}
	return fallback
}

// Validate implements vfs.Provider.
func (p *engramProvider) Validate(ctx context.Context) error {
	return ctx.Err()
}

// Stat implements vfs.Provider.
func (p *engramProvider) Stat(ctx context.Context, name string) (vfs.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return vfs.FileInfo{}, err
	}
	kind, slug, isDir, err := p.parseRel(name)
	if err != nil {
		return vfs.FileInfo{}, err
	}
	base := path.Base(name)
	if name == "" || name == "." {
		base = path.Base(p.point)
	}
	if isDir {
		if !p.dirExists(ctx, kind, name) {
			return vfs.FileInfo{}, vfs.ErrNotExist
		}
		return vfs.FileInfo{Name: base, IsDir: true, Mode: fs.ModeDir, ModTime: time.Now().UTC()}, nil
	}
	obj, err := p.lookupFile(ctx, kind, slug)
	if err != nil {
		return vfs.FileInfo{}, err
	}
	raw, err := FormatEngram(EngramFromObject(obj))
	if err != nil {
		return vfs.FileInfo{}, err
	}
	return vfs.FileInfo{Name: slug + ".md", Size: int64(len(raw)), ModTime: obj.UpdatedAt, MediaType: engramContentType}, nil
}

// OpenFile implements vfs.Provider.
func (p *engramProvider) OpenFile(ctx context.Context, name string, flag int, _ fs.FileMode) (vfs.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, slug, isDir, err := p.parseRel(name)
	if err != nil {
		return nil, err
	}
	if isDir {
		return nil, fmt.Errorf("brain: cannot open a directory")
	}
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0
	if write {
		var buf bytes.Buffer
		if flag&os.O_TRUNC == 0 && flag&os.O_CREATE == 0 {
			if obj, err := p.lookupFile(ctx, kind, slug); err == nil {
				raw, err := FormatEngram(EngramFromObject(obj))
				if err != nil {
					return nil, err
				}
				buf.Write(raw)
			}
		}
		return &engramWriteFile{
			buf:    buf,
			name:   slug + ".md",
			commit: func(b []byte) error { return p.commit(ctx, name, b) },
		}, nil
	}
	obj, err := p.lookupFile(ctx, kind, slug)
	if err != nil {
		return nil, err
	}
	raw, err := FormatEngram(EngramFromObject(obj))
	if err != nil {
		return nil, err
	}
	return &engramReadFile{Reader: bytes.NewReader(raw), name: slug + ".md"}, nil
}

// PutFile implements the session filePutter hook (write-through).
func (p *engramProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var data []byte
	if size > 0 {
		data = make([]byte, size)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
	}
	return p.commit(ctx, name, data)
}

// OpenDocument translates a brain Object into Textual IR.
func (p *engramProvider) OpenDocument(ctx context.Context, name string, _ *vfs.ContentRegistry) (vfs.Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, slug, isDir, err := p.parseRel(name)
	if err != nil {
		return nil, err
	}
	if isDir {
		return nil, fmt.Errorf("%w: %s", vfs.ErrIsDir, name)
	}
	obj, err := p.lookupFile(ctx, kind, slug)
	if err != nil {
		return nil, err
	}
	raw, err := FormatEngram(EngramFromObject(obj))
	if err != nil {
		return nil, err
	}
	return vfs.NewTextDocument(name, engramContentType, "utf-8", string(raw)), nil
}

// WriteDocument translates Textual IR into a brain Object and Puts immediately.
func (p *engramProvider) WriteDocument(ctx context.Context, name string, doc vfs.Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t, ok := doc.(vfs.Textual)
	if !ok {
		return vfs.ErrNotTextual
	}
	raw, err := vfs.EncodeTextual(t)
	if err != nil {
		return err
	}
	return p.commit(ctx, name, raw)
}

// ReadDir implements vfs.Provider.
func (p *engramProvider) ReadDir(ctx context.Context, name string) ([]vfs.DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, _, isDir, err := p.parseRel(name)
	if err != nil {
		return nil, err
	}
	if !isDir {
		return nil, vfs.ErrNotDir
	}
	if name == "" || name == "." {
		if p.mode == ModeRoots {
			return p.listFiles(ctx, p.kind)
		}
		return p.listKindDirs(ctx)
	}
	return p.listFiles(ctx, kind)
}

// Remove implements vfs.Provider (SoftDelete).
func (p *engramProvider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kind, slug, isDir, err := p.parseRel(name)
	if err != nil {
		return err
	}
	if isDir {
		return fmt.Errorf("brain: cannot remove a kind directory")
	}
	obj, err := p.lookupFile(ctx, kind, slug)
	if err != nil {
		return err
	}
	return p.eng.SoftDelete(ctx, p.scope, obj.ID)
}

// MkdirAll implements vfs.Provider: prefix kind segment is a no-op; arbitrary dirs fail.
func (p *engramProvider) MkdirAll(ctx context.Context, name string, _ fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" || name == "." {
		return nil
	}
	kind, _, isDir, err := p.parseRel(name)
	if err != nil {
		return err
	}
	if !isDir || (p.mode == ModePrefix && kind == "") {
		return fmt.Errorf("brain: cannot create arbitrary directories")
	}
	if p.mode == ModeRoots {
		return fmt.Errorf("brain: cannot create arbitrary directories")
	}
	if !p.kindAllowed(kind) {
		return fmt.Errorf("brain: unknown kind directory %q", name)
	}
	return nil
}

func (p *engramProvider) parseRel(name string) (kind, slug string, isDir bool, err error) {
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	if name == "" || name == "." {
		return p.kind, "", true, nil
	}
	if strings.Contains(name, "..") {
		return "", "", false, vfs.ErrInvalidPath
	}
	switch p.mode {
	case ModeRoots:
		if strings.Contains(name, "/") {
			return "", "", false, vfs.ErrNotExist
		}
		if !strings.HasSuffix(name, ".md") {
			return "", "", false, vfs.ErrNotExist
		}
		return p.kind, strings.TrimSuffix(name, ".md"), false, nil
	default:
		parts := strings.Split(name, "/")
		if len(parts) == 1 {
			k, ok := p.resolveKindSlug(parts[0])
			if !ok {
				return "", "", false, vfs.ErrNotExist
			}
			return k, "", true, nil
		}
		if len(parts) == 2 && strings.HasSuffix(parts[1], ".md") {
			k, ok := p.resolveKindSlug(parts[0])
			if !ok {
				// Let commit reject with "not allowed" (dir listing stays ErrNotExist).
				return parts[0], strings.TrimSuffix(parts[1], ".md"), false, nil
			}
			return k, strings.TrimSuffix(parts[1], ".md"), false, nil
		}
		return "", "", false, vfs.ErrNotExist
	}
}

func (p *engramProvider) resolveKindSlug(seg string) (string, bool) {
	seg = strings.TrimSpace(seg)
	if seg == "" {
		return "", false
	}
	want := KindSlug(seg)
	for _, k := range p.allowedKinds() {
		if KindSlug(k) == want {
			return k, true
		}
	}
	// Open / empty catalog: treat the segment as the kind name when allowed is open.
	if p.openKinds() {
		return seg, true
	}
	return "", false
}

func (p *engramProvider) openKinds() bool {
	if len(p.allow) > 0 {
		return false
	}
	if p.eng.Catalog() != nil && !p.eng.Catalog().Empty() {
		return false
	}
	return true
}

func (p *engramProvider) allowedKinds() []string {
	if len(p.allow) > 0 {
		return p.allow
	}
	if cat := p.eng.Catalog(); cat != nil && !cat.Empty() {
		var out []string
		for _, spec := range cat.All() {
			if IsParentKind(spec) {
				out = append(out, spec.Kind)
			}
		}
		return out
	}
	return nil
}

func (p *engramProvider) kindAllowed(kind string) bool {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return false
	}
	if p.openKinds() {
		return true
	}
	_, ok := p.resolveKindSlug(KindSlug(kind))
	return ok
}

func (p *engramProvider) dirExists(ctx context.Context, kind, name string) bool {
	if name == "" || name == "." {
		return true
	}
	if p.mode == ModeRoots {
		return false
	}
	return p.kindAllowed(kind)
}

// EngramPath is the virtual path for an Engram file (prefix or roots).
func EngramPath(point, mode, kind, slug string) string {
	file := slug + ".md"
	if mode == ModeRoots {
		return path.Join(point, file)
	}
	return path.Join(point, KindSlug(kind), file)
}

func (p *engramProvider) lookupFile(ctx context.Context, kind, slug string) (Object, error) {
	vpath := EngramPath(p.point, p.mode, kind, slug)
	obj, err := p.eng.GetByProperty(ctx, p.scope, PropVFSPath, vpath)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Object{}, vfs.ErrNotExist
		}
		return Object{}, fmt.Errorf("brain: lookup %s: %w", vpath, err)
	}
	if obj.IsPart() {
		return Object{}, vfs.ErrNotExist
	}
	return obj, nil
}

func (p *engramProvider) listFiles(ctx context.Context, kind string) ([]vfs.DirEntry, error) {
	if !p.kindAllowed(kind) {
		return nil, vfs.ErrNotExist
	}
	objs, err := p.eng.ListByKind(ctx, p.scope, kind, MaxEngramReadDir)
	if err != nil {
		return nil, err
	}
	out := make([]vfs.DirEntry, 0, len(objs))
	for _, obj := range objs {
		if obj.IsPart() {
			continue
		}
		slug, _ := obj.Properties[PropSlug].(string)
		if slug == "" {
			slug = Slugify(obj.Title)
		}
		if slug == "" {
			continue
		}
		out = append(out, vfs.DirEntry{Name: slug + ".md"})
		if len(out) >= MaxEngramReadDir {
			break
		}
	}
	return out, nil
}

func (p *engramProvider) listKindDirs(ctx context.Context) ([]vfs.DirEntry, error) {
	names := p.allowedKinds()
	if len(names) == 0 {
		// Empty catalog: require kinds= or list kinds that already have objects.
		if len(p.allow) == 0 {
			var err error
			names, err = p.eng.KindsWithObjects(ctx, p.scope)
			if err != nil {
				return nil, err
			}
		}
	}
	out := make([]vfs.DirEntry, 0, len(names))
	seen := map[string]struct{}{}
	for _, k := range names {
		if cat := p.eng.Catalog(); cat != nil {
			if spec, ok := cat.Get(k); ok && !IsParentKind(spec) {
				continue
			}
		}
		seg := KindSlug(k)
		if seg == "" {
			continue
		}
		if _, dup := seen[seg]; dup {
			continue
		}
		seen[seg] = struct{}{}
		out = append(out, vfs.DirEntry{Name: seg, IsDir: true, Type: fs.ModeDir})
		if len(out) >= MaxEngramReadDir {
			break
		}
	}
	return out, nil
}

func (p *engramProvider) commit(ctx context.Context, rel string, data []byte) error {
	kind, slug, isDir, err := p.parseRel(rel)
	if err != nil {
		return err
	}
	if isDir {
		return fmt.Errorf("brain: cannot write a directory")
	}
	f, err := ParseEngram(data)
	if err != nil {
		return err
	}
	if f.Kind == "" {
		f.Kind = kind
	}
	if f.Slug == "" {
		f.Slug = slug
	}
	if f.Title == "" {
		f.Title = f.Slug
	}
	if kind != "" && f.Kind != "" && KindSlug(kind) != KindSlug(f.Kind) {
		return fmt.Errorf("brain: engram kind %q does not match path kind %q", f.Kind, kind)
	}
	if canon, ok := p.resolveKindSlug(KindSlug(f.Kind)); ok {
		f.Kind = canon
	} else if !p.openKinds() {
		return fmt.Errorf("brain: kind %q is not allowed on this mount", f.Kind)
	}
	obj := ObjectFromEngram(f)
	obj.NamespaceID = *p.scope.Namespace
	vpath := EngramPath(p.point, p.mode, obj.Kind, f.Slug)
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	obj.Properties[PropVFSPath] = vpath
	obj.Properties[PropSlug] = f.Slug
	if obj.ID == uuid.Nil {
		existing, err := p.eng.GetByProperty(ctx, p.scope, PropVFSPath, vpath)
		if err == nil {
			obj.ID = existing.ID
			obj.CreatedAt = existing.CreatedAt
		} else if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("brain: lookup %s: %w", vpath, err)
		}
	}
	_, err = p.eng.Put(ctx, p.scope, obj)
	return err
}

type engramReadFile struct {
	*bytes.Reader
	name string
}

func (f *engramReadFile) Close() error { return nil }

func (f *engramReadFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: f.Size(), ModTime: time.Now().UTC()}, nil
}

type engramWriteFile struct {
	buf    bytes.Buffer
	name   string
	commit func([]byte) error
	closed bool
}

func (f *engramWriteFile) Write(p []byte) (int, error) { return f.buf.Write(p) }

func (f *engramWriteFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if f.commit != nil {
		return f.commit(f.buf.Bytes())
	}
	return nil
}

func (f *engramWriteFile) Stat() (vfs.FileInfo, error) {
	return vfs.FileInfo{Name: f.name, Size: int64(f.buf.Len()), ModTime: time.Now().UTC()}, nil
}

var (
	_ vfs.Provider        = (*engramProvider)(nil)
	_ vfs.ProviderFactory = BrainFactory{}
	_ vfs.File            = (*engramReadFile)(nil)
	_ vfs.File            = (*engramWriteFile)(nil)
)
