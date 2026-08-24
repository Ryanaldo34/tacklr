package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// graphItem is one Graph file or folder (IDs stay inside the provider).
type graphItem struct {
	ID, Name, Mime, ParentID string
	Size                     int64
	IsDir                    bool
	LastModified             string
}

// GraphFactory opens OneDrive / SharePoint library providers. Auth is session-scoped.
// Base and HTTP are for tests: point the official client at testhttp.Server.
//
// Account is organization (default) or personal. Organization mounts require
// siteId (SharePoint library) or driveId. Personal mounts use /me/drive when
// those params are empty. A bind can override with params["account"].
type GraphFactory struct {
	ID      string
	Auth    *SessionAuth
	Account string       // organization (default) or personal
	Base    string       // optional Graph root URL (tests)
	HTTP    *http.Client // optional; tests inject testhttp.Server.Client
}

// Profile implements ProviderFactory.
func (f GraphFactory) Profile() string { return f.ID }

// TokenAuth implements TokenSource.
func (f GraphFactory) TokenAuth() *SessionAuth { return f.Auth }

var _ TokenSource = GraphFactory{}

// Open implements ProviderFactory.
func (f GraphFactory) Open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.ID == "" {
		return nil, fmt.Errorf("vfs: msgraph factory needs id")
	}
	var holder *TokenHolder
	if f.Auth != nil {
		holder = f.Auth.Holder(sessionID, f.ID)
	}
	if holder == nil || holder.Current().Token == "" {
		return nil, fmt.Errorf("vfs: msgraph access token required")
	}
	sdk, err := newGraphSDK(holder, f.Base, f.HTTP)
	if err != nil {
		return nil, err
	}
	account, err := graphAccount(f.Account, spec.Params[ParamAccount])
	if err != nil {
		return nil, err
	}
	driveID := strings.TrimSpace(spec.Params[ParamDriveID])
	itemID := strings.TrimSpace(spec.Params[ParamItemID])
	siteID := strings.TrimSpace(spec.Params[ParamSiteID])
	if account == AccountOrganization && driveID == "" && siteID == "" {
		return nil, fmt.Errorf("vfs: msgraph organization account requires siteId or driveId")
	}
	driveID, itemID, err = sdk.resolveRoot(ctx, driveID, itemID, siteID, account)
	if err != nil {
		return nil, err
	}
	return &graphProvider{
		api: sdk, driveID: driveID, rootID: itemID, holder: holder, writable: !spec.ReadOnly,
	}, nil
}

type graphProvider struct {
	api      *graphSDK
	driveID  string
	rootID   string
	holder   *TokenHolder
	writable bool
}

var (
	_ documentBackend = (*graphProvider)(nil)
	_ filePutter      = (*graphProvider)(nil)
)

func graphAccount(factory, param string) (string, error) {
	raw := strings.ToLower(strings.TrimSpace(param))
	if raw == "" {
		raw = strings.ToLower(strings.TrimSpace(factory))
	}
	switch raw {
	case "", AccountOrganization, "enterprise", "work", "tenant":
		return AccountOrganization, nil
	case AccountPersonal, "consumer", "msa":
		return AccountPersonal, nil
	default:
		return "", fmt.Errorf("vfs: msgraph account %q is not organization or personal", raw)
	}
}

func (p *graphProvider) call(ctx context.Context, fn func() error) error {
	if err := p.holder.EnsureValid(ctx); err != nil {
		return err
	}
	staleToken := p.holder.Current().Token
	if err := fn(); err == nil || !errors.Is(err, ErrAuthExpired) {
		return err
	}
	if err := p.holder.RefreshIfCurrent(ctx, staleToken); err != nil {
		return err
	}
	return fn()
}

func (p *graphProvider) getItem(ctx context.Context, id string) (graphItem, error) {
	var item graphItem
	err := p.call(ctx, func() error {
		var err error
		item, err = p.api.GetItem(ctx, p.driveID, id)
		return err
	})
	return item, err
}

func (p *graphProvider) listChildren(ctx context.Context, id string) ([]graphItem, error) {
	var kids []graphItem
	err := p.call(ctx, func() error {
		var err error
		kids, err = p.api.ListChildren(ctx, p.driveID, id)
		return err
	})
	return kids, err
}

func (p *graphProvider) getByPath(ctx context.Context, id, rel string) (graphItem, error) {
	if rel == "" {
		return p.getItem(ctx, id)
	}
	var item graphItem
	err := p.call(ctx, func() error {
		var err error
		item, err = p.api.GetByPath(ctx, p.driveID, id, rel)
		return err
	})
	return item, err
}

func (p *graphProvider) lookupChild(ctx context.Context, parentID, name string) (graphItem, error) {
	return p.getByPath(ctx, parentID, name)
}

func (p *graphProvider) resolve(ctx context.Context, name string) (graphItem, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return graphItem{}, err
	}
	if rel == "" {
		root, err := p.getItem(ctx, p.rootID)
		if err != nil {
			return graphItem{}, err
		}
		root.Name = "."
		root.IsDir = true
		return root, nil
	}
	return p.getByPath(ctx, p.rootID, rel)
}

func graphFileInfo(item graphItem) FileInfo {
	mt := ""
	if !item.IsDir {
		if item.Mime != "" {
			mt = item.Mime
		} else {
			mt = DetectMediaType(item.Name, nil)
		}
	}
	mode := fs.FileMode(0o644)
	if item.IsDir {
		mode = fs.ModeDir | 0o755
	}
	mod := time.Time{}
	if item.LastModified != "" {
		if t, err := time.Parse(time.RFC3339, item.LastModified); err == nil {
			mod = t
		}
	}
	return FileInfo{
		Name:      item.Name,
		Size:      item.Size,
		Mode:      mode,
		ModTime:   mod,
		IsDir:     item.IsDir,
		MediaType: mt,
	}
}

func (p *graphProvider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	item, err := p.getItem(ctx, p.rootID)
	if err != nil {
		return err
	}
	if !item.IsDir {
		return fmt.Errorf("vfs: msgraph itemId is not a folder")
	}
	return nil
}

func (p *graphProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	item, err := p.resolve(ctx, name)
	if err != nil {
		return FileInfo{}, err
	}
	return graphFileInfo(item), nil
}

func (p *graphProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = perm
	if openWrites(flag) {
		if !p.writable {
			return nil, ErrReadOnly
		}
		return nil, ErrNotSupported
	}
	item, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if item.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	data, err := p.readBytes(ctx, item)
	if err != nil {
		return nil, err
	}
	fi := graphFileInfo(item)
	fi.Size = int64(len(data))
	return &s3ReadFile{Reader: bytes.NewReader(data), info: fi}, nil
}

func (p *graphProvider) readBytes(ctx context.Context, item graphItem) ([]byte, error) {
	if item.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	body, _, err := p.getContent(ctx, item)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := readCapped(body, MaxReadFileBytes, item.Size)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return data, nil
}

func (p *graphProvider) getContent(ctx context.Context, item graphItem) (io.ReadCloser, int64, error) {
	if item.Mime == "" && !item.IsDir {
		// OneNote / loop / shortcut: listed, no file bytes.
		return nil, 0, ErrNoCodec
	}
	var body io.ReadCloser
	var size int64
	err := p.call(ctx, func() error {
		var err error
		body, size, err = p.api.GetContent(ctx, p.driveID, item.ID)
		return err
	})
	return body, size, err
}

func (p *graphProvider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if !item.IsDir {
		return nil, ErrNotDir
	}
	kids, err := p.listChildren(ctx, item.ID)
	if err != nil {
		return nil, err
	}
	out := make([]DirEntry, 0, len(kids))
	for _, k := range kids {
		typ := fs.FileMode(0)
		if k.IsDir {
			typ = fs.ModeDir
		}
		out = append(out, DirEntry{Name: k.Name, IsDir: k.IsDir, Type: typ})
	}
	return out, nil
}

func (p *graphProvider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rel, err := cleanRel(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return ErrInvalidPath
	}
	item, err := p.resolve(ctx, name)
	if err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	return p.call(ctx, func() error { return p.api.Delete(ctx, p.driveID, item.ID) })
}

func (p *graphProvider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = perm
	rel, err := cleanRel(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}
	if !p.writable {
		return ErrReadOnly
	}
	_, err = p.ensureDir(ctx, rel)
	return err
}

func (p *graphProvider) ensureDir(ctx context.Context, rel string) (graphItem, error) {
	root, err := p.getItem(ctx, p.rootID)
	if err != nil {
		return graphItem{}, err
	}
	if rel == "" {
		return root, nil
	}
	parentID := root.ID
	var cur graphItem
	for _, part := range strings.Split(rel, "/") {
		child, err := p.lookupChild(ctx, parentID, part)
		if err == nil {
			if !child.IsDir {
				return graphItem{}, fmt.Errorf("%w: %q is not a folder", ErrNotSupported, part)
			}
			cur = child
			parentID = child.ID
			continue
		}
		if !errors.Is(err, ErrNotExist) {
			return graphItem{}, err
		}
		var created graphItem
		err = p.call(ctx, func() error {
			var e error
			created, e = p.api.CreateFolder(ctx, p.driveID, parentID, part)
			return e
		})
		if err != nil {
			return graphItem{}, err
		}
		cur = created
		parentID = created.ID
	}
	return cur, nil
}

func (p *graphProvider) parentAndLeaf(ctx context.Context, name string) (parentID, leaf string, err error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", "", err
	}
	if rel == "" {
		return "", "", ErrInvalidPath
	}
	dir, leaf := path.Split(rel)
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		root, err := p.getItem(ctx, p.rootID)
		if err != nil {
			return "", "", err
		}
		return root.ID, leaf, nil
	}
	parent, err := p.ensureDir(ctx, dir)
	if err != nil {
		return "", "", err
	}
	return parent.ID, leaf, nil
}

func (p *graphProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if item.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	data, err := p.readBytes(ctx, item)
	if err != nil {
		return nil, err
	}
	fi := graphFileInfo(item)
	fi.Size = int64(len(data))
	return decodeProviderDocument(ctx, name, fi, data, reg)
}

func (p *graphProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	if size > int64(MaxReadFileBytes) {
		return errFileExceeds(MaxReadFileBytes)
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
	if int64(len(body)) > int64(MaxReadFileBytes) {
		return errFileExceeds(MaxReadFileBytes)
	}
	item, err := p.resolve(ctx, name)
	if err == nil {
		return p.call(ctx, func() error {
			_, e := p.api.PutContent(ctx, p.driveID, item.ID, item.Name, item.ParentID, bytes.NewReader(body), int64(len(body)))
			return e
		})
	}
	if !errors.Is(err, ErrNotExist) {
		return err
	}
	parentID, leaf, err := p.parentAndLeaf(ctx, name)
	if err != nil {
		return err
	}
	return p.call(ctx, func() error {
		_, e := p.api.PutContent(ctx, p.driveID, "", leaf, parentID, bytes.NewReader(body), int64(len(body)))
		return e
	})
}

func (p *graphProvider) WriteDocument(ctx context.Context, name string, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	data, err := EncodeDocument(ctx, doc)
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}
