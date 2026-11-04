package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// GraphAPI is the OneDrive/SharePoint subset the provider uses.
// NewMicrosoftGraph implements it over the Graph REST API (no Microsoft SDK).
type GraphAPI interface {
	// ResolveRoot is the drive and folder a binding opens: driveID, else the
	// drive of siteID, else the user's own drive (/me/drive). An empty
	// itemID is the drive root. It fails when the item is not a folder.
	ResolveRoot(ctx context.Context, driveID, itemID, siteID string) (driveIDOut, itemIDOut string, err error)
	GetItem(ctx context.Context, driveID, itemID string) (GraphItem, error)
	GetByPath(ctx context.Context, driveID, itemID, rel string) (GraphItem, error)
	ListChildren(ctx context.Context, driveID, itemID string) ([]GraphItem, error)
	// GetContent opens n bytes of the item's content from off. n < 0 reads
	// to the end; n is never 0.
	GetContent(ctx context.Context, driveID, itemID string, off, n int64) (io.ReadCloser, error)
	PutContent(ctx context.Context, driveID, itemID, name, parentID string, r io.Reader, size int64) (GraphItem, error)
	CreateFolder(ctx context.Context, driveID, parentID, name string) (GraphItem, error)
	Delete(ctx context.Context, driveID, itemID string) error
}

// GraphItem is one Graph file or folder (IDs stay inside the provider).
type GraphItem struct {
	ID, Name, Mime, ParentID string
	Size                     int64
	IsDir                    bool
	LastModified             time.Time
	CTag                     string // changes only when the content changes
}

// Graph opens OneDrive / SharePoint. api is a NewMicrosoftGraph result or
// another GraphAPI.
func Graph(api GraphAPI, holder *TokenHolder, account string) Open {
	if api == nil {
		panic("vfs: Graph requires a GraphAPI client")
	}
	return func(ctx context.Context, _ string, b mount.Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h := holder
		if h == nil && strings.TrimSpace(b.Auth.Token) != "" {
			h = NewTokenHolder(b.Auth)
		}
		if h == nil || h.Current().Token == "" {
			return nil, fmt.Errorf("vfs: msgraph access token required")
		}
		acct, err := graphAccount(account, b.Params[mount.ParamAccount])
		if err != nil {
			return nil, err
		}
		driveID := strings.TrimSpace(b.Params[mount.ParamDriveID])
		itemID := strings.TrimSpace(b.Params[mount.ParamItemID])
		siteID := strings.TrimSpace(b.Params[mount.ParamSiteID])
		if acct == AccountOrganization && driveID == "" && siteID == "" {
			return nil, fmt.Errorf("vfs: msgraph organization account requires siteId or driveId")
		}
		driveID, itemID, err = api.ResolveRoot(ctx, driveID, itemID, siteID)
		if err != nil {
			return nil, err
		}
		return &graphProvider{
			api: api, driveID: driveID, rootID: itemID, holder: h, writable: b.Writable,
		}, nil
	}
}

type graphProvider struct {
	api      GraphAPI
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

func (p *graphProvider) getItem(ctx context.Context, id string) (GraphItem, error) {
	var item GraphItem
	err := p.call(ctx, func() error {
		var err error
		item, err = p.api.GetItem(ctx, p.driveID, id)
		return err
	})
	return item, err
}

func (p *graphProvider) listChildren(ctx context.Context, id string) ([]GraphItem, error) {
	var kids []GraphItem
	err := p.call(ctx, func() error {
		var err error
		kids, err = p.api.ListChildren(ctx, p.driveID, id)
		return err
	})
	return kids, err
}

// getByPath is the item at rel, a non-empty path under id.
func (p *graphProvider) getByPath(ctx context.Context, id, rel string) (GraphItem, error) {
	var item GraphItem
	err := p.call(ctx, func() error {
		var err error
		item, err = p.api.GetByPath(ctx, p.driveID, id, rel)
		return err
	})
	return item, err
}

func (p *graphProvider) lookupChild(ctx context.Context, parentID, name string) (GraphItem, error) {
	return p.getByPath(ctx, parentID, name)
}

func (p *graphProvider) resolve(ctx context.Context, name string) (GraphItem, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return GraphItem{}, err
	}
	if rel == "" {
		root, err := p.getItem(ctx, p.rootID)
		if err != nil {
			return GraphItem{}, err
		}
		root.Name = "."
		root.IsDir = true
		return root, nil
	}
	return p.getByPath(ctx, p.rootID, rel)
}

func graphFileInfo(item GraphItem) FileInfo {
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
	return FileInfo{
		Name:      item.Name,
		Size:      item.Size,
		Mode:      mode,
		ModTime:   item.LastModified,
		IsDir:     item.IsDir,
		MediaType: mt,
		Version:   graphVersion(item),
	}
}

// graphVersion is the item's cTag. Folders have no version.
func graphVersion(item GraphItem) string {
	if item.IsDir {
		return ""
	}
	return item.CTag
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
	if !item.hasContent() {
		return nil, ErrNoCodec
	}
	return newRemoteFile(ctx, graphFileInfo(item), func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		return p.getContent(ctx, item, off, n)
	}), nil
}

func (p *graphProvider) readBytes(ctx context.Context, item GraphItem) ([]byte, error) {
	if item.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	if !item.hasContent() {
		return nil, ErrNoCodec
	}
	body, err := p.getContent(ctx, item, 0, -1)
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

// hasContent reports whether the item has file bytes. OneNote, Loop and
// shortcut items are listed with no MIME type and no content.
func (item GraphItem) hasContent() bool {
	return item.Mime != "" || item.IsDir
}

func (p *graphProvider) getContent(ctx context.Context, item GraphItem, off, n int64) (io.ReadCloser, error) {
	var body io.ReadCloser
	err := p.call(ctx, func() error {
		var err error
		body, err = p.api.GetContent(ctx, p.driveID, item.ID, off, n)
		return err
	})
	return body, err
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

// ensureDir makes every directory of rel, a non-empty path under the root.
func (p *graphProvider) ensureDir(ctx context.Context, rel string) (GraphItem, error) {
	root, err := p.getItem(ctx, p.rootID)
	if err != nil {
		return GraphItem{}, err
	}
	parentID := root.ID
	var cur GraphItem
	for _, part := range strings.Split(rel, "/") {
		child, err := p.lookupChild(ctx, parentID, part)
		if err == nil {
			if !child.IsDir {
				return GraphItem{}, fmt.Errorf("%w: %q is not a folder", ErrNotSupported, part)
			}
			cur = child
			parentID = child.ID
			continue
		}
		if !errors.Is(err, ErrNotExist) {
			return GraphItem{}, err
		}
		var created GraphItem
		err = p.call(ctx, func() error {
			var e error
			created, e = p.api.CreateFolder(ctx, p.driveID, parentID, part)
			return e
		})
		if err != nil {
			return GraphItem{}, err
		}
		cur = created
		parentID = created.ID
	}
	return cur, nil
}

// parentAndLeaf is name's parent folder, made if missing, and its last
// element. The caller resolved name, so it is clean and not the root.
func (p *graphProvider) parentAndLeaf(ctx context.Context, name string) (parentID, leaf string, err error) {
	rel, _ := cleanRel(name)
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

func (p *graphProvider) WriteDocument(ctx context.Context, name string, doc Document, reg *ContentRegistry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		return ErrReadOnly
	}
	data, err := reg.Encode(ctx, doc)
	if err != nil {
		return err
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}
