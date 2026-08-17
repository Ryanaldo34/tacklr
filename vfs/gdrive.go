package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	mimeGoogleFolder   = "application/vnd.google-apps.folder"
	mimeGoogleShortcut = "application/vnd.google-apps.shortcut"
	driveMetaFields    = "id,name,mimeType,modifiedTime,size,shortcutDetails"
	driveListFields    = "nextPageToken,files(" + driveMetaFields + ")"
)

// DriveMeta is one Drive file or folder (IDs stay inside the provider).
type DriveMeta struct {
	ID         string
	Name       string
	MimeType   string
	Size       int64
	ModTime    time.Time
	IsDir      bool
	TargetID   string
	TargetMime string
}

// DriveAPI is the Drive subset used by the provider. Tests inject a fake.
type DriveAPI interface {
	GetMeta(ctx context.Context, fileID string) (DriveMeta, error)
	GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error)
	List(ctx context.Context, folderID string) ([]DriveMeta, error)
}

// GoogleDrive implements DriveAPI with google.golang.org/api/drive/v3.
type GoogleDrive struct {
	Service *drive.Service
}

// NewGoogleDrive builds a Drive service that reads the live TokenHolder.
func NewGoogleDrive(ctx context.Context, holder *TokenHolder) (*GoogleDrive, error) {
	if holder == nil {
		return nil, fmt.Errorf("vfs: drive token required")
	}
	svc, err := drive.NewService(ctx, option.WithTokenSource(holder))
	if err != nil {
		return nil, fmt.Errorf("vfs: drive service: %w", err)
	}
	return &GoogleDrive{Service: svc}, nil
}

func (g GoogleDrive) require() error {
	if g.Service == nil {
		return fmt.Errorf("vfs: drive service required")
	}
	return nil
}

// GetMeta implements DriveAPI.
func (g GoogleDrive) GetMeta(ctx context.Context, fileID string) (DriveMeta, error) {
	if err := g.require(); err != nil {
		return DriveMeta{}, err
	}
	f, err := g.Service.Files.Get(fileID).
		Fields(driveMetaFields).
		SupportsAllDrives(true).
		Context(ctx).
		Do()
	if err != nil {
		return DriveMeta{}, mapDriveError(err)
	}
	return fileToMeta(f), nil
}

// GetMedia implements DriveAPI.
func (g GoogleDrive) GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error) {
	if err := g.require(); err != nil {
		return nil, 0, err
	}
	res, err := g.Service.Files.Get(fileID).
		SupportsAllDrives(true).
		Context(ctx).
		Download()
	if err != nil {
		return nil, 0, mapDriveError(err)
	}
	size := res.ContentLength
	return res.Body, size, nil
}

// List implements DriveAPI.
func (g GoogleDrive) List(ctx context.Context, folderID string) ([]DriveMeta, error) {
	if err := g.require(); err != nil {
		return nil, err
	}
	q := fmt.Sprintf("'%s' in parents and trashed = false", escapeDriveQ(folderID))
	var out []DriveMeta
	err := g.Service.Files.List().
		Q(q).
		Fields(driveListFields).
		SupportsAllDrives(true).
		IncludeItemsFromAllDrives(true).
		PageSize(1000).
		Context(ctx).
		Pages(ctx, func(page *drive.FileList) error {
			for _, f := range page.Files {
				out = append(out, fileToMeta(f))
			}
			return nil
		})
	if err != nil {
		return nil, mapDriveError(err)
	}
	return out, nil
}

func fileToMeta(f *drive.File) DriveMeta {
	if f == nil {
		return DriveMeta{}
	}
	m := DriveMeta{
		ID:       f.Id,
		Name:     f.Name,
		MimeType: f.MimeType,
		Size:     f.Size,
		IsDir:    f.MimeType == mimeGoogleFolder,
	}
	if f.ModifiedTime != "" {
		if t, err := time.Parse(time.RFC3339, f.ModifiedTime); err == nil {
			m.ModTime = t
		}
	}
	if f.ShortcutDetails != nil {
		m.TargetID = f.ShortcutDetails.TargetId
		m.TargetMime = f.ShortcutDetails.TargetMimeType
	}
	return m
}

func escapeDriveQ(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	return strings.ReplaceAll(id, `'`, `\'`)
}

func mapDriveError(err error) error {
	if err == nil {
		return nil
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case 404:
			return ErrNotExist
		case 401:
			return ErrAuthExpired
		case 403:
			return ErrPermission
		}
	}
	return err
}

// DriveFactory opens read-only Drive folder providers. Auth is session-scoped.
type DriveFactory struct {
	ID   string
	Auth *SessionAuth
	API  DriveAPI // optional; nil → GoogleDrive from the session token
}

// Profile implements ProviderFactory.
func (f DriveFactory) Profile() string { return f.ID }

// Open implements ProviderFactory.
func (f DriveFactory) Open(ctx context.Context, sessionID string, spec MountSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.ID == "" {
		return nil, fmt.Errorf("vfs: gdrive factory needs id")
	}
	folderID := strings.TrimSpace(spec.Params[ParamFolderID])
	if folderID == "" {
		return nil, fmt.Errorf("vfs: gdrive folderId required")
	}
	var holder *TokenHolder
	if f.Auth != nil {
		holder = f.Auth.Holder(sessionID, f.ID)
	}
	api := f.API
	if api == nil {
		if holder == nil || holder.Current().Token == "" {
			return nil, fmt.Errorf("vfs: gdrive access token required")
		}
		gd, err := NewGoogleDrive(ctx, holder)
		if err != nil {
			return nil, err
		}
		api = gd
	}
	return &driveProvider{api: api, rootID: folderID, holder: holder}, nil
}

type driveProvider struct {
	api    DriveAPI
	rootID string
	holder *TokenHolder
}

var _ documentBackend = (*driveProvider)(nil)

func (p *driveProvider) call(ctx context.Context, fn func() error) error {
	if err := fn(); err == nil || !errors.Is(err, ErrAuthExpired) {
		return err
	}
	if p.holder == nil {
		return ErrAuthExpired
	}
	if err := p.holder.RefreshOnce(ctx); err != nil {
		return err
	}
	return fn()
}

func (p *driveProvider) getMeta(ctx context.Context, id string) (DriveMeta, error) {
	var meta DriveMeta
	err := p.call(ctx, func() error {
		var err error
		meta, err = p.api.GetMeta(ctx, id)
		return err
	})
	return meta, err
}

func (p *driveProvider) getMedia(ctx context.Context, id string) (io.ReadCloser, int64, error) {
	var body io.ReadCloser
	var size int64
	err := p.call(ctx, func() error {
		var err error
		body, size, err = p.api.GetMedia(ctx, id)
		return err
	})
	return body, size, err
}

func (p *driveProvider) list(ctx context.Context, folderID string) ([]DriveMeta, error) {
	var kids []DriveMeta
	err := p.call(ctx, func() error {
		var err error
		kids, err = p.api.List(ctx, folderID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return kids, nil
}

func (m DriveMeta) dir() bool {
	return m.IsDir || m.MimeType == mimeGoogleFolder
}

func (p *driveProvider) child(ctx context.Context, parentID, name string) (DriveMeta, error) {
	kids, err := p.list(ctx, parentID)
	if err != nil {
		return DriveMeta{}, err
	}
	var found DriveMeta
	n := 0
	for _, k := range kids {
		if k.Name != name {
			continue
		}
		found = k
		n++
	}
	switch n {
	case 0:
		return DriveMeta{}, ErrNotExist
	case 1:
		return p.follow(ctx, found, 4)
	default:
		return DriveMeta{}, fmt.Errorf("%w: %q", ErrAmbiguous, name)
	}
}

func isGoogleNativeFile(mime string) bool {
	return strings.HasPrefix(mime, "application/vnd.google-apps.") &&
		mime != mimeGoogleFolder && mime != mimeGoogleShortcut
}

func (p *driveProvider) follow(ctx context.Context, meta DriveMeta, depth int) (DriveMeta, error) {
	name := meta.Name
	for meta.MimeType == mimeGoogleShortcut {
		if depth <= 0 || meta.TargetID == "" {
			return DriveMeta{}, ErrNotExist
		}
		depth--
		next, err := p.getMeta(ctx, meta.TargetID)
		if err != nil {
			return DriveMeta{}, err
		}
		next.Name = name
		meta = next
	}
	return meta, nil
}

func (p *driveProvider) resolve(ctx context.Context, name string) (DriveMeta, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return DriveMeta{}, err
	}
	root, err := p.getMeta(ctx, p.rootID)
	if err != nil {
		return DriveMeta{}, err
	}
	root, err = p.follow(ctx, root, 4)
	if err != nil {
		return DriveMeta{}, err
	}
	if rel == "" {
		root.Name = "."
		root.IsDir = true
		return root, nil
	}
	parentID := root.ID
	parts := strings.Split(rel, "/")
	var cur DriveMeta
	for i, part := range parts {
		child, err := p.child(ctx, parentID, part)
		if err != nil {
			return DriveMeta{}, err
		}
		if i < len(parts)-1 && !child.dir() {
			return DriveMeta{}, ErrNotExist
		}
		cur = child
		parentID = child.ID
	}
	return cur, nil
}

func driveFileInfo(meta DriveMeta) FileInfo {
	mt := ""
	if !meta.dir() {
		if isGoogleNativeFile(meta.MimeType) {
			mt = "application/octet-stream"
		} else if t := s3KnownType(meta.MimeType); t != "" {
			mt = t
		} else {
			mt = DetectMediaType(meta.Name, nil)
		}
	}
	mode := fs.FileMode(0o644)
	if meta.dir() {
		mode = fs.ModeDir | 0o755
	}
	return FileInfo{
		Name:      meta.Name,
		Size:      meta.Size,
		Mode:      mode,
		ModTime:   meta.ModTime,
		IsDir:     meta.dir(),
		MediaType: mt,
	}
}

// Validate implements Provider.
func (p *driveProvider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := p.getMeta(ctx, p.rootID)
	if err != nil {
		return err
	}
	meta, err = p.follow(ctx, meta, 4)
	if err != nil {
		return err
	}
	if !meta.dir() {
		return fmt.Errorf("vfs: gdrive folderId is not a folder")
	}
	return nil
}

// Stat implements Provider.
func (p *driveProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	meta, err := p.resolve(ctx, name)
	if err != nil {
		return FileInfo{}, err
	}
	return driveFileInfo(meta), nil
}

// OpenFile implements Provider (read-only).
func (p *driveProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = perm
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND) != 0 || flag&os.O_CREATE != 0 || flag&os.O_TRUNC != 0
	if write {
		return nil, ErrReadOnly
	}
	meta, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if meta.dir() {
		return nil, fmt.Errorf("vfs: %s is a directory", name)
	}
	if isGoogleNativeFile(meta.MimeType) {
		return nil, ErrNotSupported
	}
	body, _, err := p.getMedia(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(MaxReadFileBytes)+1))
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	fi := driveFileInfo(meta)
	fi.Size = int64(len(data))
	return &s3ReadFile{Reader: bytes.NewReader(data), info: fi}, nil
}

// ReadDir implements Provider.
func (p *driveProvider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if !meta.dir() {
		return nil, fmt.Errorf("vfs: not a directory")
	}
	kids, err := p.list(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	out := make([]DirEntry, 0, len(kids))
	for _, k := range kids {
		dir := k.dir() || k.TargetMime == mimeGoogleFolder
		typ := fs.FileMode(0)
		if dir {
			typ = fs.ModeDir
		}
		out = append(out, DirEntry{Name: k.Name, IsDir: dir, Type: typ})
	}
	return out, nil
}

// Remove implements Provider.
func (p *driveProvider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := p.resolve(ctx, name); err != nil {
		return err
	}
	return ErrReadOnly
}

// MkdirAll implements Provider.
func (p *driveProvider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
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
	return ErrReadOnly
}

// OpenDocument implements documentBackend.
func (p *driveProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if meta.dir() {
		return nil, fmt.Errorf("vfs: %s is a directory", name)
	}
	if isGoogleNativeFile(meta.MimeType) {
		return nil, ErrNoCodec
	}
	if meta.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	body, _, err := p.getMedia(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(MaxReadFileBytes)+1))
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadFileBytes {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	fi := driveFileInfo(meta)
	fi.Size = int64(len(data))
	return decodeProviderDocument(ctx, name, fi, data, reg)
}

// WriteDocument implements documentBackend.
func (p *driveProvider) WriteDocument(ctx context.Context, name string, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = name
	_ = doc
	return ErrReadOnly
}
