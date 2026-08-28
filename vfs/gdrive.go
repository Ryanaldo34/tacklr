package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	driveMetaFields  = "id,name,mimeType,modifiedTime,size,shortcutDetails,version"
	driveListFields  = "nextPageToken,files(" + driveMetaFields + ")"
	driveMyDriveRoot = "root" // Google Drive alias for the caller's My Drive.
)

// DriveMeta is one Drive file or folder (IDs stay inside the provider).
type DriveMeta struct {
	ID         string
	Name       string
	MimeType   string
	Size       int64
	ModTime    time.Time
	Version    string
	IsDir      bool
	TargetID   string
	TargetMime string
}

// DriveAPI is the Drive subset used by the provider. Tests inject a fake.
type DriveAPI interface {
	GetMeta(ctx context.Context, fileID string) (DriveMeta, error)
	GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error)
	List(ctx context.Context, folderID string) ([]DriveMeta, error)
	Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error)
	PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, size int64) (DriveMeta, error)
	Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (DriveMeta, error)
	Trash(ctx context.Context, fileID string) error
	Mkdir(ctx context.Context, parentID, name string) (DriveMeta, error)
}

// GoogleDrive implements DriveAPI with google.golang.org/api/drive/v3.
type googleDrive struct {
	service *drive.Service
}

// NewGoogleDrive builds a DriveAPI from a user token holder. Call from OpenVFS
// when a drive bind arrives, then pass the result to Drive.
func NewGoogleDrive(ctx context.Context, holder *TokenHolder) (DriveAPI, error) {
	return newGoogleDrive(ctx, holder)
}

func newGoogleDrive(ctx context.Context, holder *TokenHolder) (*googleDrive, error) {
	if holder == nil {
		return nil, fmt.Errorf("vfs: drive token required")
	}
	svc, err := drive.NewService(ctx, option.WithTokenSource(holder))
	if err != nil {
		return nil, fmt.Errorf("vfs: drive service: %w", err)
	}
	return &googleDrive{service: svc}, nil
}

func (g googleDrive) require() error {
	if g.service == nil {
		return fmt.Errorf("vfs: drive service required")
	}
	return nil
}

// GetMeta implements DriveAPI.
func (g googleDrive) GetMeta(ctx context.Context, fileID string) (DriveMeta, error) {
	if err := g.require(); err != nil {
		return DriveMeta{}, err
	}
	f, err := g.service.Files.Get(fileID).
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
func (g googleDrive) GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error) {
	if err := g.require(); err != nil {
		return nil, 0, err
	}
	res, err := g.service.Files.Get(fileID).
		SupportsAllDrives(true).
		Context(ctx).
		Download()
	if err != nil {
		return nil, 0, mapDriveError(err)
	}
	size := res.ContentLength
	return res.Body, size, nil
}

// Export implements DriveAPI (official HTML export is application/zip).
func (g googleDrive) Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error) {
	if err := g.require(); err != nil {
		return nil, 0, err
	}
	res, err := g.service.Files.Export(fileID, mimeType).Context(ctx).Download()
	if err != nil {
		return nil, 0, mapDriveError(err)
	}
	return res.Body, res.ContentLength, nil
}

// PutMedia implements DriveAPI.
func (g googleDrive) PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, size int64) (DriveMeta, error) {
	if err := g.require(); err != nil {
		return DriveMeta{}, err
	}
	_ = size
	call := g.service.Files.Update(fileID, nil).
		SupportsAllDrives(true).
		Fields(driveMetaFields).
		Context(ctx)
	if r != nil && mediaMIME != "" {
		call = call.Media(r, googleapi.ContentType(mediaMIME))
	}
	f, err := call.Do()
	if err != nil {
		return DriveMeta{}, mapDriveError(err)
	}
	return fileToMeta(f), nil
}

// Create implements DriveAPI. r == nil or mediaMIME == "" is metadata-only.
func (g googleDrive) Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (DriveMeta, error) {
	if err := g.require(); err != nil {
		return DriveMeta{}, err
	}
	_ = size
	f := &drive.File{Name: name, MimeType: metadataMIME, Parents: []string{parentID}}
	call := g.service.Files.Create(f).
		SupportsAllDrives(true).
		Fields(driveMetaFields).
		Context(ctx)
	if r != nil && mediaMIME != "" {
		call = call.Media(r, googleapi.ContentType(mediaMIME))
	}
	created, err := call.Do()
	if err != nil {
		return DriveMeta{}, mapDriveError(err)
	}
	return fileToMeta(created), nil
}

// Trash implements DriveAPI (files.update trashed:true, not files.delete).
func (g googleDrive) Trash(ctx context.Context, fileID string) error {
	if err := g.require(); err != nil {
		return err
	}
	_, err := g.service.Files.Update(fileID, &drive.File{Trashed: true}).
		SupportsAllDrives(true).
		Context(ctx).
		Do()
	return mapDriveError(err)
}

// Mkdir implements DriveAPI.
func (g googleDrive) Mkdir(ctx context.Context, parentID, name string) (DriveMeta, error) {
	return g.Create(ctx, parentID, name, mimeGoogleFolder, "", nil, 0)
}

// List implements DriveAPI.
func (g googleDrive) List(ctx context.Context, folderID string) ([]DriveMeta, error) {
	q := fmt.Sprintf("'%s' in parents and trashed = false", escapeDriveQ(folderID))
	return g.listQuery(ctx, q, 1000)
}

// Find lists children of folderID whose Drive name matches. Path resolve uses
// this instead of List so a large folder does not transfer every sibling.
func (g googleDrive) Find(ctx context.Context, folderID, name string) ([]DriveMeta, error) {
	q := fmt.Sprintf("'%s' in parents and name = '%s' and trashed = false",
		escapeDriveQ(folderID), escapeDriveQ(name))
	return g.listQuery(ctx, q, 100)
}

func (g googleDrive) listQuery(ctx context.Context, q string, pageSize int64) ([]DriveMeta, error) {
	if err := g.require(); err != nil {
		return nil, err
	}
	var out []DriveMeta
	err := g.service.Files.List().
		Q(q).
		Fields(driveListFields).
		SupportsAllDrives(true).
		IncludeItemsFromAllDrives(true).
		PageSize(pageSize).
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
	if f.Version != 0 {
		m.Version = strconv.FormatInt(f.Version, 10)
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

func googleHTTPErr(svc string, code int, msg string) error {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return fmt.Errorf("vfs: %s HTTP %d", svc, code)
	}
	return fmt.Errorf("vfs: %s HTTP %d: %s", svc, code, msg)
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
			msg := strings.ToLower(gerr.Message)
			if strings.Contains(msg, "exportsizelimitexceeded") || strings.Contains(msg, "export size") {
				return ErrTooLarge
			}
			if gerr.Message == "" {
				return ErrPermission
			}
			return fmt.Errorf("%w: %s", ErrPermission, gerr.Message)
		case 400:
			msg := strings.ToLower(gerr.Message)
			if strings.Contains(msg, "exportsizelimitexceeded") || strings.Contains(msg, "export size") {
				return ErrTooLarge
			}
			return googleHTTPErr("drive", gerr.Code, gerr.Message)
		default:
			return googleHTTPErr("drive", gerr.Code, gerr.Message)
		}
	}
	return err
}

// Drive opens a Google Drive folder. api is required (host-built SDK or a test
// fake). folderId and writable come from this turn's Binding.
func Drive(api DriveAPI) Open {
	return DriveWith(api, nil, nil)
}

// DriveWith is Drive plus Docs/Sheets clients for native Google files.
func DriveWith(api DriveAPI, docs DocsAPI, sheets SheetsAPI) Open {
	if api == nil {
		panic("vfs: Drive requires a DriveAPI client")
	}
	return func(ctx context.Context, _ string, b Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		holder := b.Live
		if holder == nil && strings.TrimSpace(b.Auth.Token) != "" {
			holder = NewTokenHolder(b.Auth)
		}
		folderID := strings.TrimSpace(b.Params[ParamFolderID])
		if folderID == "" {
			folderID = driveMyDriveRoot
		}
		return &driveProvider{
			api: api, docs: docs, sheets: sheets, rootID: folderID, holder: holder, writable: b.Writable,
			zipCache: newFIFO[[]byte](32), getCache: newFIFO[DocsSnapshot](32),
			sheetCache: newFIFO[SheetsSnapshot](32),
		}, nil
	}
}

type driveProvider struct {
	api        DriveAPI
	docs       DocsAPI
	sheets     SheetsAPI
	rootID     string
	holder     *TokenHolder
	writable   bool
	zipCache   *fifo[[]byte]
	getCache   *fifo[DocsSnapshot]
	sheetCache *fifo[SheetsSnapshot]
}

var _ documentBackend = (*driveProvider)(nil)

func (p *driveProvider) call(ctx context.Context, fn func() error) error {
	staleToken := ""
	if p.holder != nil {
		if err := p.holder.EnsureValid(ctx); err != nil {
			return err
		}
		staleToken = p.holder.Current().Token
	}
	if err := fn(); err == nil || !errors.Is(err, ErrAuthExpired) {
		return err
	}
	if p.holder == nil {
		return ErrAuthExpired
	}
	if err := p.holder.RefreshIfCurrent(ctx, staleToken); err != nil {
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

func (p *driveProvider) childrenNamed(ctx context.Context, parentID, name string) ([]DriveMeta, error) {
	var find func(context.Context, string, string) ([]DriveMeta, error)
	switch a := p.api.(type) {
	case googleDrive:
		find = a.Find
	case *googleDrive:
		find = a.Find
	default:
		return p.list(ctx, parentID)
	}
	var kids []DriveMeta
	err := p.call(ctx, func() error {
		var e error
		kids, e = find(ctx, parentID, name)
		return e
	})
	return kids, err
}

func (m DriveMeta) dir() bool {
	return m.IsDir || m.MimeType == mimeGoogleFolder
}

func (p *driveProvider) child(ctx context.Context, parentID, name string) (DriveMeta, error) {
	return p.lookupChild(ctx, parentID, name, true)
}

func (p *driveProvider) childRaw(ctx context.Context, parentID, name string) (DriveMeta, error) {
	return p.lookupChild(ctx, parentID, name, false)
}

func (p *driveProvider) lookupChild(ctx context.Context, parentID, name string, follow bool) (DriveMeta, error) {
	kids, err := p.childrenNamed(ctx, parentID, name)
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
		if follow {
			return p.follow(ctx, found, 4)
		}
		return found, nil
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
			mt = meta.MimeType
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
		if !p.writable {
			return nil, ErrReadOnly
		}
		return nil, ErrNotSupported
	}
	meta, err := p.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if meta.dir() {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	if isGoogleNativeFile(meta.MimeType) {
		return nil, ErrNotSupported
	}
	body, _, err := p.getMedia(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	data, err := readCapped(body, MaxReadFileBytes, meta.Size)
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
		return nil, ErrNotDir
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

// Remove implements Provider. Drive trash; does not follow shortcuts.
func (p *driveProvider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.writable {
		if _, err := p.resolve(ctx, name); err != nil {
			return err
		}
		return ErrReadOnly
	}
	meta, err := p.resolveLeaf(ctx, name)
	if err != nil {
		return err
	}
	err = p.call(ctx, func() error { return p.api.Trash(ctx, meta.ID) })
	if err != nil {
		return err
	}
	p.invalidate(meta.ID)
	return nil
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
	if !p.writable {
		return ErrReadOnly
	}
	_, err = p.ensureDir(ctx, rel)
	return err
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
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	if isGoogleNativeFile(meta.MimeType) {
		if meta.MimeType == mimeGoogleDocument {
			return p.openGoogleDoc(ctx, name, meta)
		}
		if meta.MimeType == mimeGoogleSpreadsheet {
			return p.openGoogleSheet(ctx, name, meta)
		}
		return nil, ErrNoCodec
	}
	if meta.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	body, _, err := p.getMedia(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	data, err := readCapped(body, MaxReadFileBytes, meta.Size)
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
