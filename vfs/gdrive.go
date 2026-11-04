package vfs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/vfs/internal/google"
	"github.com/ryanaldo34/tacklr/vfs/internal/google/drive"
	"github.com/ryanaldo34/tacklr/vfs/internal/httprange"
	"github.com/ryanaldo34/tacklr/vfs/internal/rest"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// driveMyDriveRoot is the Google Drive alias for the caller's My Drive.
const driveMyDriveRoot = "root"

// DriveMeta is one Drive file or folder (IDs stay inside the provider).
type DriveMeta struct {
	ID         string
	Name       string
	MimeType   string
	Size       int64
	ModTime    time.Time
	Version    string
	MD5        string // content checksum; empty for folders and Google-native files
	IsDir      bool
	TargetID   string
	TargetMime string
}

// DriveFinder is a DriveAPI that can list only the children of folderID with
// one name (a Drive query), so a path resolves without listing whole
// folders. The provider uses it when the DriveAPI has it.
type DriveFinder interface {
	Find(ctx context.Context, folderID, name string) ([]DriveMeta, error)
}

// DriveAPI is the Drive subset used by the provider. Hosts pass
// NewGoogleDrive.
type DriveAPI interface {
	GetMeta(ctx context.Context, fileID string) (DriveMeta, error)
	// GetMedia opens n bytes of the file's content from off. n < 0 reads to
	// the end; n is never 0.
	GetMedia(ctx context.Context, fileID string, off, n int64) (io.ReadCloser, error)
	List(ctx context.Context, folderID string) ([]DriveMeta, error)
	Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error)
	PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, size int64) (DriveMeta, error)
	Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (DriveMeta, error)
	Trash(ctx context.Context, fileID string) error
	Mkdir(ctx context.Context, parentID, name string) (DriveMeta, error)
}

// Drive opens a Google Drive folder. api is required (NewGoogleDrive).
// holder is the token holder api authenticates with, so a near-expiry or
// rejected token refreshes there; nil uses the binding's token without
// refresh. folderId and writable come from this turn's Binding.
func Drive(api DriveAPI, holder *TokenHolder) Open {
	return DriveWith(api, nil, nil, holder)
}

// DriveWith is Drive plus Docs/Sheets clients for native Google files.
func DriveWith(api DriveAPI, docs DocsAPI, sheets SheetsAPI, holder *TokenHolder) Open {
	if api == nil {
		panic("vfs: Drive requires a DriveAPI client")
	}
	return func(ctx context.Context, _ string, b mount.Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h := holder
		if h == nil && strings.TrimSpace(b.Auth.Token) != "" {
			h = NewTokenHolder(b.Auth)
		}
		folderID := strings.TrimSpace(b.Params[mount.ParamFolderID])
		if folderID == "" {
			folderID = driveMyDriveRoot
		}
		return &driveProvider{
			api: api, docs: docs, sheets: sheets, rootID: folderID, holder: h, writable: b.Writable,
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

// codecs are the codecs of Drive's native Google Docs and Sheets.
func (p *driveProvider) codecs() []Codec {
	return []Codec{DocsCodec{}, SheetsCodec{}}
}

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

func (p *driveProvider) getMedia(ctx context.Context, id string, off, n int64) (io.ReadCloser, error) {
	var body io.ReadCloser
	err := p.call(ctx, func() error {
		var err error
		body, err = p.api.GetMedia(ctx, id, off, n)
		return err
	})
	return body, err
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
	finder, ok := p.api.(DriveFinder)
	if !ok {
		return p.list(ctx, parentID)
	}
	var kids []DriveMeta
	err := p.call(ctx, func() error {
		var e error
		kids, e = finder.Find(ctx, parentID, name)
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
		Version:   driveVersion(meta),
	}
}

// driveVersion is the content checksum, or the revision for Google-native
// files, which have no checksum. Folders have no version.
func driveVersion(m DriveMeta) string {
	if m.dir() {
		return ""
	}
	return cmp.Or(m.MD5, driveRevision(m))
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
	return newRemoteFile(ctx, driveFileInfo(meta), func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		return p.getMedia(ctx, meta.ID, off, n)
	}), nil
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
	body, err := p.getMedia(ctx, meta.ID, 0, -1)
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

const (
	driveMetaFields = "id,name,mimeType,modifiedTime,size,shortcutDetails,version,md5Checksum"
	driveListFields = "nextPageToken,files(" + driveMetaFields + ")"
)

// googleDrive implements DriveAPI with the Drive v3 REST API.
type googleDrive struct {
	c rest.Client
}

// NewGoogleDrive returns a DriveAPI that authenticates with holder
// (required). base is the API root ("" for Google's); httpClient nil is
// http.DefaultClient. Call it from OpenVFS when a drive bind arrives, then
// pass the result to Drive.
func NewGoogleDrive(holder *TokenHolder, base string, httpClient *http.Client) DriveAPI {
	return googleDrive{c: apiClient(holder, base, "https://www.googleapis.com/", httpClient)}
}

// driveQuery is q plus supportsAllDrives, which every Drive call sends.
func driveQuery(q url.Values) url.Values {
	q.Set("supportsAllDrives", "true")
	return q
}

// GetMeta implements DriveAPI.
func (g googleDrive) GetMeta(ctx context.Context, fileID string) (DriveMeta, error) {
	var f drive.File
	err := g.c.Call(ctx, http.MethodGet, "drive/v3/files/"+url.PathEscape(fileID), driveQuery(url.Values{"fields": {driveMetaFields}}), nil, &f)
	return fileToMeta(&f), mapDriveError(err)
}

// GetMedia implements DriveAPI.
func (g googleDrive) GetMedia(ctx context.Context, fileID string, off, n int64) (io.ReadCloser, error) {
	h := http.Header{}
	if r := httprange.Header(off, n); r != "" {
		h.Set("Range", r)
	}
	res, err := g.c.Media(ctx, "drive/v3/files/"+url.PathEscape(fileID), driveQuery(url.Values{"alt": {"media"}}), h) //nolint:bodyclose // httprange.Body returns the body or closes it
	if err != nil {
		return nil, mapDriveError(err)
	}
	return httprange.Body(res, off, n)
}

// Export implements DriveAPI (official HTML export is application/zip).
func (g googleDrive) Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error) {
	res, err := g.c.Media(ctx, "drive/v3/files/"+url.PathEscape(fileID)+"/export", url.Values{"mimeType": {mimeType}}, nil) //nolint:bodyclose // the caller closes the body
	if err != nil {
		return nil, 0, mapDriveError(err)
	}
	return res.Body, res.ContentLength, nil
}

// PutMedia implements DriveAPI.
func (g googleDrive) PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, _ int64) (DriveMeta, error) {
	var f drive.File
	err := google.Upload(ctx, g.c, http.MethodPatch, "upload/drive/v3/files/"+url.PathEscape(fileID), driveQuery(url.Values{"fields": {driveMetaFields}}), drive.File{}, mediaMIME, r, &f)
	return fileToMeta(&f), mapDriveError(err)
}

// Create implements DriveAPI. r == nil or mediaMIME == "" is metadata-only.
func (g googleDrive) Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, _ int64) (DriveMeta, error) {
	meta := drive.File{Name: name, MimeType: metadataMIME, Parents: []string{parentID}}
	q := driveQuery(url.Values{"fields": {driveMetaFields}})
	var f drive.File
	var err error
	if r != nil && mediaMIME != "" {
		err = google.Upload(ctx, g.c, http.MethodPost, "upload/drive/v3/files", q, meta, mediaMIME, r, &f)
	} else {
		err = g.c.Call(ctx, http.MethodPost, "drive/v3/files", q, meta, &f)
	}
	return fileToMeta(&f), mapDriveError(err)
}

// Trash implements DriveAPI (files.update trashed:true, not files.delete).
func (g googleDrive) Trash(ctx context.Context, fileID string) error {
	err := g.c.Call(ctx, http.MethodPatch, "drive/v3/files/"+url.PathEscape(fileID), driveQuery(url.Values{}), drive.File{Trashed: true}, nil)
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

func (g googleDrive) listQuery(ctx context.Context, q string, pageSize int) ([]DriveMeta, error) {
	query := driveQuery(url.Values{
		"q":                         {q},
		"fields":                    {driveListFields},
		"includeItemsFromAllDrives": {"true"},
		"pageSize":                  {strconv.Itoa(pageSize)},
	})
	var out []DriveMeta
	for {
		var page drive.FileList
		if err := g.c.Call(ctx, http.MethodGet, "drive/v3/files", query, nil, &page); err != nil {
			return nil, mapDriveError(err)
		}
		for _, f := range page.Files {
			out = append(out, fileToMeta(f))
		}
		if page.NextPageToken == "" {
			return out, nil
		}
		query.Set("pageToken", page.NextPageToken)
	}
}

func fileToMeta(f *drive.File) DriveMeta {
	m := DriveMeta{
		ID:       f.ID,
		Name:     f.Name,
		MimeType: f.MimeType,
		Size:     f.Size,
		ModTime:  f.ModifiedTime,
		MD5:      f.MD5Checksum,
		IsDir:    f.MimeType == mimeGoogleFolder,
	}
	if f.Version != 0 {
		m.Version = strconv.FormatInt(f.Version, 10)
	}
	if f.ShortcutDetails != nil {
		m.TargetID = f.ShortcutDetails.TargetID
		m.TargetMime = f.ShortcutDetails.TargetMimeType
	}
	return m
}

func escapeDriveQ(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	return strings.ReplaceAll(id, `'`, `\'`)
}

// mapDriveError maps a Drive reply error to the vfs errors.
func mapDriveError(err error) error {
	var gerr *rest.Error
	if !errors.As(err, &gerr) {
		return err
	}
	msg := strings.ToLower(gerr.Message)
	exportTooLarge := strings.Contains(msg, "exportsizelimitexceeded") || strings.Contains(msg, "export size")
	switch {
	case gerr.Code == http.StatusNotFound:
		return ErrNotExist
	case gerr.Code == http.StatusUnauthorized:
		return ErrAuthExpired
	case (gerr.Code == http.StatusBadRequest || gerr.Code == http.StatusForbidden) && exportTooLarge:
		return ErrTooLarge
	case gerr.Code == http.StatusForbidden && gerr.Message == "":
		return ErrPermission
	case gerr.Code == http.StatusForbidden:
		return fmt.Errorf("%w: %s", ErrPermission, gerr.Message)
	default:
		return fmt.Errorf("vfs: drive %w", gerr)
	}
}
