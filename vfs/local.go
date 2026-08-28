package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// localProvider is a host directory as a mount source (unexported host root).
type localProvider struct {
	root string
}

// NewLocalProvider returns a Provider rooted at a canonical absolute directory.
func NewLocalProvider(dir string) (Provider, error) {
	root, err := canonicalizeDir(dir)
	if err != nil {
		return nil, err
	}
	return localProvider{root: root}, nil
}

// Validate implements Provider.
func (p localProvider) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.root == "" || !filepath.IsAbs(p.root) {
		return fmt.Errorf("vfs: local root must be absolute")
	}
	info, err := os.Stat(p.root)
	if err != nil {
		return fmt.Errorf("vfs: local root %q: %w", p.root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("vfs: local root is not a directory")
	}
	return nil
}

// Stat implements Provider.
func (p localProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return FileInfo{}, err
	}
	info, err := os.Lstat(host)
	if err != nil {
		return FileInfo{}, mapOSError(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		if _, err := p.follow(host); err != nil {
			return FileInfo{}, err
		}
	}
	return localFileInfo(host, info), nil
}

// OpenFile implements Provider.
func (p localProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return nil, err
	}
	if flag&os.O_CREATE == 0 {
		if resolved, err := p.follow(host); err == nil {
			host = resolved
		} else if !errors.Is(err, ErrNotExist) {
			return nil, err
		}
	}
	f, err := os.OpenFile(host, flag, perm)
	if err != nil {
		return nil, mapOSError(err)
	}
	return &localFile{File: f}, nil
}

// PutFile writes a full file in one shot (used by MountSession.WriteFile).
func (p localProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return err
	}
	// 0o644 matches prior WriteFile/OpenFile create mode for workspace files.
	f, err := os.OpenFile(host, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644) //nolint:gosec // G302: agent workspace files are group/world-readable by design
	if err != nil {
		return mapOSError(err)
	}
	defer f.Close()
	if size == 0 {
		return nil
	}
	if _, err := io.Copy(f, io.LimitReader(r, size)); err != nil {
		return mapOSError(err)
	}
	return nil
}

// OpenDocument translates local file bytes into Document IR.
func (p localProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fi, err := p.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	if fi.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	host, err := p.hostPath(name)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(host) //nolint:gosec // G304: host is jailed by hostPath
	if err != nil {
		return nil, mapOSError(err)
	}
	return decodeProviderDocument(ctx, name, fi, data, reg)
}

// WriteDocument encodes IR and writes the file immediately.
func (p localProvider) WriteDocument(ctx context.Context, name string, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := EncodeDocument(ctx, doc)
	if err != nil {
		return err
	}
	if dir := path.Dir(name); dir != "" && dir != "." {
		if err := p.MkdirAll(ctx, dir, 0o755); err != nil {
			return err
		}
	}
	return p.PutFile(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// ReadDir implements Provider.
func (p localProvider) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return nil, err
	}
	host, err = p.follow(host)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(host)
	if err != nil {
		return nil, mapOSError(err)
	}
	out := make([]DirEntry, len(entries))
	for i, e := range entries {
		out[i] = DirEntry{Name: e.Name(), IsDir: e.IsDir(), Type: e.Type()}
	}
	return out, nil
}

// Remove implements Provider.
func (p localProvider) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return err
	}
	return mapOSError(os.Remove(host))
}

// MkdirAll implements Provider.
func (p localProvider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	host, err := p.hostPath(name)
	if err != nil {
		return err
	}
	return mapOSError(os.MkdirAll(host, perm))
}

func (p localProvider) hostPath(name string) (string, error) {
	rel, err := cleanRel(name)
	if err != nil {
		return "", err
	}
	if rel == "" {
		return p.root, nil
	}
	full := filepath.Join(p.root, filepath.FromSlash(rel))
	if !within(p.root, full) {
		return "", ErrInvalidPath
	}
	return full, nil
}

func (p localProvider) follow(host string) (string, error) {
	resolved, err := filepath.EvalSymlinks(host)
	if err != nil {
		return "", mapOSError(err)
	}
	if !within(p.root, resolved) {
		return "", ErrInvalidPath
	}
	return resolved, nil
}

func cleanRel(name string) (string, error) {
	name = strings.Trim(name, "/")
	if name == "" || name == "." {
		return "", nil
	}
	cleaned := path.Clean(name)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || path.IsAbs(cleaned) {
		return "", ErrInvalidPath
	}
	for _, seg := range strings.Split(cleaned, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", ErrInvalidPath
		}
	}
	return cleaned, nil
}

func within(root, full string) bool {
	rel, err := filepath.Rel(root, filepath.Clean(full))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func canonicalizeDir(dir string) (string, error) {
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("vfs: local root must be absolute")
	}
	dir = filepath.Clean(dir)
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("vfs: local root %q: %w", dir, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("vfs: local root is not a directory")
	}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	return dir, nil
}

func fileInfoFromOS(info os.FileInfo) FileInfo {
	return FileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}
}

func localFileInfo(host string, info os.FileInfo) FileInfo {
	fi := fileInfoFromOS(info)
	if fi.IsDir {
		return fi
	}
	fi.MediaType = DetectMediaType(info.Name(), nil)
	if fi.MediaType != "application/octet-stream" {
		return fi
	}
	// No known extension: peek so README (no suffix) can still be text.
	f, err := os.Open(host)
	if err != nil {
		return fi
	}
	defer f.Close()
	var buf [512]byte
	n, _ := f.Read(buf[:])
	if n > 0 {
		fi.MediaType = DetectMediaType(info.Name(), buf[:n])
	}
	return fi
}

func mapOSError(err error) error {
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		return ErrNotExist
	}
	if os.IsExist(err) {
		return ErrExist
	}
	return err
}

type localFile struct{ *os.File }

func (f *localFile) Stat() (FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		return FileInfo{}, mapOSError(err)
	}
	return localFileInfo(f.Name(), info), nil
}

var _ documentBackend = localProvider{}

// Local opens providers under a fixed jail. Params: "subpath" (optional),
// "session_scoped=true" (optional). The jail stays in the closure; providers
// never expose host roots.
func Local(base string) Open {
	return func(ctx context.Context, sessionID string, b Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		root, err := canonicalizeDir(base)
		if err != nil {
			return nil, err
		}
		if sub := b.Params["subpath"]; sub != "" {
			if filepath.IsAbs(sub) {
				return nil, fmt.Errorf("vfs: subpath must be relative")
			}
			cleaned := filepath.Clean(sub)
			if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
				return nil, fmt.Errorf("vfs: subpath escapes base")
			}
			joined := filepath.Join(root, cleaned)
			if !within(root, joined) {
				return nil, fmt.Errorf("vfs: subpath escapes base")
			}
			root = joined
		}
		if b.Params["session_scoped"] == "true" && sessionID != "" {
			if strings.ContainsAny(sessionID, `/\`) || sessionID == ".." || sessionID == "." {
				return nil, fmt.Errorf("vfs: unsafe session id for path")
			}
			root = filepath.Join(root, sessionID)
		}
		if err := os.MkdirAll(root, 0o750); err != nil {
			return nil, fmt.Errorf("vfs: mkdir %q: %w", root, err)
		}
		return NewLocalProvider(root)
	}
}
