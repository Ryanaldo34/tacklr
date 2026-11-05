package vfs

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// localProvider is a host directory as a mount source (unexported host root).
//
// Every operation goes through an os.Root on the directory, so no path and no
// symlink, at any segment, reaches outside it: the agent can make symlinks
// with its shell, and their targets are the host's.
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
	info, err := os.Stat(p.root)
	if err != nil {
		return fmt.Errorf("vfs: local root %q: %w", p.root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("vfs: local root is not a directory")
	}
	return nil
}

// at opens the provider's root for one operation on name, and gives name
// relative to it.
func (p localProvider) at(ctx context.Context, name string) (*os.Root, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	rel, err := cleanRel(name)
	if err != nil {
		return nil, "", err
	}
	r, err := os.OpenRoot(p.root)
	if err != nil {
		return nil, "", mapOSError(err)
	}
	return r, cmp.Or(rel, "."), nil
}

// Stat implements Provider. A symlink is its target's info under its own
// name.
func (p localProvider) Stat(ctx context.Context, name string) (FileInfo, error) {
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return FileInfo{}, err
	}
	defer r.Close()
	info, err := r.Stat(rel)
	if err != nil {
		return FileInfo{}, mapOSError(err)
	}
	return localFileInfo(info, func() (io.ReaderAt, func() error, error) {
		f, err := r.Open(rel)
		if err != nil {
			return nil, nil, err
		}
		return f, f.Close, nil
	}), nil
}

// OpenFile implements Provider.
func (p localProvider) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (File, error) {
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	f, err := r.OpenFile(rel, flag, perm)
	if err != nil {
		return nil, mapOSError(err)
	}
	// A directory is no file to read, as on every other backend.
	if fi, err := f.Stat(); err == nil && fi.IsDir() {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrIsDir, name)
	}
	return &localFile{File: f}, nil
}

// PutFile writes a full file in one shot (used by MountSession.WriteFile).
func (p localProvider) PutFile(ctx context.Context, name string, r io.Reader, size int64) error {
	root, rel, err := p.at(ctx, name)
	if err != nil {
		return err
	}
	defer root.Close()
	// 0o644 matches prior WriteFile/OpenFile create mode for workspace files.
	f, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644) //nolint:gosec // G302: agent workspace files are group/world-readable by design
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
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := r.ReadFile(rel)
	if err != nil {
		return nil, mapOSError(err)
	}
	return decodeProviderDocument(ctx, name, fi, data, reg)
}

// WriteDocument encodes IR and writes the file immediately.
func (p localProvider) WriteDocument(ctx context.Context, name string, doc Document, reg *ContentRegistry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := reg.Encode(ctx, doc)
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
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	dir, err := r.Open(rel)
	if err != nil {
		return nil, mapOSError(err)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
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
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return err
	}
	defer r.Close()
	return mapOSError(r.Remove(rel))
}

// MkdirAll implements Provider.
func (p localProvider) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	r, rel, err := p.at(ctx, name)
	if err != nil {
		return err
	}
	defer r.Close()
	return mapOSError(r.MkdirAll(rel, perm))
}

func cleanRel(name string) (string, error) {
	name = strings.Trim(name, "/")
	if name == "" || name == "." {
		return "", nil
	}
	// A cleaned relative path has a ".." only at the start, and is "." only
	// when its segments cancel ("a/..").
	cleaned := path.Clean(name)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", ErrInvalidPath
	}
	return cleaned, nil
}

// safeSegment rejects a session id or member name that would leave its
// parent directory when used as one host path segment.
func safeSegment(seg string) error {
	if seg == "" || strings.ContainsAny(seg, `/\`) || seg == ".." || seg == "." {
		return fmt.Errorf("vfs: unsafe path segment %q", seg)
	}
	return nil
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
	fi := FileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}
	if !fi.IsDir {
		fi.Version = localVersion(info)
	}
	return fi
}

// localVersion is size, mtime in nanoseconds and inode (where the OS has
// one), base 36. A write changes the size or mtime; a replace by rename
// changes the inode.
func localVersion(info os.FileInfo) string {
	v := strconv.FormatInt(info.Size(), 36) + "-" + strconv.FormatInt(info.ModTime().UnixNano(), 36)
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		v += "-" + strconv.FormatUint(st.Ino, 36)
	}
	return v
}

// localFileInfo is info as a FileInfo. A file with no known extension is
// peeked through open, so README (no suffix) can still be text.
func localFileInfo(info os.FileInfo, open func() (io.ReaderAt, func() error, error)) FileInfo {
	fi := fileInfoFromOS(info)
	if fi.IsDir {
		return fi
	}
	fi.MediaType = DetectMediaType(info.Name(), nil)
	if fi.MediaType != "application/octet-stream" {
		return fi
	}
	f, done, err := open()
	if err != nil {
		return fi
	}
	defer func() { _ = done() }()
	var buf [512]byte
	n, _ := f.ReadAt(buf[:], 0)
	if n > 0 {
		fi.MediaType = DetectMediaType(info.Name(), buf[:n])
	}
	return fi
}

// mapOSError is err as the vfs sentinel the agent can act on. Any other OS
// error keeps its operation and cause but loses its path: the path is the
// host's, which the agent must never see.
func mapOSError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return ErrNotExist
	case errors.Is(err, fs.ErrExist):
		return ErrExist
	case errors.Is(err, fs.ErrPermission):
		return ErrPermission
	case errors.Is(err, syscall.ENOTDIR):
		return ErrNotDir
	case errors.Is(err, syscall.EISDIR):
		return ErrIsDir
	case strings.Contains(err.Error(), "path escapes from parent"):
		// os.Root's refusal of a path or symlink that leaves the root; the
		// os package does not export its sentinel.
		return ErrInvalidPath
	}
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		return fmt.Errorf("vfs: %s: %w", pe.Op, pe.Err)
	}
	return err
}

type localFile struct{ *os.File }

func (f *localFile) Stat() (FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		return FileInfo{}, mapOSError(err)
	}
	return localFileInfo(info, func() (io.ReaderAt, func() error, error) {
		return f.File, func() error { return nil }, nil
	}), nil
}

var _ documentBackend = localProvider{}

// Local opens providers under a fixed jail. Params: "subpath" (optional),
// "session_scoped=true" (optional). The jail stays in the closure; providers
// never expose host roots.
func Local(base string) Open {
	return func(ctx context.Context, sessionID string, b mount.Binding) (Provider, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		root, err := canonicalizeDir(base)
		if err != nil {
			return nil, err
		}
		var rel string
		if sub := b.Params["subpath"]; sub != "" {
			if filepath.IsAbs(sub) {
				return nil, fmt.Errorf("vfs: subpath must be relative")
			}
			cleaned := filepath.Clean(sub)
			if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
				return nil, fmt.Errorf("vfs: subpath escapes base")
			}
			rel = cleaned
		}
		if b.Params["session_scoped"] == "true" && sessionID != "" {
			if err := safeSegment(sessionID); err != nil {
				return nil, err
			}
			rel = filepath.Join(rel, sessionID)
		}
		if rel != "" {
			// Made through the base's os.Root, so a symlink under the base
			// cannot move the mount's root outside it.
			br, err := os.OpenRoot(root)
			if err != nil {
				return nil, fmt.Errorf("vfs: open %q: %w", root, err)
			}
			err = br.MkdirAll(rel, 0o750)
			_ = br.Close()
			if err != nil {
				return nil, fmt.Errorf("vfs: mkdir under %q: %w", root, mapOSError(err))
			}
			root = filepath.Join(root, rel)
		}
		return NewLocalProvider(root)
	}
}
