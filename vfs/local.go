package vfs

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
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
func (p localProvider) Validate(context.Context) error {
	info, err := os.Stat(p.root)
	if err != nil {
		return fmt.Errorf("vfs: local root %q: %w", p.root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("vfs: local root is not a directory")
	}
	return nil
}

// in runs fn on an os.Root of the provider's directory with name relative
// to it, and returns fn's error as the vfs error the agent can act on.
func (p localProvider) in(name string, fn func(r *os.Root, rel string) error) error {
	r, err := os.OpenRoot(p.root)
	if err != nil {
		return mapOSError(err)
	}
	defer r.Close()
	return mapOSError(fn(r, cmp.Or(name, ".")))
}

// Stat implements Provider. A symlink is its target's info under its own
// name.
func (p localProvider) Stat(_ context.Context, name string) (fi FileInfo, err error) {
	err = p.in(name, func(r *os.Root, rel string) error {
		info, err := r.Stat(rel)
		if err != nil {
			return err
		}
		fi = localFileInfo(info, func() (io.ReaderAt, func() error, error) {
			f, err := r.Open(rel)
			if err != nil {
				return nil, nil, err
			}
			return f, f.Close, nil
		})
		return nil
	})
	return fi, err
}

// OpenFile implements Provider. A directory is no file to open, as on
// every other backend.
func (p localProvider) OpenFile(_ context.Context, name string, flag int, perm fs.FileMode) (f File, err error) {
	err = p.in(name, func(r *os.Root, rel string) error {
		osf, err := r.OpenFile(rel, flag, perm)
		if err != nil {
			return err
		}
		if fi, err := osf.Stat(); err == nil && fi.IsDir() {
			_ = osf.Close()
			return fmt.Errorf("%w: %s", ErrIsDir, name)
		}
		f = &localFile{File: osf}
		return nil
	})
	return f, err
}

// PutFile writes a whole file in one call.
func (p localProvider) PutFile(_ context.Context, name string, data []byte) error {
	return p.in(name, func(r *os.Root, rel string) error {
		return r.WriteFile(rel, data, 0o644) //nolint:gosec // G306: agent workspace files are group/world-readable by design
	})
}

// OpenDocument translates local file bytes into Document IR.
func (p localProvider) OpenDocument(ctx context.Context, name string, reg *ContentRegistry) (doc Document, err error) {
	err = p.in(name, func(r *os.Root, rel string) error {
		info, err := r.Stat(rel)
		switch {
		case err != nil:
			return err
		case info.IsDir():
			return fmt.Errorf("%w: %s", ErrIsDir, name)
		case info.Size() > int64(MaxReadFileBytes):
			return errFileExceeds(MaxReadFileBytes)
		}
		data, err := r.ReadFile(rel)
		if err != nil {
			return err
		}
		fi := localFileInfo(info, func() (io.ReaderAt, func() error, error) {
			return bytes.NewReader(data), func() error { return nil }, nil
		})
		doc, err = decodeProviderDocument(ctx, name, fi, data, reg)
		return err
	})
	return doc, err
}

// ReadDir implements Provider.
func (p localProvider) ReadDir(_ context.Context, name string) (out []DirEntry, err error) {
	err = p.in(name, func(r *os.Root, rel string) error {
		dir, err := r.Open(rel)
		if err != nil {
			return err
		}
		defer dir.Close()
		entries, err := dir.ReadDir(-1)
		if err != nil {
			return err
		}
		out = make([]DirEntry, len(entries))
		for i, e := range entries {
			out[i] = DirEntry{Name: e.Name(), IsDir: e.IsDir(), Type: e.Type()}
		}
		return nil
	})
	return out, err
}

// Remove implements Provider.
func (p localProvider) Remove(_ context.Context, name string) error {
	return p.in(name, func(r *os.Root, rel string) error { return r.Remove(rel) })
}

// MkdirAll implements Provider.
func (p localProvider) MkdirAll(_ context.Context, name string, perm fs.FileMode) error {
	return p.in(name, func(r *os.Root, rel string) error { return r.MkdirAll(rel, perm) })
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

// Local opens providers under a fixed jail. Params: "subpath" (optional),
// "session_scoped=true" (optional: the session's own directory, as
// SessionLocal gives). The jail stays in the closure; providers never
// expose host roots.
func Local(base string) Open { return local(base, false) }

// SessionLocal is Local where each session gets its own directory under
// base, so no session sees another's files. A child session (a specialist
// or a job) gets a directory of its own too.
func SessionLocal(base string) Open { return local(base, true) }

func local(base string, perSession bool) Open {
	return func(_ context.Context, sessionID string, b mount.Binding) (Provider, error) {
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
		if (perSession || b.Params["session_scoped"] == "true") && sessionID != "" {
			// One directory name: a child's id has slashes, which escape.
			dir := url.PathEscape(sessionID)
			if err := safeSegment(dir); err != nil {
				return nil, err
			}
			rel = filepath.Join(rel, dir)
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
