package vfs

import (
	"fmt"
	"path"
	"strings"
)

// CleanPath requires an absolute virtual path with no backslash or NUL.
// It returns path.Clean(s).
func CleanPath(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" || !path.IsAbs(s) || strings.ContainsAny(s, "\\\x00") {
		return "", ErrInvalidPath
	}
	return path.Clean(s), nil
}

// ValidMountPoint reports whether point is a single-segment virtual path
// (/contracts). FUSE and client binds require this shape.
func ValidMountPoint(point string) error {
	cleaned, err := CleanPath(point)
	if err != nil {
		return err
	}
	if cleaned == "/" || strings.Count(cleaned, "/") != 1 {
		return fmt.Errorf("%w: mount point must be one segment", ErrInvalidPath)
	}
	return nil
}
