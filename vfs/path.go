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

func validMemberName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || strings.ContainsAny(name, "/\\\x00") || name == "." || name == ".." {
		return fmt.Errorf("%w: alias must be one path segment", ErrInvalidPath)
	}
	return nil
}
