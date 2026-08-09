package vfs

import (
	"bufio"
	"context"
	"errors"
	"io"
	"unicode/utf8"
)

// ReadLines streams virtualPath and returns lines in the half-open 1-based range
// [start, end), matching TextDocument line rules (\n separator; \r kept).
//
// Only the returned window is retained. Stops after the last needed line when
// possible. Enforces MaxReadFileBytes scanned and MaxLineBytes per line.
func (m *MountSession) ReadLines(ctx context.Context, virtualPath string, start, end int) ([]string, error) {
	if start < 1 || end < start {
		return nil, ErrLineOutOfRange
	}
	if doc, _, _, _, ok := m.cache.get(virtualPath); ok {
		return doc.Lines(start, end)
	}

	f, err := m.Open(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if start == 1 && end == 1 {
		return []string{}, nil
	}
	if fi, err := f.Stat(); err == nil && fi.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}
	return readLineRange(f, start, end)
}

func readLineRange(r io.Reader, start, end int) ([]string, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	lineNo, scanned := 0, 0
	out := make([]string, 0, min(end-start, 64))

	for {
		s, err := br.ReadString('\n')
		if len(s) == 0 && errors.Is(err, io.EOF) {
			break
		}
		scanned += len(s)
		if scanned > MaxReadFileBytes {
			return nil, errFileExceeds(MaxReadFileBytes)
		}
		if len(s) > 0 && s[len(s)-1] == '\n' {
			s = s[:len(s)-1]
		}
		if len(s) > MaxLineBytes {
			return nil, ErrLineTooLong
		}
		if !utf8.ValidString(s) {
			return nil, ErrInvalidUTF8
		}
		lineNo++
		if lineNo >= start && lineNo < end {
			out = append(out, s)
		}
		if end > start && lineNo >= end-1 {
			return out, nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if end > lineNo+1 {
		return nil, ErrLineOutOfRange
	}
	return out, nil
}
