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
// Memory: only the returned window is retained. Lines before start are discarded
// after counting. Scanning stops after line end-1 when the range is fully
// satisfied (does not read the rest of a large file).
//
// Enforces MaxReadFileBytes on bytes scanned and MaxLineBytes per line.
// Prefer this over ReadText when tools only need a window (no full IR, no edit).
func (m *MountSession) ReadLines(ctx context.Context, virtualPath string, start, end int) ([]string, error) {
	if start < 1 || end < start {
		return nil, ErrLineOutOfRange
	}
	f, err := m.Open(ctx, virtualPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// [1,1) is valid for any existing file (including empty) — no scan.
	if start == 1 && end == 1 {
		return []string{}, nil
	}

	if fi, stErr := f.Stat(); stErr == nil && fi.Size > int64(MaxReadFileBytes) {
		return nil, errFileExceeds(MaxReadFileBytes)
	}

	return readLineRange(f, start, end)
}

func readLineRange(r io.Reader, start, end int) ([]string, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	lineNo := 0
	scanned := 0
	out := make([]string, 0, min(end-start, 64))

	for {
		chunk, err := readLineSlice(br)
		if len(chunk) == 0 && errors.Is(err, io.EOF) {
			break
		}
		scanned += len(chunk)
		if scanned > MaxReadFileBytes {
			return nil, errFileExceeds(MaxReadFileBytes)
		}
		line := chunk
		if n := len(line); n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
		if len(line) > MaxLineBytes {
			return nil, ErrLineTooLong
		}
		if !utf8.Valid(line) {
			return nil, ErrInvalidUTF8
		}
		lineNo++
		if lineNo >= start && lineNo < end {
			out = append(out, string(line))
		}
		// Window complete — stop; end-1 exists so end <= lineCount+1.
		if end > start && lineNo >= end-1 && lineNo >= start {
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

// readLineSlice reads through the next '\n' or EOF.
func readLineSlice(br *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		part, err := br.ReadSlice('\n')
		if err == nil {
			if len(buf) == 0 {
				return part, nil
			}
			return append(buf, part...), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			buf = append(buf, part...)
			if len(buf) > MaxLineBytes+1 {
				return nil, ErrLineTooLong
			}
			continue
		}
		if len(part) > 0 {
			if len(buf) == 0 {
				return part, err
			}
			return append(buf, part...), err
		}
		if len(buf) > 0 {
			return buf, err
		}
		return nil, err
	}
}
