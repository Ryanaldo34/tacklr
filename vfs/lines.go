package vfs

import (
	"bufio"
	"context"
	"errors"
	"io"
	"unicode/utf8"
)

// MaxLineScanBytes caps how many bytes a single streaming ReadLines call may
// read from the start of a file (or from a seek point later). Distinct from
// MaxReadFileBytes (full IR materialize).
const MaxLineScanBytes = 64 << 20 // 64 MiB

// MaxLinesPerWindow caps how many lines one ReadLines call may return.
const MaxLinesPerWindow = 500

// LineWindow is one progressive page of lines from a virtual path.
//
// Start/End are the half-open 1-based range requested (End may be clamped by
// MaxLinesPerWindow). EOF is true when the file ended at or before the last
// returned line. NextStart is Start+Returned (useful when !EOF for paging).
//
// Rev is set when the window is served from session IR cache (cheap full-body
// hash). Stream reads leave Rev empty; callers use ContentRev when needed.
type LineWindow struct {
	Path      string
	Start     int
	End       int
	Lines     []string
	Returned  int
	EOF       bool
	NextStart int
	Rev       ContentRev
}

// ReadLines streams virtualPath and returns a line window for the half-open
// 1-based range [start, end).
//
// Large files are allowed: unlike ReadFile/ReadText there is no full-object
// size reject. Only MaxLineScanBytes (bytes read this call), MaxLineBytes
// (per line), and MaxLinesPerWindow apply.
//
// If the file ends before end, the available lines are returned with EOF=true
// (not ErrLineOutOfRange). ErrLineOutOfRange only when start is past EOF.
func (m *MountSession) ReadLines(ctx context.Context, virtualPath string, start, end int) (LineWindow, error) {
	if start < 1 || end < start {
		return LineWindow{}, ErrLineOutOfRange
	}
	if end-start > MaxLinesPerWindow {
		end = start + MaxLinesPerWindow
	}

	cleaned, err := cleanVirtualPath(virtualPath)
	if err != nil {
		return LineWindow{}, err
	}
	if doc, _, _, _, ok := m.cache.get(cleaned); ok {
		return lineWindowFromDoc(cleaned, doc, start, end)
	}

	// Prefer full IR when the object is within the materialize cap (session-visible).
	if fi, stErr := m.Stat(ctx, cleaned); stErr == nil && !fi.IsDir && fi.Size >= 0 && fi.Size <= int64(MaxReadFileBytes) {
		if doc, rerr := m.ReadText(ctx, cleaned); rerr == nil {
			return lineWindowFromDoc(cleaned, doc, start, end)
		}
	}

	f, err := m.Open(ctx, cleaned)
	if err != nil {
		return LineWindow{}, err
	}
	defer f.Close()

	if start == 1 && end == 1 {
		return LineWindow{Path: cleaned, Start: 1, End: 1, NextStart: 1}, nil
	}
	return readLineRange(f, cleaned, start, end)
}

func lineWindowFromDoc(path string, doc Textual, start, end int) (LineWindow, error) {
	n := doc.LineCount()
	if start > n+1 {
		return LineWindow{}, ErrLineOutOfRange
	}
	eof := false
	if end > n+1 {
		end = n + 1
		eof = true
	}
	lines, err := doc.Lines(start, end)
	if err != nil {
		return LineWindow{}, err
	}
	for _, line := range lines {
		if len(line) > MaxLineBytes {
			return LineWindow{}, ErrLineTooLong
		}
	}
	return LineWindow{
		Path: path, Start: start, End: end, Lines: lines,
		Returned: len(lines), EOF: eof, NextStart: start + len(lines),
		Rev: ContentRev{Path: path, Hash: ContentHash(doc.Text())},
	}, nil
}

func readLineRange(r io.Reader, path string, start, end int) (LineWindow, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	lineNo, scanned := 0, 0
	out := make([]string, 0, min(end-start, 64))

	for {
		s, err := br.ReadString('\n')
		if len(s) == 0 && errors.Is(err, io.EOF) {
			break
		}
		scanned += len(s)
		if scanned > MaxLineScanBytes {
			return LineWindow{}, errFileExceeds(MaxLineScanBytes)
		}
		if len(s) > 0 && s[len(s)-1] == '\n' {
			s = s[:len(s)-1]
		}
		if len(s) > MaxLineBytes {
			return LineWindow{}, ErrLineTooLong
		}
		if !utf8.ValidString(s) {
			return LineWindow{}, ErrInvalidUTF8
		}
		lineNo++
		if lineNo >= start && lineNo < end {
			out = append(out, s)
		}
		// Full requested window collected; more file may remain.
		if end > start && lineNo >= end-1 {
			return LineWindow{
				Path: path, Start: start, End: end, Lines: out,
				Returned: len(out), EOF: false, NextStart: start + len(out),
			}, nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return LineWindow{}, err
		}
	}

	// Hit EOF.
	if start > lineNo+1 {
		return LineWindow{}, ErrLineOutOfRange
	}
	return LineWindow{
		Path: path, Start: start, End: end, Lines: out,
		Returned: len(out), EOF: true, NextStart: start + len(out),
	}, nil
}
