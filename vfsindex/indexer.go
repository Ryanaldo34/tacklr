package vfsindex

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Stable UUID namespace for path-derived document ids (not a secret).
var documentIDNS = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// Defaults for streaming index.
const (
	DefaultLinesPerChunk = 40
	DefaultMaxIndexBytes = 64 << 20 // 64 MiB per file
)

// MountIndexer streams text-like mount files into brain Document + Chunk objects.
type MountIndexer struct {
	VFS   *vfs.MountSession
	Brain *brain.Engine
	Scope brain.Scope

	DocumentKind  string // default Document
	ChunkKind     string // default Chunk
	LinesPerChunk int    // default DefaultLinesPerChunk
	MaxIndexBytes int64  // default DefaultMaxIndexBytes

	// nsKey is Scope.Namespace.String() cached for DocumentID (avoids per-call alloc).
	nsKey string
}

// IndexOpts configures a tree walk.
type IndexOpts struct {
	// MaxFiles stops after this many files opened (0 = unlimited).
	MaxFiles int
}

// Stats summarizes IndexPrefix work.
type Stats struct {
	Indexed int // files written or re-chunked
	Skipped int // hash match, binary, non-text, or empty skip
	Removed int // missing paths soft-deleted from brain
}

// NewMountIndexer validates required fields. Scope.Namespace must be non-nil
// (brain Put requires namespace_id).
func NewMountIndexer(ms *vfs.MountSession, eng *brain.Engine, scope brain.Scope) (*MountIndexer, error) {
	if ms == nil {
		return nil, fmt.Errorf("vfsindex: MountSession required")
	}
	if eng == nil {
		return nil, fmt.Errorf("vfsindex: brain Engine required")
	}
	if scope.Namespace == nil || *scope.Namespace == uuid.Nil {
		return nil, fmt.Errorf("vfsindex: scope namespace required")
	}
	return &MountIndexer{
		VFS:           ms,
		Brain:         eng,
		Scope:         scope,
		DocumentKind:  DefaultDocumentKind,
		ChunkKind:     DefaultChunkKind,
		LinesPerChunk: DefaultLinesPerChunk,
		MaxIndexBytes: DefaultMaxIndexBytes,
		nsKey:         scope.Namespace.String(),
	}, nil
}

// DocumentID returns the stable brain id for a virtual path under this scope.
func (x *MountIndexer) DocumentID(virtualPath string) uuid.UUID {
	nsKey := x.nsKey
	if nsKey == "" && x.Scope.Namespace != nil {
		nsKey = x.Scope.Namespace.String()
	}
	// Single buffer: nsKey is usually precomputed in NewMountIndexer.
	buf := make([]byte, 0, len(nsKey)+1+len(virtualPath))
	buf = append(buf, nsKey...)
	buf = append(buf, 0)
	buf = append(buf, virtualPath...)
	return uuid.NewSHA1(documentIDNS, buf)
}

// PathIndexResult is a compact outcome of indexing one path
// (indexed|skipped|removed|directory). Used by agent tools and hosts.
type PathIndexResult string

const (
	// PathIndexed means Document/Chunks were written or re-chunked.
	PathIndexed PathIndexResult = "indexed"
	// PathSkipped means hash match, binary, non-text, or empty skip.
	PathSkipped PathIndexResult = "skipped"
	// PathRemoved means the path was missing and any brain mirror was soft-deleted.
	PathRemoved PathIndexResult = "removed"
	// PathDirectory means the path is a directory (IndexPath is a no-op for dirs).
	PathDirectory PathIndexResult = "directory"
)

// IndexPath indexes one virtual file (or removes the brain mirror if missing).
func (x *MountIndexer) IndexPath(ctx context.Context, virtualPath string) error {
	_, err := x.indexPath(ctx, virtualPath)
	return err
}

// IndexPathResult indexes one path and returns a compact outcome for tools/hosts.
func (x *MountIndexer) IndexPathResult(ctx context.Context, virtualPath string) (PathIndexResult, error) {
	return x.indexPath(ctx, virtualPath)
}

// IndexFileResult indexes a path already known to be an existing file (caller Stat'd).
// Skips a second Stat round-trip — useful for remote mounts and batch tools that
// pre-validate paths before any write work.
func (x *MountIndexer) IndexFileResult(ctx context.Context, virtualPath string, st vfs.FileInfo) (PathIndexResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cleaned, err := cleanPath(virtualPath)
	if err != nil {
		return "", err
	}
	if x.skipIndex(cleaned) {
		return PathSkipped, nil
	}
	if st.IsDir {
		return PathDirectory, nil
	}
	return x.indexFile(ctx, cleaned, st)
}

// UnindexPath soft-deletes the brain Document/Chunks for virtualPath without
// touching the VFS file. Returns true when a mirror was present and removed,
// false when nothing was indexed (idempotent noop).
func (x *MountIndexer) UnindexPath(ctx context.Context, virtualPath string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	cleaned, err := cleanPath(virtualPath)
	if err != nil {
		return false, err
	}
	parentID := x.DocumentID(cleaned)
	if _, err := x.Brain.Read(ctx, x.Scope, parentID); err != nil {
		if errors.Is(err, brain.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if err := x.removeIndex(ctx, cleaned); err != nil {
		return false, err
	}
	return true, nil
}

func (x *MountIndexer) skipIndex(virtualPath string) bool {
	if x == nil || x.VFS == nil {
		return false
	}
	spec, err := x.VFS.SpecAt(virtualPath)
	return err == nil && NormalizePolicy(spec.IndexPolicy) == PolicyNone
}

func (x *MountIndexer) indexPath(ctx context.Context, virtualPath string) (PathIndexResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cleaned, err := cleanPath(virtualPath)
	if err != nil {
		return "", err
	}
	if x.skipIndex(cleaned) {
		return PathSkipped, nil
	}
	st, err := x.VFS.Stat(ctx, cleaned)
	if err != nil {
		if errors.Is(err, vfs.ErrNotExist) {
			if err := x.removeIndex(ctx, cleaned); err != nil {
				return "", err
			}
			return PathRemoved, nil
		}
		return "", err
	}
	if st.IsDir {
		return PathDirectory, nil
	}
	return x.indexFile(ctx, cleaned, st)
}

// IndexPrefix walks a directory (or single file) and indexes text-like files.
func (x *MountIndexer) IndexPrefix(ctx context.Context, prefix string, opts IndexOpts) (Stats, error) {
	var stats Stats
	cleaned, err := cleanPath(prefix)
	if err != nil {
		return stats, err
	}
	if x.skipIndex(cleaned) {
		return stats, nil
	}
	files := 0
	err = walk(ctx, x.VFS, cleaned, func(vpath string, isDir bool) error {
		if isDir {
			return nil
		}
		if opts.MaxFiles > 0 && files >= opts.MaxFiles {
			return errIndexLimit
		}
		files++
		out, err := x.indexPath(ctx, vpath)
		if err != nil {
			return err
		}
		switch out {
		case PathIndexed:
			stats.Indexed++
		case PathSkipped, PathDirectory:
			stats.Skipped++
		case PathRemoved:
			stats.Removed++
		}
		return nil
	})
	if err != nil && !errors.Is(err, errIndexLimit) {
		return stats, err
	}
	return stats, nil
}

var errIndexLimit = errors.New("vfsindex: file limit")

func (x *MountIndexer) indexFile(ctx context.Context, vpath string, st vfs.FileInfo) (PathIndexResult, error) {
	linesPer := x.LinesPerChunk
	if linesPer <= 0 {
		linesPer = DefaultLinesPerChunk
	}
	maxBytes := x.MaxIndexBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxIndexBytes
	}
	docKind := x.DocumentKind
	if docKind == "" {
		docKind = DefaultDocumentKind
	}
	chunkKind := x.ChunkKind
	if chunkKind == "" {
		chunkKind = DefaultChunkKind
	}

	mt := st.MediaType
	if mt == "" {
		mt = "application/octet-stream"
	}
	if !vfs.IsTextLike(mt) {
		return PathSkipped, nil
	}

	parentID := x.DocumentID(vpath)
	base := path.Base(vpath)

	// Session IR when available: hash-check before chunking; Structured → block chunks.
	if doc, err := x.VFS.ReadText(ctx, vpath); err == nil {
		body := doc.Text()
		hash := vfs.ContentHash(body)
		mediaType := doc.MediaType()
		if mediaType == "" {
			mediaType = mt
		}
		unchanged, err := x.contentHashUnchanged(ctx, parentID, hash)
		if err != nil {
			return "", err
		}
		if unchanged {
			return PathSkipped, nil
		}
		var chunks []chunkDraft
		if s, ok := doc.(vfs.Structured); ok {
			if blocks := s.Blocks(); len(blocks) > 0 {
				chunks = chunksFromBlocks(base, body, blocks, parentID)
			}
		}
		if len(chunks) == 0 {
			chunks = lineChunksFromText(body, linesPer)
		}
		return x.putFileIndex(ctx, vpath, base, st, parentID, docKind, chunkKind, mediaType, hash, int64(len(body)), chunks)
	}

	// One-pass stream: hash while chunking (re-open for hash-only would double IO
	// on the common AfterPersist "content changed" path).
	f, err := x.VFS.Open(ctx, vpath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	r, ok := f.(io.Reader)
	if !ok {
		return "", fmt.Errorf("vfsindex: file is not readable")
	}
	chunks, hash, nBytes, err := streamChunks(ctx, r, linesPer, maxBytes)
	if err != nil {
		return "", err
	}
	if chunks == nil && hash == "" {
		return PathSkipped, nil // binary skip
	}
	unchanged, err := x.contentHashUnchanged(ctx, parentID, hash)
	if err != nil {
		return "", err
	}
	if unchanged {
		return PathSkipped, nil
	}
	return x.putFileIndex(ctx, vpath, base, st, parentID, docKind, chunkKind, mt, hash, nBytes, chunks)
}

// contentHashUnchanged reports whether the parent Document already has hash.
func (x *MountIndexer) contentHashUnchanged(ctx context.Context, parentID uuid.UUID, hash string) (bool, error) {
	existing, err := x.Brain.Read(ctx, x.Scope, parentID)
	if err != nil {
		if errors.Is(err, brain.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	h, _ := existing.Properties[PropContentHash].(string)
	return h == hash, nil
}

// putFileIndex writes parent Document and replaces Chunk children.
func (x *MountIndexer) putFileIndex(
	ctx context.Context,
	vpath, base string,
	st vfs.FileInfo,
	parentID uuid.UUID,
	docKind, chunkKind, mediaType, hash string,
	nBytes int64,
	chunks []chunkDraft,
) (PathIndexResult, error) {
	mtime := ""
	if !st.ModTime.IsZero() {
		mtime = st.ModTime.UTC().Format(time.RFC3339)
	}
	size := float64(nBytes)
	if st.Size > 0 {
		size = float64(st.Size)
	}

	parent := brain.Object{
		ID:          parentID,
		Kind:        docKind,
		Title:       base,
		Summary:     vpath,
		ContentType: mediaType,
		Properties: map[string]any{
			PropVFSPath:     vpath,
			PropSize:        size,
			PropContentHash: hash,
			PropMediaType:   mediaType,
		},
	}
	if mtime != "" {
		parent.Properties[PropMTime] = mtime
	}
	if _, err := x.Brain.Put(ctx, x.Scope, parent); err != nil {
		return "", err
	}

	old, err := x.Brain.ListChildren(ctx, x.Scope, parentID)
	if err != nil {
		return "", err
	}
	for _, c := range old {
		if err := x.Brain.SoftDelete(ctx, x.Scope, c.ID); err != nil && !errors.Is(err, brain.ErrNotFound) {
			return "", err
		}
	}

	for i, ch := range chunks {
		pos := i + 1
		id := ch.ID
		if id == uuid.Nil {
			id = chunkID(parentID, pos)
		}
		title := ch.Title
		if title == "" {
			title = base + ":" + strconv.Itoa(ch.StartLine) + "-" + strconv.Itoa(ch.EndLine)
		}
		obj := brain.Object{
			ID:       id,
			Kind:     chunkKind,
			Title:    title,
			Content:  ch.Text,
			ParentID: &parentID,
			Position: &pos,
			Properties: map[string]any{
				PropStartLine: float64(ch.StartLine),
				PropEndLine:   float64(ch.EndLine),
				PropByteStart: float64(ch.ByteStart),
				PropByteEnd:   float64(ch.ByteEnd),
			},
		}
		if ch.BlockID != "" {
			obj.Properties[PropBlockID] = ch.BlockID
			obj.Properties[PropHeadingPath] = ch.BlockID
		}
		if _, err := x.Brain.Put(ctx, x.Scope, obj); err != nil {
			return "", err
		}
	}
	return PathIndexed, nil
}

func chunksFromBlocks(fileTitle, body string, blocks []vfs.Block, parentID uuid.UUID) []chunkDraft {
	// One line-start scan; span text is a body substring (no Split/Join copies).
	starts := lineStarts(body)
	nLines := len(starts)
	out := make([]chunkDraft, 0, len(blocks))
	var textBuf strings.Builder
	for _, b := range blocks {
		start, end := b.Style.Span.StartLine, b.Style.Span.EndLine
		if start < 1 {
			continue
		}
		if end > nLines+1 {
			end = nLines + 1
		}
		if start > end {
			continue
		}
		seg := lineSpan(body, starts, start, end)
		title := b.Text
		if title == "" {
			title = b.ID
		}
		textBuf.Reset()
		textBuf.Grow(len(fileTitle) + 1 + len(title) + 1 + len(seg))
		textBuf.WriteString(fileTitle)
		textBuf.WriteByte('\n')
		textBuf.WriteString(title)
		textBuf.WriteByte('\n')
		textBuf.WriteString(seg)
		out = append(out, chunkDraft{
			ID:        chunkIDByKey(parentID, b.ID),
			Title:     fileTitle + "#" + b.ID,
			Text:      textBuf.String(),
			StartLine: start,
			EndLine:   end,
			BlockID:   b.ID,
		})
	}
	return out
}

func lineChunksFromText(body string, linesPer int) []chunkDraft {
	if linesPer <= 0 {
		linesPer = DefaultLinesPerChunk
	}
	starts := lineStarts(body)
	n := len(starts)
	out := make([]chunkDraft, 0, (n+linesPer-1)/linesPer)
	var byteOff int64
	for i := 0; i < n; {
		end := i + linesPer
		if end > n {
			end = n
		}
		seg := lineSpan(body, starts, i+1, end+1)
		out = append(out, chunkDraft{
			Text:      seg,
			StartLine: i + 1,
			EndLine:   end, // inclusive, same as streamChunks
			ByteStart: byteOff,
			ByteEnd:   byteOff + int64(len(seg)),
		})
		byteOff += int64(len(seg)) + 1
		i = end
	}
	return out
}

// lineStarts returns byte offsets of each line start (matches strings.Split count).
func lineStarts(text string) []int {
	starts := make([]int, 1, strings.Count(text, "\n")+1)
	starts[0] = 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

// lineSpan returns body lines [start, end) as a substring of body (1-based half-open).
func lineSpan(body string, starts []int, start, end int) string {
	if start < 1 || end <= start {
		return ""
	}
	lo := start - 1
	if lo >= len(starts) {
		return ""
	}
	startByte := starts[lo]
	hi := end - 1
	if hi >= len(starts) {
		return body[startByte:]
	}
	endByte := starts[hi] - 1 // drop the '\n' that ends the last included line
	if endByte < startByte {
		return ""
	}
	return body[startByte:endByte]
}

func (x *MountIndexer) removeIndex(ctx context.Context, vpath string) error {
	parentID := x.DocumentID(vpath)
	children, err := x.Brain.ListChildren(ctx, x.Scope, parentID)
	if err != nil && !errors.Is(err, brain.ErrNotFound) {
		// ListChildren may fail if parent missing — treat as gone.
		if _, rerr := x.Brain.Read(ctx, x.Scope, parentID); errors.Is(rerr, brain.ErrNotFound) {
			return nil
		}
		return err
	}
	for _, c := range children {
		if err := x.Brain.SoftDelete(ctx, x.Scope, c.ID); err != nil && !errors.Is(err, brain.ErrNotFound) {
			return err
		}
	}
	if err := x.Brain.SoftDelete(ctx, x.Scope, parentID); err != nil && !errors.Is(err, brain.ErrNotFound) {
		return err
	}
	return nil
}

type chunkDraft struct {
	ID        uuid.UUID
	Title     string
	Text      string
	StartLine int
	EndLine   int
	ByteStart int64
	ByteEnd   int64
	BlockID   string
}

func chunkIDByKey(parent uuid.UUID, key string) uuid.UUID {
	return uuid.NewSHA1(parent, []byte("block:"+key))
}

func streamChunks(ctx context.Context, r io.Reader, linesPerChunk int, maxBytes int64) ([]chunkDraft, string, int64, error) {
	h := sha256.New()
	br := bufio.NewReaderSize(io.TeeReader(r, h), 64*1024)
	var (
		out            []chunkDraft
		buf            strings.Builder
		linesInChunk   int
		chunkStart     = 1
		chunkByteStart int64
		byteOff        int64
		scanned        int64
		checkedBinary  bool
	)
	if linesPerChunk > 0 {
		buf.Grow(linesPerChunk * 64)
	}
	flush := func() {
		if linesInChunk == 0 {
			return
		}
		text := buf.String()
		endLine := chunkStart + linesInChunk - 1
		out = append(out, chunkDraft{
			Text:      text,
			StartLine: chunkStart,
			EndLine:   endLine,
			ByteStart: chunkByteStart,
			ByteEnd:   byteOff,
		})
		buf.Reset()
		linesInChunk = 0
		chunkStart = endLine + 1
		chunkByteStart = byteOff
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, "", 0, err
		}
		s, err := br.ReadString('\n')
		if len(s) == 0 && errors.Is(err, io.EOF) {
			break
		}
		if !checkedBinary {
			checkedBinary = true
			n := len(s)
			if n > 512 {
				n = 512
			}
			sample := []byte(s[:n])
			if bytes.IndexByte(sample, 0) >= 0 || !utf8.Valid(sample) {
				return nil, "", 0, nil // binary skip
			}
		}
		scanned += int64(len(s))
		if scanned > maxBytes {
			flush()
			break
		}
		line := s
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) > vfs.MaxLineBytes {
			line = line[:vfs.MaxLineBytes]
		}
		if linesInChunk > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)
		linesInChunk++
		byteOff += int64(len(s))
		if linesInChunk >= linesPerChunk {
			flush()
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, "", 0, err
		}
	}
	flush()
	sum := hex.EncodeToString(h.Sum(nil))
	return out, sum, scanned, nil
}

func chunkID(parent uuid.UUID, pos int) uuid.UUID {
	// "chunk:" + decimal pos; 24 bytes covers typical positions without growth.
	buf := make([]byte, 0, 24)
	buf = append(buf, "chunk:"...)
	buf = strconv.AppendInt(buf, int64(pos), 10)
	return uuid.NewSHA1(parent, buf)
}

func cleanPath(p string) (string, error) {
	if p == "" || !path.IsAbs(p) || strings.ContainsAny(p, "\\\x00") {
		return "", vfs.ErrInvalidPath
	}
	return path.Clean(p), nil
}

func walk(ctx context.Context, ms *vfs.MountSession, vpath string, fn func(vpath string, isDir bool) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Root only: one Stat. Descendants reuse DirEntry.IsDir (no re-Stat per dir).
	st, err := ms.Stat(ctx, vpath)
	if err != nil {
		return err
	}
	return walkKnown(ctx, ms, vpath, st.IsDir, fn)
}

func walkKnown(ctx context.Context, ms *vfs.MountSession, vpath string, isDir bool, fn func(vpath string, isDir bool) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isDir {
		return fn(vpath, false)
	}
	if err := fn(vpath, true); err != nil {
		return err
	}
	ents, err := ms.ReadDir(ctx, vpath)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		child := path.Join(vpath, e.Name)
		if err := walkKnown(ctx, ms, child, e.IsDir, fn); err != nil {
			return err
		}
	}
	return nil
}
