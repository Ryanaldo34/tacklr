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
	}, nil
}

// DocumentID returns the stable brain id for a virtual path under this scope.
func (x *MountIndexer) DocumentID(virtualPath string) uuid.UUID {
	ns := *x.Scope.Namespace
	return uuid.NewSHA1(documentIDNS, []byte(ns.String()+"\x00"+virtualPath))
}

// indexOutcome classifies a successful IndexPath for Stats.
type indexOutcome int

const (
	outcomeWrote indexOutcome = iota
	outcomeSkipped
	outcomeRemoved
	outcomeDir
)

// IndexPath indexes one virtual file (or removes the brain mirror if missing).
func (x *MountIndexer) IndexPath(ctx context.Context, virtualPath string) error {
	_, err := x.indexPath(ctx, virtualPath)
	return err
}

func (x *MountIndexer) indexPath(ctx context.Context, virtualPath string) (indexOutcome, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cleaned, err := cleanPath(virtualPath)
	if err != nil {
		return 0, err
	}
	st, err := x.VFS.Stat(ctx, cleaned)
	if err != nil {
		if errors.Is(err, vfs.ErrNotExist) {
			if err := x.removeIndex(ctx, cleaned); err != nil {
				return 0, err
			}
			return outcomeRemoved, nil
		}
		return 0, err
	}
	if st.IsDir {
		return outcomeDir, nil
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
		case outcomeWrote:
			stats.Indexed++
		case outcomeSkipped, outcomeDir:
			stats.Skipped++
		case outcomeRemoved:
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

func (x *MountIndexer) indexFile(ctx context.Context, vpath string, st vfs.FileInfo) (indexOutcome, error) {
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

	// Extension gate before open when known binary.
	mt := vfs.DetectMediaType(vpath, nil)
	if mt != "application/octet-stream" && !vfs.IsTextLike(mt) {
		return outcomeSkipped, nil
	}

	f, err := x.VFS.Open(ctx, vpath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	chunks, hash, mediaType, nBytes, err := streamChunks(ctx, f, vpath, linesPer, maxBytes)
	if err != nil {
		return 0, err
	}
	if mediaType == "" {
		return outcomeSkipped, nil // binary skip
	}

	parentID := x.DocumentID(vpath)
	// Hash skip when unchanged.
	if existing, err := x.Brain.Read(ctx, x.Scope, parentID); err == nil {
		if h, _ := existing.Properties[PropContentHash].(string); h == hash {
			return outcomeSkipped, nil
		}
	} else if !errors.Is(err, brain.ErrNotFound) {
		return 0, err
	}

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
		Title:       path.Base(vpath),
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
		return 0, err
	}

	// Soft-delete obsolete trailing chunks, then upsert current set.
	old, err := x.Brain.ListChildren(ctx, x.Scope, parentID)
	if err != nil {
		return 0, err
	}
	for _, c := range old {
		if c.Position == nil || *c.Position > len(chunks) {
			if err := x.Brain.SoftDelete(ctx, x.Scope, c.ID); err != nil && !errors.Is(err, brain.ErrNotFound) {
				return 0, err
			}
		}
	}

	for i, ch := range chunks {
		pos := i + 1
		id := chunkID(parentID, pos)
		obj := brain.Object{
			ID:       id,
			Kind:     chunkKind,
			Title:    fmt.Sprintf("%s:%d-%d", path.Base(vpath), ch.StartLine, ch.EndLine),
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
		if _, err := x.Brain.Put(ctx, x.Scope, obj); err != nil {
			return 0, err
		}
	}
	return outcomeWrote, nil
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
	Text      string
	StartLine int
	EndLine   int
	ByteStart int64
	ByteEnd   int64
}

func streamChunks(ctx context.Context, r io.Reader, vpath string, linesPerChunk int, maxBytes int64) ([]chunkDraft, string, string, int64, error) {
	h := sha256.New()
	br := bufio.NewReaderSize(io.TeeReader(r, h), 64*1024)
	var (
		out            []chunkDraft
		lineBuf        []string
		chunkStart     = 1
		chunkByteStart int64
		byteOff        int64
		lineNo         int
		scanned        int64
		checkedBinary  bool
		mediaType      string
	)
	flush := func() {
		if len(lineBuf) == 0 {
			return
		}
		text := strings.Join(lineBuf, "\n")
		endLine := chunkStart + len(lineBuf) - 1
		out = append(out, chunkDraft{
			Text:      text,
			StartLine: chunkStart,
			EndLine:   endLine,
			ByteStart: chunkByteStart,
			ByteEnd:   byteOff,
		})
		lineBuf = lineBuf[:0]
		chunkStart = endLine + 1
		chunkByteStart = byteOff
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, "", "", 0, err
		}
		s, err := br.ReadString('\n')
		if len(s) == 0 && errors.Is(err, io.EOF) {
			break
		}
		if !checkedBinary {
			checkedBinary = true
			sample := []byte(s)
			if len(sample) > 512 {
				sample = sample[:512]
			}
			if bytes.IndexByte(sample, 0) >= 0 || !utf8.Valid(sample) {
				return nil, "", "", 0, nil // binary skip
			}
			mediaType = vfs.DetectMediaType(vpath, sample)
			if !vfs.IsTextLike(mediaType) && mediaType != "application/octet-stream" {
				return nil, "", "", 0, nil
			}
			if mediaType == "application/octet-stream" {
				mediaType = "text/plain"
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
		lineNo++
		lineBuf = append(lineBuf, line)
		byteOff += int64(len(s))
		if len(lineBuf) >= linesPerChunk {
			flush()
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, "", "", 0, err
		}
	}
	flush()
	sum := hex.EncodeToString(h.Sum(nil))
	return out, sum, mediaType, scanned, nil
}

func chunkID(parent uuid.UUID, pos int) uuid.UUID {
	return uuid.NewSHA1(parent, []byte(fmt.Sprintf("chunk:%d", pos)))
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
	st, err := ms.Stat(ctx, vpath)
	if err != nil {
		return err
	}
	if !st.IsDir {
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
		if e.IsDir {
			if err := walk(ctx, ms, child, fn); err != nil {
				return err
			}
			continue
		}
		if err := fn(child, false); err != nil {
			return err
		}
	}
	return nil
}
