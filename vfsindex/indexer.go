package vfsindex

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Stable UUID namespace for path-derived document ids (not a secret).
var documentIDNS = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// DefaultMaxIndexBytes caps how much of a plain-text file is indexed.
const DefaultMaxIndexBytes = 64 << 20 // 64 MiB per file

// maxConflictRetries bounds re-indexing a file another indexer committed first.
const maxConflictRetries = 3

// MountIndexer streams text-like mount files into brain Document + Chunk objects.
type MountIndexer struct {
	VFS   *vfs.MountSession
	Brain *brain.Engine
	Scope brain.Scope

	DocumentKind string // default Document
	ChunkKind    string // default Chunk
	// Chunking sizes the chunks; zero fields take DefaultChunkSizes.
	Chunking ChunkSizes
	// MaxIndexBytes caps how much of a plain-text file is read and indexed
	// (default DefaultMaxIndexBytes). Structured files (Markdown, Docs,
	// Sheets) are read whole, up to vfs.MaxReadFileBytes.
	MaxIndexBytes int64

	// Readers returns the source ACL for a file: the principals allowed to
	// read it (for example from Drive or SharePoint sharing). Its result is
	// stored on the Document and every Chunk, so search only shows the file
	// to scopes holding one of those principals. Nil, or an empty result,
	// leaves the file visible to every scope that covers Scope.Namespace.
	Readers ReadersFunc
}

// ReadersFunc resolves the principals that may read virtualPath at its source.
type ReadersFunc func(ctx context.Context, virtualPath string, st vfs.FileInfo) ([]string, error)

// IndexOpts configures a tree walk.
type IndexOpts struct {
	// MaxFiles stops after this many files opened (0 = unlimited).
	MaxFiles int
}

// Stats summarizes IndexPrefix work.
type Stats struct {
	Indexed int // files written or re-chunked
	Skipped int // version match, binary, non-text, or PolicyNone
	Removed int // missing paths soft-deleted from brain
}

// NewMountIndexer validates required fields. Scope.Namespace must be set
// (brain Put requires namespace attrs).
func NewMountIndexer(ms *vfs.MountSession, eng *brain.Engine, scope brain.Scope) (*MountIndexer, error) {
	if ms == nil {
		return nil, fmt.Errorf("vfsindex: MountSession required")
	}
	if eng == nil {
		return nil, fmt.Errorf("vfsindex: brain Engine required")
	}
	if err := scope.Namespace.Validate(); err != nil {
		return nil, fmt.Errorf("vfsindex: scope namespace: %w", err)
	}
	return &MountIndexer{VFS: ms, Brain: eng, Scope: scope}, nil
}

// DocumentID returns the stable brain id for a virtual path under this scope.
func (x *MountIndexer) DocumentID(virtualPath string) uuid.UUID {
	return uuid.NewSHA1(documentIDNS, []byte(x.Scope.Namespace.Key()+"\x00"+virtualPath))
}

// PathIndexResult is a compact outcome of indexing one path
// (indexed|skipped|removed|directory). Used by agent tools and hosts.
type PathIndexResult string

const (
	// PathIndexed means Document/Chunks were written or re-chunked.
	PathIndexed PathIndexResult = "indexed"
	// PathSkipped means version match, binary, non-text, or PolicyNone.
	PathSkipped PathIndexResult = "skipped"
	// PathRemoved means the path was missing and any brain mirror was soft-deleted.
	PathRemoved PathIndexResult = "removed"
	// PathDirectory means the path is a directory (IndexPath does nothing for a directory).
	PathDirectory PathIndexResult = "directory"
)

func (x *MountIndexer) readyPath(ctx context.Context, virtualPath string) (string, PathIndexResult, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	cleaned, err := vfs.CleanPath(virtualPath)
	if err != nil {
		return "", "", err
	}
	if x.skipIndex(cleaned) {
		return cleaned, PathSkipped, nil
	}
	return cleaned, "", nil
}

// IndexFile indexes a path already known to exist (caller Stat'd).
// It does not Stat again, so batch tools can validate once then index.
func (x *MountIndexer) IndexFile(ctx context.Context, virtualPath string, st vfs.FileInfo) (PathIndexResult, error) {
	cleaned, skip, err := x.readyPath(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	if skip != "" {
		return skip, nil
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
	cleaned, err := vfs.CleanPath(virtualPath)
	if err != nil {
		return false, err
	}
	parentID := x.DocumentID(cleaned)
	if _, err := x.Brain.Get(ctx, x.Scope, parentID); err != nil {
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
	spec, err := x.VFS.SpecAt(virtualPath)
	return err == nil && NormalizePolicy(spec.IndexPolicy) == PolicyNone
}

// IndexPath indexes one virtual file, or removes its mirror when the file is missing.
func (x *MountIndexer) IndexPath(ctx context.Context, virtualPath string) (PathIndexResult, error) {
	cleaned, skip, err := x.readyPath(ctx, virtualPath)
	if err != nil {
		return "", err
	}
	if skip != "" {
		return skip, nil
	}

	st, err := x.VFS.Route(ctx, cleaned).
		Stat(ctx)
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
	cleaned, skip, err := x.readyPath(ctx, prefix)
	if err != nil {
		return stats, err
	}
	if skip != "" {
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
		out, err := x.IndexPath(ctx, vpath)
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
	mt := cmp.Or(st.MediaType, "application/octet-stream")
	if !vfs.IsTextLike(mt) && !vfs.IsProjected(mt) {
		return PathSkipped, nil
	}
	sizes, err := x.Chunking.withDefaults()
	if err != nil {
		return "", err
	}
	readers, err := x.readers(ctx, vpath, st)
	if err != nil {
		return "", err
	}
	doc := x.document(vpath, st, mt, readers)
	unchanged, err := x.unchanged(ctx, doc.ID, st.Version, readers)
	if err != nil {
		return "", err
	}
	if unchanged {
		return PathSkipped, nil
	}
	for attempt := 1; ; attempt++ {
		out, err := x.writeFile(ctx, vpath, doc, sizes)
		if !errors.Is(err, brain.ErrConflict) || attempt == maxConflictRetries {
			return out, err
		}
	}
}

// readers resolves the file's source ACL, sorted so a re-index can compare it.
func (x *MountIndexer) readers(ctx context.Context, vpath string, st vfs.FileInfo) ([]string, error) {
	if x.Readers == nil {
		return nil, nil
	}
	got, err := x.Readers(ctx, vpath, st)
	if err != nil {
		return nil, fmt.Errorf("vfsindex: readers for %s: %w", vpath, err)
	}
	if len(got) == 0 {
		return nil, nil
	}
	return slices.Compact(slices.Sorted(slices.Values(got))), nil
}

// document is the Document object that mirrors the file at vpath.
func (x *MountIndexer) document(vpath string, st vfs.FileInfo, mt string, readers []string) brain.Object {
	doc := brain.Object{
		ID:          x.DocumentID(vpath),
		Kind:        cmp.Or(x.DocumentKind, DefaultDocumentKind),
		Title:       path.Base(vpath),
		Summary:     vpath,
		ContentType: mt,
		Readers:     readers,
		Properties: map[string]any{
			PropVFSPath:   vpath,
			PropSize:      float64(st.Size),
			PropMediaType: mt,
		},
	}
	if st.Version != "" {
		doc.Properties[PropSourceVersion] = st.Version
	}
	if !st.ModTime.IsZero() {
		doc.Properties[PropMTime] = st.ModTime.UTC().Format(time.RFC3339)
	}
	return doc
}

// unchanged reports whether the Document already mirrors this version of
// the file with these readers, so the file is not opened at all. A backend
// without versions always reads; unchanged chunks then write nothing.
func (x *MountIndexer) unchanged(ctx context.Context, id uuid.UUID, version string, readers []string) (bool, error) {
	if version == "" {
		return false, nil
	}
	existing, err := x.Brain.Get(ctx, x.Scope, id)
	if errors.Is(err, brain.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	v, _ := existing.Properties[PropSourceVersion].(string)
	return v == version && slices.Equal(existing.Readers, readers), nil
}

// writeFile reads the file once and replaces doc's Chunks with one PutTree.
// Chunks stream into PutTree, which embeds and writes only the ones whose
// text changed; a failure leaves the previous version in place.
func (x *MountIndexer) writeFile(ctx context.Context, vpath string, doc brain.Object, sizes ChunkSizes) (PathIndexResult, error) {
	kind := cmp.Or(x.ChunkKind, DefaultChunkKind)
	structured, err := x.readStructured(ctx, vpath, doc.ContentType)
	if err != nil {
		return "", err
	}
	var parts iter.Seq2[brain.Object, error]
	if structured != nil {
		if text := structured.Text(); isBinarySample([]byte(text[:min(len(text), 512)])) {
			return PathSkipped, nil
		}
		doc.ContentType = cmp.Or(structured.MediaType(), doc.ContentType)
		doc.Properties[PropMediaType] = doc.ContentType
		parts = blockParts(ctx, doc.Title, kind, structured, sizes)
	} else {
		f, err := x.VFS.Route(ctx, vpath).Open(ctx)
		if err != nil {
			return "", err
		}
		defer f.Close()
		r, ok := f.(io.Reader)
		if !ok {
			return "", fmt.Errorf("vfsindex: file is not readable")
		}
		br := bufio.NewReaderSize(r, 64<<10)
		if sample, _ := br.Peek(512); isBinarySample(sample) {
			return PathSkipped, nil
		}
		parts = streamParts(chunks(ctx, br, sizes, cmp.Or(x.MaxIndexBytes, DefaultMaxIndexBytes)), kind)
	}
	if _, err := x.Brain.PutTree(ctx, x.Scope, doc, parts); err != nil {
		return "", err
	}
	return PathIndexed, nil
}

// readStructured decodes a file whose media type has a block tree. It
// returns nil, so the file streams as plain text, for other types and for
// files the document path cannot read: over vfs.MaxReadFileBytes, not
// UTF-8, or on a backend without documents.
func (x *MountIndexer) readStructured(ctx context.Context, vpath, mt string) (vfs.Textual, error) {
	if !vfs.HasStructure(mt) {
		return nil, nil
	}
	doc, err := x.VFS.Route(ctx, vpath).ReadText(ctx)
	if errors.Is(err, vfs.ErrTooLarge) || errors.Is(err, vfs.ErrInvalidUTF8) || errors.Is(err, vfs.ErrNotSupported) {
		return nil, nil
	}
	return doc, err
}

// streamParts maps streamed chunks to Chunk parts. Blank chunks are dropped.
func streamParts(src iter.Seq2[chunk, error], kind string) iter.Seq2[brain.Object, error] {
	return func(yield func(brain.Object, error) bool) {
		for ch, err := range src {
			if err != nil {
				yield(brain.Object{}, err)
				return
			}
			if strings.TrimSpace(ch.Text) == "" {
				continue
			}
			if !yield(chunkPart(kind, "", "", ch), nil) {
				return
			}
		}
	}
}

// blockParts yields one Chunk per block of doc; a block over sizes.Max is
// split into content-defined chunks. A document without blocks is chunked
// like a plain-text file.
func blockParts(ctx context.Context, base, kind string, doc vfs.Textual, sizes ChunkSizes) iter.Seq2[brain.Object, error] {
	body := doc.Text()
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	if len(blocks) == 0 {
		return streamParts(chunks(ctx, strings.NewReader(body), sizes, 0), kind)
	}
	projected := vfs.IsProjected(doc.MediaType())
	var starts []int
	if !projected {
		starts = lineStarts(body)
	}
	return func(yield func(brain.Object, error) bool) {
		for _, b := range blocks {
			start, end := b.Style.Span.StartLine, b.Style.Span.EndLine
			seg, offset := b.Text, int64(0)
			if !projected {
				// Clamp the span to the body, so a malformed block reads as empty.
				start = min(max(start, 1), len(starts))
				end = min(max(end, start), len(starts)+1)
				seg, offset = lineSpan(body, starts, start, end), int64(starts[start-1])
			}
			title := base + "#" + b.ID
			if len(seg) <= sizes.Max {
				ch := chunk{Text: seg, StartLine: start, EndLine: end, ByteStart: offset, ByteEnd: offset + int64(len(seg))}
				if !yield(chunkPart(kind, title, b.ID, ch), nil) {
					return
				}
				continue
			}
			for ch, err := range chunks(ctx, strings.NewReader(seg), sizes, 0) {
				if err != nil {
					yield(brain.Object{}, err)
					return
				}
				ch.StartLine, ch.EndLine = ch.StartLine+start-1, ch.EndLine+start-1
				ch.ByteStart, ch.ByteEnd = ch.ByteStart+offset, ch.ByteEnd+offset
				if !yield(chunkPart(kind, title, b.ID, ch), nil) {
					return
				}
			}
		}
	}
}

// chunkPart is the Chunk object for ch. Line and byte ranges live in
// properties, not the title, so moving a chunk never changes its embedding.
func chunkPart(kind, title, blockID string, ch chunk) brain.Object {
	props := map[string]any{
		PropStartLine: float64(ch.StartLine),
		PropEndLine:   float64(ch.EndLine),
		PropByteStart: float64(ch.ByteStart),
		PropByteEnd:   float64(ch.ByteEnd),
	}
	if blockID != "" {
		props[PropBlockID] = blockID
		props[PropHeadingPath] = blockID
	}
	return brain.Object{Kind: kind, Title: title, Content: ch.Text, Properties: props}
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
	if err := x.Brain.SoftDelete(ctx, x.Scope, x.DocumentID(vpath)); err != nil && !errors.Is(err, brain.ErrNotFound) {
		return err
	}
	return nil
}

func walk(ctx context.Context, ms *vfs.MountSession, vpath string, fn func(vpath string, isDir bool) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Root only: one Stat. Descendants reuse DirEntry.IsDir (no re-Stat per dir).

	st, err := ms.Route(ctx, vpath).
		Stat(ctx)
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

	ents, err := ms.Route(ctx, vpath).
		ReadDir(ctx)
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
