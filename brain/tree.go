package brain

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// PutTree upserts a first-class parent and makes parts its only parts, in order.
//
// Part ids come from content: a part whose kind, content type, title,
// summary, content and index text match a live part has that part's id, so
// the store keeps its row and index and indexes only new parts. Engine
// prepares each part as the store reads it (ParentID, the parent's namespace
// and readers, Position from its index unless set, times, validation), so
// memory never holds every part. The store commits the tree atomically:
// until then reads and searches see the previous parts, and a failure
// leaves them in place.
//
// Part ids must be nil. When another PutTree of the same parent commits
// between this one's read and commit, PutTree returns ErrConflict and
// changes nothing.
func (e *Engine) PutTree(ctx context.Context, scope Scope, parent Object, parts iter.Seq2[Object, error]) (Object, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Object{}, err
	}
	if parent.ParentID != nil {
		return Object{}, fmt.Errorf("%w: parent must be a first-class object", ErrInvalid)
	}
	in, err := e.preparePut(ctx, scope, parent)
	if err != nil {
		return Object{}, err
	}
	if parts == nil {
		parts = Parts()
	}
	n := 0
	prepared := func(yield func(Indexed, error) bool) {
		seen := make(map[[sha256.Size]byte]int)
		for p, err := range parts {
			if err != nil {
				yield(Indexed{}, err)
				return
			}
			part, err := e.preparePart(in.Object, p, n)
			if err != nil {
				yield(Indexed{}, err)
				return
			}
			n++
			digest := part.partDigest(part.Text)
			part.ID = partID(in.ID, digest, seen[digest])
			seen[digest]++
			if !yield(part, nil) {
				return
			}
		}
	}
	stored, err := e.store.PutTree(ctx, scope, in, prepared)
	if err != nil {
		return Object{}, err
	}
	if err := e.committed(ctx, scope, AuditEvent{Op: AuditPut, ObjectIDs: []uuid.UUID{stored.ID}, Count: n}, e.putIndexes(ctx, stored, in.Text)); err != nil {
		return Object{}, err
	}
	return stored, nil
}

// Parts yields objs as PutTree parts.
func Parts(objs ...Object) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		for _, o := range objs {
			if !yield(o, nil) {
				return
			}
		}
	}
}

// preparePart fills a part's defaults from parent, validates it, and
// shapes its index text.
func (e *Engine) preparePart(parent Object, p Object, i int) (Indexed, error) {
	if p.ID != uuid.Nil {
		return Indexed{}, fmt.Errorf("%w: part ids come from content; leave ID empty", ErrInvalid)
	}
	if p.DeletedAt != nil {
		return Indexed{}, fmt.Errorf("%w: put refuses soft-deleted objects; use SoftDelete", ErrInvalid)
	}
	if !p.Namespace.Empty() && !p.Namespace.Equal(parent.Namespace) {
		return Indexed{}, fmt.Errorf("%w: part namespace %s is not the parent's %s", ErrInvalid, p.Namespace, parent.Namespace)
	}
	now := e.cfg.Now()
	p.ParentID = &parent.ID
	p.Namespace = parent.Namespace.Clone()
	p.Readers = slices.Clone(parent.Readers)
	if p.Position == nil {
		p.Position = new(i)
	}
	if p.Properties == nil {
		p.Properties = map[string]any{}
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	p.UpdatedAt = now
	p.Kind = strings.TrimSpace(p.Kind)
	p.Embedding, p.EmbeddingModel = nil, ""
	if err := p.Validate(e.catalog); err != nil {
		return Indexed{}, err
	}
	return Indexed{Object: p, Text: e.indexText(p, &parent)}, nil
}

// partDigest hashes everything a part's row and index depend on, except
// its place in the tree (position and properties change in place).
func (p Object) partDigest(text string) [sha256.Size]byte {
	h := sha256.New()
	for _, s := range []string{p.Kind, p.ContentType, p.Title, p.Summary, p.Content, text} {
		_ = binary.Write(h, binary.BigEndian, uint64(len(s)))
		h.Write([]byte(s))
	}
	return [sha256.Size]byte(h.Sum(nil))
}

// partID is the id of the n-th part under parent with digest (n counts
// earlier parts with the same digest, so repeated text keeps unique ids).
func partID(parent uuid.UUID, digest [sha256.Size]byte, n int) uuid.UUID {
	return uuid.NewHash(sha256.New(), parent, binary.BigEndian.AppendUint64(digest[:], uint64(n)), 8) //nolint:gosec // G115: n counts parts, never negative
}
