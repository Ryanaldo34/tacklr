package brain

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// PutTree upserts a first-class parent and makes parts its only parts, in order.
//
// Part ids come from content: a part whose embedding input (kind, title,
// summary, content, index text, embedding model and parent) matches a live
// part keeps that row and its vector, so only new parts are embedded and
// written. New parts are embedded in batches of EngineConfig.EmbedBatchSize,
// at most EmbedConcurrency batches at a time, and staged as each batch
// finishes. Memory therefore holds a few batches plus the live part ids,
// whatever the number of parts. CommitTree then swaps the parts in
// atomically: until it runs, reads and searches see the previous parts, and
// a failure leaves them in place.
//
// Each part gets ParentID, the parent's namespace and readers, and Position
// (its index in parts unless set). Part ids must be nil. When another
// PutTree of the same parent commits between this one's read and commit,
// PutTree returns ErrConflict and changes nothing.
func (e *Engine) PutTree(ctx context.Context, scope Scope, parent Object, parts iter.Seq2[Object, error]) (Object, error) {
	w, err := capability[ObjectWriter](ctx, e, scope.Namespace, "object writes")
	if err != nil {
		return Object{}, err
	}
	if parent.ParentID != nil {
		return Object{}, fmt.Errorf("%w: parent must be a first-class object", ErrInvalid)
	}
	parent, text, err := e.preparePut(ctx, scope, parent, nil)
	if err != nil {
		return Object{}, err
	}
	state, err := w.TreeState(ctx, scope, parent.ID)
	if err != nil {
		return Object{}, err
	}
	run := uuid.New()
	n, err := e.stageTree(ctx, scope, w, run, parent, state.Live, parts)
	if err != nil {
		return Object{}, err
	}
	if err := w.CommitTree(ctx, scope, run, state.Rev, parent); err != nil {
		return Object{}, err
	}
	if err := e.ensureGraph(ctx, parent, text); err != nil {
		return Object{}, err
	}
	return parent, e.record(ctx, scope, AuditEvent{Op: AuditPut, ObjectIDs: []uuid.UUID{parent.ID}, Count: n})
}

// ValidateTreePart checks a part before a store stages it: a reused part
// needs its id and parent; a new part is a full object.
func ValidateTreePart(p TreePart) error {
	if p.Object.ParentID == nil {
		return fmt.Errorf("%w: tree part %s has no parent", ErrInvalid, p.Object.ID)
	}
	if !p.Reused {
		return ValidateObjectIdentity(p.Object)
	}
	if p.Object.ID == uuid.Nil {
		return fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	return p.Object.Namespace.Validate()
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

// stageTree streams parts into run in batches: a live part as a reference,
// a new part with its embedding. It returns how many parts it staged. The
// first error cancels the batches in flight and is returned once they stop.
func (e *Engine) stageTree(ctx context.Context, scope Scope, w ObjectWriter, run uuid.UUID, parent Object, live map[uuid.UUID]struct{}, parts iter.Seq2[Object, error]) (int, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make(chan struct{}, e.cfg.EmbedConcurrency)
	var wg sync.WaitGroup
	flush := func(batch []TreePart, texts []string) {
		// A full set of slots blocks the reader: backpressure. A cancelled
		// batch still returns its slot, so this never waits forever.
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			if err := e.stageBatch(ctx, scope, w, run, batch, texts); err != nil {
				cancel(err)
			}
		})
	}

	model := ""
	if e.embedder != nil {
		model = e.embedder.Model()
	}
	if parts == nil {
		parts = Parts()
	}
	seen := make(map[[sha256.Size]byte]int)
	var batch []TreePart
	var texts []string
	i := 0
	for p, err := range parts {
		if err != nil {
			cancel(err)
			break
		}
		if ctx.Err() != nil {
			break
		}
		part, text, err := e.preparePart(parent, p, i)
		if err != nil {
			cancel(err)
			break
		}
		i++
		digest := partDigest(model, part, text)
		part.ID = partID(parent.ID, digest, seen[digest])
		seen[digest]++
		if _, ok := live[part.ID]; ok {
			batch = append(batch, TreePart{Reused: true, Object: Object{
				ID: part.ID, ParentID: part.ParentID, Namespace: part.Namespace,
				Position: part.Position, Properties: part.Properties,
			}})
			texts = append(texts, "")
		} else {
			batch = append(batch, TreePart{Object: part})
			texts = append(texts, text)
		}
		if len(batch) == e.cfg.EmbedBatchSize {
			flush(batch, texts)
			batch, texts = nil, nil
		}
	}
	if len(batch) > 0 && ctx.Err() == nil {
		flush(batch, texts)
	}
	wg.Wait()
	return i, context.Cause(ctx)
}

// stageBatch embeds the batch's new parts in one Embed call and stages the
// batch. texts[i] is part i's index text; empty means no embedding.
func (e *Engine) stageBatch(ctx context.Context, scope Scope, w ObjectWriter, run uuid.UUID, batch []TreePart, texts []string) error {
	if e.embedder != nil {
		var idx []int
		var in []string
		for i, t := range texts {
			if t != "" {
				idx = append(idx, i)
				in = append(in, t)
			}
		}
		if len(in) > 0 {
			vecs, err := e.embed(ctx, in)
			if err != nil {
				return fmt.Errorf("brain: embed parts: %w", err)
			}
			model := e.embedder.Model()
			for j, i := range idx {
				batch[i].Object.Embedding = vecs[j]
				batch[i].Object.EmbeddingModel = model
			}
		}
	}
	return w.StageParts(ctx, scope, run, batch)
}

// preparePart fills a part's defaults from parent and validates it. It does
// not embed: stageBatch embeds only parts that are new.
func (e *Engine) preparePart(parent Object, p Object, i int) (Object, string, error) {
	if p.ID != uuid.Nil {
		return Object{}, "", fmt.Errorf("%w: part ids come from content; leave ID empty", ErrInvalid)
	}
	if p.DeletedAt != nil {
		return Object{}, "", fmt.Errorf("%w: put refuses soft-deleted objects; use SoftDelete", ErrInvalid)
	}
	if !p.Namespace.Empty() && !p.Namespace.Equal(parent.Namespace) {
		return Object{}, "", fmt.Errorf("%w: part namespace %s is not the parent's %s", ErrInvalid, p.Namespace, parent.Namespace)
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
	if err := ValidateObject(p, e.catalog); err != nil {
		return Object{}, "", err
	}
	return p, e.indexText(p, &parent), nil
}

// partDigest hashes everything a part's row and vector depend on, except
// its place in the tree (position and properties change in place).
func partDigest(model string, p Object, text string) [sha256.Size]byte {
	h := sha256.New()
	for _, s := range []string{model, p.Kind, p.ContentType, p.Title, p.Summary, p.Content, text} {
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
