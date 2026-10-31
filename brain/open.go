package brain

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Ref names an object by id, or by the virtual path of an indexed file
// (its PropVFSPath property). Path wins when both are set.
type Ref struct {
	ID   uuid.UUID
	Path string
}

// Opened is an object read in full: its content, its kind's fields (which
// ones a search can filter on), and its first children.
type Opened struct {
	Object RichObject      `json:"object"`
	Kind   *ObjectKindInfo `json:"kind,omitempty"`
	// Children are the first openChildren parts; MoreChildren reports more
	// (Related lists them all).
	Children     []RichObject `json:"children,omitempty"`
	MoreChildren bool         `json:"more_children,omitempty"`
}

// openChildren is how many children Open lists.
const openChildren = 10

// Resolve returns the id ref names under scope.
func (e *Engine) Resolve(ctx context.Context, scope Scope, ref Ref) (uuid.UUID, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return uuid.Nil, err
	}
	if p := strings.TrimSpace(ref.Path); p != "" {
		obj, err := e.store.GetByProperty(ctx, scope, PropVFSPath, p)
		if errors.Is(err, ErrNotFound) {
			return uuid.Nil, fmt.Errorf("%w: path %s is not indexed (index_file it first)", ErrNotFound, p)
		}
		if err != nil {
			return uuid.Nil, err
		}
		return obj.ID, nil
	}
	if ref.ID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: an id or a path is required", ErrInvalid)
	}
	return ref.ID, nil
}

// Open reads the object id names in full under scope, with its first
// children, in one store read. The store returns a saved session message
// when Scope.SessionID is set and id is one. Kind fields come from the
// catalog; in open mode, from the store's kind registry, which the engine
// reads once (kinds persisted later show after a restart).
func (e *Engine) Open(ctx context.Context, scope Scope, id uuid.UUID) (Opened, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Opened{}, err
	}
	if id == uuid.Nil {
		return Opened{}, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	kinds, err := e.kinds(ctx)
	if err != nil {
		return Opened{}, err
	}
	tree, err := e.store.GetTree(ctx, scope, id, openChildren+1)
	if err != nil {
		return Opened{}, err
	}
	out := Opened{Object: tree.Object.Rich(true), Kind: kinds(tree.Object.Kind)}
	if !tree.Object.IsPart() {
		out.MoreChildren = len(tree.Parts) > openChildren
		for _, k := range tree.Parts[:min(len(tree.Parts), openChildren)] {
			out.Children = append(out.Children, k.Rich(false))
		}
	}
	ids := []uuid.UUID{id}
	for _, c := range out.Children {
		ids = append(ids, c.ID)
	}
	if err := e.recordRead(ctx, scope, ids); err != nil {
		return Opened{}, err
	}
	return out, nil
}

// kinds returns how Open documents a kind: from the catalog when kinds are
// registered, else from the store's kind registry, read on first use. A
// kind neither holds has none.
func (e *Engine) kinds(ctx context.Context) (func(kind string) *ObjectKindInfo, error) {
	if !e.catalog.Empty() {
		return func(kind string) *ObjectKindInfo {
			spec, ok := e.catalog.Get(kind)
			if !ok {
				return nil
			}
			return new(spec.Info())
		}, nil
	}
	e.registry.Lock()
	defer e.registry.Unlock()
	if e.registry.kinds == nil {
		rows, err := e.store.ListKinds(ctx)
		if err != nil {
			return nil, fmt.Errorf("brain: kind registry: %w", err)
		}
		e.registry.kinds = make(map[string]ObjectKind, len(rows))
		for _, k := range rows {
			e.registry.kinds[k.Kind] = k
		}
	}
	registry := e.registry.kinds
	return func(kind string) *ObjectKindInfo {
		k, ok := registry[kind]
		if !ok {
			return nil
		}
		return new(k.info())
	}, nil
}

// Get returns the stored object id names under scope, as the store holds it
// (with Readers and properties), for hosts. The agent tools use Open.
func (e *Engine) Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Object{}, err
	}
	if id == uuid.Nil {
		return Object{}, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return Object{}, err
	}
	return obj, e.recordRead(ctx, scope, []uuid.UUID{id})
}
