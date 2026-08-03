package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemoryStore is an in-process Store. Put/PutKind are seed helpers, not the product write API.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[uuid.UUID]Object
	kinds   map[string]ObjectKind
}

// NewMemoryStore returns an empty memory-backed store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[uuid.UUID]Object),
		kinds:   make(map[string]ObjectKind),
	}
}

// Put upserts an object. Soft-deleted rows may be stored; Get hides them.
func (s *MemoryStore) Put(obj Object) error {
	if obj.ID == uuid.Nil {
		return fmt.Errorf("brain: object id is required")
	}
	if obj.Kind == "" {
		return fmt.Errorf("brain: object kind is required")
	}
	if obj.NamespaceID == uuid.Nil {
		return fmt.Errorf("brain: object namespace_id is required")
	}
	if obj.Properties == nil {
		obj.Properties = map[string]any{}
	}
	now := time.Now().UTC()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	if obj.UpdatedAt.IsZero() {
		obj.UpdatedAt = now
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[obj.ID] = obj
	return nil
}

// PutKind upserts a kind registry row.
func (s *MemoryStore) PutKind(k ObjectKind) error {
	if k.Kind == "" {
		return fmt.Errorf("brain: kind is required")
	}
	if len(k.FilterableFields) == 0 {
		k.FilterableFields = json.RawMessage("[]")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kinds[k.Kind] = k
	return nil
}

// Get implements ObjectReader.
func (s *MemoryStore) Get(_ context.Context, scope Scope, id uuid.UUID) (Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[id]
	if !ok || obj.DeletedAt != nil {
		return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if scope.Namespace != nil && obj.NamespaceID != *scope.Namespace {
		return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return cloneObject(obj), nil
}

// ListChildren implements ObjectReader.
func (s *MemoryStore) ListChildren(_ context.Context, scope Scope, parentID uuid.UUID) ([]Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Object
	for _, obj := range s.objects {
		if obj.DeletedAt != nil {
			continue
		}
		if obj.ParentID == nil || *obj.ParentID != parentID {
			continue
		}
		if scope.Namespace != nil && obj.NamespaceID != *scope.Namespace {
			continue
		}
		out = append(out, cloneObject(obj))
	}
	slices.SortFunc(out, func(a, b Object) int {
		pa, pb := 0, 0
		if a.Position != nil {
			pa = *a.Position
		}
		if b.Position != nil {
			pb = *b.Position
		}
		if pa < pb {
			return -1
		}
		if pa > pb {
			return 1
		}
		if a.ID.String() < b.ID.String() {
			return -1
		}
		if a.ID.String() > b.ID.String() {
			return 1
		}
		return 0
	})
	return out, nil
}

// GetKind implements KindReader.
func (s *MemoryStore) GetKind(_ context.Context, kind string) (ObjectKind, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k, ok := s.kinds[kind]
	if !ok {
		return ObjectKind{}, fmt.Errorf("%w: kind %q", ErrNotFound, kind)
	}
	return k, nil
}

// ListKinds implements KindReader.
func (s *MemoryStore) ListKinds(_ context.Context) ([]ObjectKind, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ObjectKind, 0, len(s.kinds))
	for _, k := range s.kinds {
		out = append(out, k)
	}
	slices.SortFunc(out, func(a, b ObjectKind) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return 0
	})
	return out, nil
}

func cloneObject(o Object) Object {
	cp := o
	if o.Properties != nil {
		cp.Properties = make(map[string]any, len(o.Properties))
		for k, v := range o.Properties {
			cp.Properties[k] = v
		}
	}
	if o.ParentID != nil {
		p := *o.ParentID
		cp.ParentID = &p
	}
	if o.Position != nil {
		p := *o.Position
		cp.Position = &p
	}
	if o.DeletedAt != nil {
		d := *o.DeletedAt
		cp.DeletedAt = &d
	}
	return cp
}
