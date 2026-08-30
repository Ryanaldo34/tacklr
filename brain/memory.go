package brain

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var (
	_ Store        = (*MemoryStore)(nil)
	_ ObjectWriter = (*MemoryStore)(nil)
	_ ObjectLister = (*MemoryStore)(nil)
	_ KindWriter   = (*MemoryStore)(nil)
)

// MemoryStore is an in-process Store (tests, fixtures, and ObjectWriter for Engine.Put).
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

// Put implements ObjectWriter. Soft-deleted rows may be stored; Get hides them.
// Clones maps/slices so callers cannot mutate the store through shared references.
func (s *MemoryStore) Put(_ context.Context, obj Object) error {
	if err := ValidateObjectIdentity(obj); err != nil {
		return err
	}
	obj = cloneObject(obj)
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
	s.objects[obj.ID] = obj
	s.mu.Unlock()
	return nil
}

// SoftDelete implements ObjectWriter.
func (s *MemoryStore) SoftDelete(_ context.Context, scope Scope, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[id]
	if !ok || obj.DeletedAt != nil {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	now := time.Now().UTC()
	obj.DeletedAt = &now
	obj.UpdatedAt = now
	s.objects[id] = obj
	return nil
}

// PutKind implements KindWriter.
func (s *MemoryStore) PutKind(_ context.Context, k ObjectKind) error {
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
	return s.getLocked(scope, id)
}

// GetMany implements ObjectReader.
func (s *MemoryStore) GetMany(_ context.Context, scope Scope, ids []uuid.UUID) ([]Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Object, 0, len(ids))
	for _, id := range ids {
		obj, err := s.getLocked(scope, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, obj)
	}
	return out, nil
}

func (s *MemoryStore) getLocked(scope Scope, id uuid.UUID) (Object, error) {
	obj, ok := s.objects[id]
	if !ok || obj.DeletedAt != nil {
		return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return cloneObject(obj), nil
}

// ListByKind implements ObjectLister (first-class objects only).
func (s *MemoryStore) ListByKind(_ context.Context, scope Scope, kind string, limit int) ([]Object, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" || limit <= 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var matches []Object
	for _, obj := range s.objects {
		if obj.DeletedAt != nil || obj.ParentID != nil {
			continue
		}
		if !scope.Namespace.Covers(obj.Namespace) {
			continue
		}
		if obj.Kind != kind {
			continue
		}
		matches = append(matches, obj)
	}
	slices.SortFunc(matches, func(a, b Object) int {
		if c := strings.Compare(a.Title, b.Title); c != 0 {
			return c
		}
		return cmpUUID(a.ID, b.ID)
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	out := make([]Object, len(matches))
	for i, obj := range matches {
		out[i] = cloneObject(obj)
	}
	return out, nil
}

// GetByProperty implements ObjectLister.
func (s *MemoryStore) GetByProperty(_ context.Context, scope Scope, key, value string) (Object, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Object{}, fmt.Errorf("brain: property key is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, obj := range s.objects {
		if obj.DeletedAt != nil {
			continue
		}
		if !scope.Namespace.Covers(obj.Namespace) {
			continue
		}
		if obj.Properties == nil {
			continue
		}
		got, ok := obj.Properties[key]
		if !ok {
			continue
		}
		sval, ok := got.(string)
		if !ok || sval != value {
			continue
		}
		return cloneObject(obj), nil
	}
	return Object{}, fmt.Errorf("%w: %s=%s", ErrNotFound, key, value)
}

// KindsWithObjects implements ObjectLister.
func (s *MemoryStore) KindsWithObjects(_ context.Context, scope Scope) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := map[string]struct{}{}
	for _, obj := range s.objects {
		if obj.DeletedAt != nil || obj.ParentID != nil {
			continue
		}
		if !scope.Namespace.Covers(obj.Namespace) {
			continue
		}
		if obj.Kind == "" {
			continue
		}
		seen[obj.Kind] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen)), nil
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
		if !scope.Namespace.Covers(obj.Namespace) {
			continue
		}
		out = append(out, cloneObject(obj))
	}
	slices.SortFunc(out, func(a, b Object) int {
		if c := cmp.Compare(positionOf(a), positionOf(b)); c != 0 {
			return c
		}
		return cmpUUID(a.ID, b.ID)
	})
	return out, nil
}

func positionOf(o Object) int {
	if o.Position == nil {
		return 0
	}
	return *o.Position
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
		return strings.Compare(a.Kind, b.Kind)
	})
	return out, nil
}

// SearchLexical implements PartSearcher with a deterministic TF×IDF-style score.
// Only parts (parent_id set) are candidates.
func (s *MemoryStore) SearchLexical(_ context.Context, scope Scope, query string, filters Filter, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	qTokens := tokenize(query)
	if len(qTokens) == 0 || k <= 0 {
		return nil, nil
	}
	parts, err := s.candidateParts(scope, filters)
	if err != nil {
		return nil, err
	}
	type doc struct {
		obj    Object
		tokens map[string]int
		len    int
	}
	df := make(map[string]int)
	docs := make([]doc, 0, len(parts))
	for _, obj := range parts {
		toks := tokenize(searchText(obj))
		if len(toks) == 0 {
			continue
		}
		tf := make(map[string]int, len(toks))
		for _, t := range toks {
			tf[t]++
		}
		for t := range tf {
			df[t]++
		}
		docs = append(docs, doc{obj: obj, tokens: tf, len: len(toks)})
	}
	n := float64(len(docs))
	if n == 0 {
		return nil, nil
	}
	scored := make([]ScoredID, 0, len(docs))
	for _, d := range docs {
		var score float64
		for _, qt := range qTokens {
			tf := float64(d.tokens[qt])
			if tf == 0 {
				continue
			}
			idf := math.Log(1 + n/float64(1+df[qt]))
			score += (tf / float64(d.len)) * idf
		}
		if score <= 0 {
			continue
		}
		scored = append(scored, scoredFromObject(d.obj, score))
	}
	return topKScored(scored, k), nil
}

// SearchVector implements PartSearcher over part embeddings only.
func (s *MemoryStore) SearchVector(_ context.Context, scope Scope, embedding []float32, filters Filter, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(embedding) == 0 || k <= 0 {
		return nil, nil
	}
	parts, err := s.candidateParts(scope, filters)
	if err != nil {
		return nil, err
	}
	scored := make([]ScoredID, 0, len(parts))
	for _, obj := range parts {
		if len(obj.Embedding) != len(embedding) {
			continue
		}
		sim := cosine(embedding, obj.Embedding)
		if sim <= 0 {
			continue
		}
		scored = append(scored, scoredFromObject(obj, float64(sim)))
	}
	return topKScored(scored, k), nil
}

// SearchTrigram implements PartSearcher with case-fold substring / trigram overlap.
func (s *MemoryStore) SearchTrigram(_ context.Context, scope Scope, query string, filters Filter, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" || k <= 0 {
		return nil, nil
	}
	parts, err := s.candidateParts(scope, filters)
	if err != nil {
		return nil, err
	}
	qTri := trigrams(q)
	scored := make([]ScoredID, 0, len(parts))
	for _, obj := range parts {
		text := strings.ToLower(searchText(obj))
		if text == "" {
			continue
		}
		var score float64
		if strings.Contains(text, q) {
			score = 1.0
		} else if len(qTri) > 0 {
			score = trigramOverlap(qTri, trigrams(text))
		}
		if score <= 0 {
			continue
		}
		scored = append(scored, scoredFromObject(obj, score))
	}
	return topKScored(scored, k), nil
}

// candidateParts returns live parts (parent_id set) under the caller's lock.
// Do not retain the slice across unlock.
func (s *MemoryStore) candidateParts(scope Scope, filters Filter) ([]Object, error) {
	plan, err := compileFilters(filters)
	if err != nil {
		return nil, err
	}
	out := make([]Object, 0)
	for _, obj := range s.objects {
		if obj.DeletedAt != nil || obj.ParentID == nil {
			continue
		}
		if !scope.Namespace.Covers(obj.Namespace) {
			continue
		}
		if !plan.match(obj) {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}

func scoredFromObject(obj Object, score float64) ScoredID {
	var props map[string]any
	if obj.Properties != nil {
		props = maps.Clone(obj.Properties)
	}
	return ScoredID{
		ID:         obj.ID,
		Score:      score,
		UpdatedAt:  obj.UpdatedAt,
		ParentID:   obj.ParentID,
		Title:      obj.Title,
		Content:    obj.Content,
		Position:   obj.Position,
		Properties: props,
	}
}

func searchText(o Object) string {
	return strings.TrimSpace(o.Title + " " + o.Summary + " " + o.Content)
}

func topKScored(in []ScoredID, k int) []ScoredID {
	sortScored(in)
	if len(in) > k {
		in = in[:k]
	}
	return in
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}

func trigrams(s string) map[string]struct{} {
	s = strings.ToLower(s)
	out := map[string]struct{}{}
	if len(s) < 3 {
		if s != "" {
			out[s] = struct{}{}
		}
		return out
	}
	for i := 0; i+3 <= len(s); i++ {
		out[s[i:i+3]] = struct{}{}
	}
	return out
}

func trigramOverlap(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	var inter int
	for t := range a {
		if _, ok := b[t]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func cloneObject(o Object) Object {
	cp := o
	if o.Properties != nil {
		cp.Properties = maps.Clone(o.Properties)
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
	if o.Embedding != nil {
		cp.Embedding = slices.Clone(o.Embedding)
	}
	cp.Namespace = o.Namespace.Clone()
	return cp
}
