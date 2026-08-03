package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

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

// SearchLexical implements PartSearcher with a deterministic TF×IDF-style score.
// Only content-bearing parts (parent_id set) are candidates.
func (s *MemoryStore) SearchLexical(_ context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	qTokens := tokenize(query)
	if len(qTokens) == 0 || k <= 0 {
		return nil, nil
	}
	parts := s.candidateParts(scope, filters)
	// Document frequency over candidate parts.
	df := map[string]int{}
	docs := make([]struct {
		obj    Object
		tokens map[string]int
	}, 0, len(parts))
	for _, obj := range parts {
		toks := tokenize(searchText(obj))
		tf := map[string]int{}
		for _, t := range toks {
			tf[t]++
		}
		seen := map[string]struct{}{}
		for t := range tf {
			if _, ok := seen[t]; !ok {
				df[t]++
				seen[t] = struct{}{}
			}
		}
		docs = append(docs, struct {
			obj    Object
			tokens map[string]int
		}{obj: obj, tokens: tf})
	}
	n := float64(len(docs))
	if n == 0 {
		return nil, nil
	}
	var scored []ScoredID
	for _, d := range docs {
		var score float64
		dl := 0
		for _, c := range d.tokens {
			dl += c
		}
		if dl == 0 {
			continue
		}
		for _, qt := range qTokens {
			tf := float64(d.tokens[qt])
			if tf == 0 {
				continue
			}
			// Mild length norm + IDF.
			idf := math.Log(1 + n/float64(1+df[qt]))
			score += (tf / float64(dl)) * idf
		}
		if score <= 0 {
			continue
		}
		scored = append(scored, scoredFromObject(d.obj, score))
	}
	return topKScored(scored, k), nil
}

// SearchVector implements PartSearcher via cosine similarity on Object.Embedding.
func (s *MemoryStore) SearchVector(_ context.Context, scope Scope, embedding []float32, filters Filters, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(embedding) == 0 || k <= 0 {
		return nil, nil
	}
	var scored []ScoredID
	for _, obj := range s.candidateParts(scope, filters) {
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
func (s *MemoryStore) SearchTrigram(_ context.Context, scope Scope, query string, filters Filters, k int) ([]ScoredID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" || k <= 0 {
		return nil, nil
	}
	qTri := trigrams(q)
	var scored []ScoredID
	for _, obj := range s.candidateParts(scope, filters) {
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

func (s *MemoryStore) candidateParts(scope Scope, filters Filters) []Object {
	var out []Object
	for _, obj := range s.objects {
		if obj.DeletedAt != nil || obj.ParentID == nil {
			continue
		}
		if scope.Namespace != nil && obj.NamespaceID != *scope.Namespace {
			continue
		}
		if !objectMatchesFilters(obj, filters) {
			continue
		}
		out = append(out, obj)
	}
	return out
}

func scoredFromObject(obj Object, score float64) ScoredID {
	return ScoredID{
		ID:        obj.ID,
		Score:     score,
		UpdatedAt: obj.UpdatedAt,
		ParentID:  obj.ParentID,
		Title:     obj.Title,
		Content:   obj.Content,
		Position:  obj.Position,
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
	s = strings.ToLower(s)
	var b strings.Builder
	var out []string
	flush := func() {
		if b.Len() == 0 {
			return
		}
		out = append(out, b.String())
		b.Reset()
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return out
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
	// Jaccard-ish
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
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
	if o.Embedding != nil {
		cp.Embedding = append([]float32(nil), o.Embedding...)
	}
	return cp
}
