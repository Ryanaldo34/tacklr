package brain

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var _ Store = (*MemoryStore)(nil)

// MemoryStore is an in-process Store for tests, fixtures and offline hosts.
// It embeds with its own Embedding (none means lexical search only) and
// fuses its channels with its Ranking.
type MemoryStore struct {
	embedding   Embedding
	ranking     Ranking
	mu          sync.RWMutex
	objects     map[uuid.UUID]Object
	kinds       map[string]ObjectKind
	sessionMsgs map[uuid.UUID]sessionRow
	sessionKeys map[string]map[string]int
	treeRevs    map[uuid.UUID]int64
}

// MemoryOption configures NewMemoryStore.
type MemoryOption func(*MemoryStore)

// WithMemoryEmbedding sets how the store embeds objects, parts, session
// messages and queries.
func WithMemoryEmbedding(e Embedding) MemoryOption {
	return func(s *MemoryStore) { s.embedding = e }
}

// WithMemoryRanking sets how the store fuses its search channels.
func WithMemoryRanking(r Ranking) MemoryOption {
	return func(s *MemoryStore) { s.ranking = r }
}

// NewMemoryStore returns an empty memory-backed store.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	s := &MemoryStore{
		objects:     make(map[uuid.UUID]Object),
		kinds:       make(map[string]ObjectKind),
		sessionMsgs: make(map[uuid.UUID]sessionRow),
		sessionKeys: make(map[string]map[string]int),
		treeRevs:    make(map[uuid.UUID]int64),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Put implements Store. It embeds obj.Text before it takes the lock, and
// clones maps and slices so callers cannot change the store through them.
func (s *MemoryStore) Put(ctx context.Context, scope Scope, in Indexed) (Object, error) {
	obj, err := s.indexed(ctx, in)
	if err != nil {
		return Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkPutLocked(scope, obj); err != nil {
		return Object{}, err
	}
	return s.storeLocked(obj), nil
}

// indexed returns in's object with the embedding of its text.
func (s *MemoryStore) indexed(ctx context.Context, in Indexed) (Object, error) {
	obj := in.Object
	vec, err := s.embedding.EmbedOne(ctx, in.Text)
	if err != nil {
		return Object{}, fmt.Errorf("brain: embed object: %w", err)
	}
	obj.Embedding, obj.EmbeddingModel = vec, ""
	if vec != nil {
		obj.EmbeddingModel = s.embedding.Model()
	}
	return obj, nil
}

// PutTree implements Store: it reads the tree's revision and reusable parts,
// embeds only new parts in batches, then commits under the lock, failing
// with ErrConflict when another tree write committed first.
func (s *MemoryStore) PutTree(ctx context.Context, scope Scope, parent Indexed, parts iter.Seq2[Indexed, error]) (Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return Object{}, err
	}
	p, err := s.indexed(ctx, parent)
	if err != nil {
		return Object{}, err
	}
	model := s.embedding.Model()
	s.mu.RLock()
	rev := s.treeRevs[p.ID]
	reusable := map[uuid.UUID]struct{}{}
	for id, obj := range s.objects {
		if obj.ParentID != nil && *obj.ParentID == p.ID && obj.DeletedAt == nil && scope.CanRead(obj) && obj.EmbeddingModel == model {
			reusable[id] = struct{}{}
		}
	}
	s.mu.RUnlock()
	var staged []TreePart
	var mu sync.Mutex
	_, err = s.embedding.EmbedBatches(ctx, parts, func(id uuid.UUID) bool {
		_, ok := reusable[id]
		return ok
	}, func(_ context.Context, batch []TreePart) error {
		mu.Lock()
		defer mu.Unlock()
		staged = append(staged, batch...)
		return nil
	})
	if err != nil {
		return Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkPutLocked(scope, p); err != nil {
		return Object{}, err
	}
	if s.treeRevs[p.ID] != rev {
		return Object{}, fmt.Errorf("%w: tree %s changed since revision %d", ErrConflict, p.ID, rev)
	}
	keep := make(map[uuid.UUID]struct{}, len(staged))
	for _, part := range staged {
		if !scope.Namespace.Covers(part.Object.Namespace) {
			return Object{}, fmt.Errorf("%w: part namespace is outside scope", ErrInvalid)
		}
		if _, dup := keep[part.Object.ID]; dup {
			return Object{}, fmt.Errorf("%w: part %s appears twice", ErrInvalid, part.Object.ID)
		}
		keep[part.Object.ID] = struct{}{}
		if old, ok := s.objects[part.Object.ID]; part.Reused && (!ok || old.ParentID == nil || *old.ParentID != p.ID) {
			return Object{}, fmt.Errorf("%w: reused part %s is gone", ErrConflict, part.Object.ID)
		}
	}
	for id, obj := range s.objects {
		if _, ok := keep[id]; !ok && obj.ParentID != nil && *obj.ParentID == p.ID {
			delete(s.objects, id)
		}
	}
	stored := s.storeLocked(p)
	for _, part := range staged {
		obj := part.Object
		if part.Reused {
			old := s.objects[obj.ID]
			old.Position, old.Properties, old.DeletedAt = obj.Position, obj.Properties, nil
			obj = old
		}
		obj.Namespace, obj.Readers = stored.Namespace.Clone(), slices.Clone(stored.Readers)
		s.storeLocked(obj)
	}
	s.treeRevs[p.ID] = rev + 1
	return stored, nil
}

// checkPutLocked enforces the Store write guard: obj lies inside scope, and
// a row that already holds obj.ID is visible under scope.
func (s *MemoryStore) checkPutLocked(scope Scope, obj Object) error {
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	if err := obj.ValidateIdentity(); err != nil {
		return err
	}
	if !scope.Namespace.Covers(obj.Namespace) {
		return fmt.Errorf("%w: object namespace is outside scope", ErrInvalid)
	}
	if old, ok := s.objects[obj.ID]; ok && !scope.CanRead(old) {
		return fmt.Errorf("%w: %s", ErrNotFound, obj.ID)
	}
	return nil
}

// storeLocked writes a copy of an already-checked obj and returns it.
func (s *MemoryStore) storeLocked(obj Object) Object {
	obj = obj.clone()
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
	s.objects[obj.ID] = obj
	return obj.clone()
}

// SoftDelete implements Store.
func (s *MemoryStore) SoftDelete(_ context.Context, scope Scope, id uuid.UUID) error {
	if err := scope.Namespace.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[id]
	if !ok || obj.DeletedAt != nil || !scope.CanRead(obj) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	now := time.Now().UTC()
	for cid, c := range s.objects {
		if cid == id || (c.ParentID != nil && *c.ParentID == id && c.DeletedAt == nil) {
			c.DeletedAt = &now
			c.UpdatedAt = now
			s.objects[cid] = c
		}
	}
	return nil
}

// PurgeDeleted implements Store.
func (s *MemoryStore) PurgeDeleted(_ context.Context, scope Scope, before time.Time) (int, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, obj := range s.objects {
		if obj.DeletedAt != nil && obj.DeletedAt.Before(before) && scope.Namespace.Covers(obj.Namespace) {
			delete(s.objects, id)
			delete(s.treeRevs, id)
			n++
		}
	}
	return n, nil
}

// Erase implements Store.
func (s *MemoryStore) Erase(_ context.Context, scope Scope) ([]uuid.UUID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var parents []uuid.UUID
	for id, obj := range s.objects {
		if !scope.CanRead(obj) {
			continue
		}
		if obj.ParentID == nil {
			parents = append(parents, id)
		}
		delete(s.objects, id)
		delete(s.treeRevs, id)
	}
	for id, row := range s.sessionMsgs {
		if scope.Namespace.Covers(row.namespace) {
			delete(s.sessionMsgs, id)
		}
	}
	return parents, nil
}

// PutKind implements Store.
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

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, scope Scope, id uuid.UUID) (Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return Object{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getLocked(scope, id)
}

// GetMany implements Store.
func (s *MemoryStore) GetMany(_ context.Context, scope Scope, ids []uuid.UUID) ([]Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Object, 0, len(ids))
	for _, id := range ids {
		obj, err := s.getLocked(scope, id)
		if err != nil { // getLocked fails only with ErrNotFound
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}

func (s *MemoryStore) getLocked(scope Scope, id uuid.UUID) (Object, error) {
	if obj, ok := s.objects[id]; ok && obj.DeletedAt == nil && scope.CanRead(obj) {
		return obj.clone(), nil
	}
	if sid := strings.TrimSpace(scope.SessionID); sid != "" && s.sessionMsgs != nil {
		if row, ok := s.sessionMsgs[id]; ok && row.sessionID == sid {
			return row.object(), nil
		}
	}
	return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
}

// GetByProperty implements Store.
func (s *MemoryStore) GetByProperty(_ context.Context, scope Scope, key, value string) (Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return Object{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, obj := range s.objects {
		if obj.DeletedAt != nil {
			continue
		}
		if !scope.CanRead(obj) {
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
		return obj.clone(), nil
	}
	return Object{}, fmt.Errorf("%w: %s=%s", ErrNotFound, key, value)
}

// GetTree implements Store.
func (s *MemoryStore) GetTree(_ context.Context, scope Scope, id uuid.UUID, limit int) (Tree, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return Tree{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, err := s.getLocked(scope, id)
	if err != nil {
		return Tree{}, err
	}
	t := Tree{Object: obj}
	record := obj.ID
	if obj.ParentID != nil {
		record = *obj.ParentID
		if parent, err := s.getLocked(scope, record); err == nil {
			t.Parent = &parent
		}
	}
	if _, saved := s.objects[obj.ID]; saved {
		t.Parts = s.partsLocked(scope, record, limit)
	}
	return t, nil
}

// partsLocked is record's live parts scope can read, by position, at most
// limit (all when limit is not positive).
func (s *MemoryStore) partsLocked(scope Scope, record uuid.UUID, limit int) []Object {
	var out []Object
	for _, obj := range s.objects {
		if obj.DeletedAt == nil && obj.ParentID != nil && *obj.ParentID == record && scope.CanRead(obj) {
			out = append(out, obj.clone())
		}
	}
	slices.SortFunc(out, func(a, b Object) int {
		return cmp.Or(cmp.Compare(a.position(), b.position()), cmpUUID(a.ID, b.ID))
	})
	if limit > 0 {
		out = out[:min(len(out), limit)]
	}
	return out
}

func (o Object) position() int {
	if o.Position == nil {
		return 0
	}
	return *o.Position
}

// ListKinds implements Store.
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

// Search implements Store. It ranks records, parts and the session's messages by a
// TF×IDF lexical score and, unless q.Exact, by cosine similarity to the
// query's embedding (same model only; a failed query embedding leaves the
// lexical list alone); q.Exact adds case-fold substring and
// trigram overlap instead, and puts exact title matches first. The lists
// are fused with the store's Ranking.
func (s *MemoryStore) Search(ctx context.Context, scope Scope, q RecordSearch) ([]ScoredID, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	plan, err := q.Filters.Compile()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(q.Query) == "" {
		return s.listRecords(scope, plan, q.Limit), nil
	}
	var vec []float32
	if !q.Exact {
		// An embedder that fails answers by the words alone.
		if vec, err = s.embedding.EmbedOne(ctx, q.Query); err != nil {
			slog.WarnContext(ctx, "query embed failed; searching text only", "error", err)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	parts := s.candidateRows(scope, plan)
	if q.Within != uuid.Nil {
		record := q.Within
		if w, ok := s.objects[q.Within]; ok && w.ParentID != nil {
			record = *w.ParentID
		}
		parts = slices.DeleteFunc(parts, func(o Object) bool { return o.ParentID == nil || *o.ParentID != record })
	} else {
		parts = s.appendSessionParts(scope, parts)
	}
	lists := [][]ScoredID{lexicalScores(parts, q.Query)}
	if q.Exact {
		lists = append(lists, trigramScores(parts, q.Query))
	} else if vec != nil {
		lists = append(lists, vectorScores(parts, s.embedding.Model(), vec))
	}
	fused := s.ranking.Fuse(lists...)
	if q.Exact {
		fused = titleFirst(fused, q.Query)
	}
	return fused[:min(len(fused), max(q.Limit, 0))], nil
}

// lexicalScores scores parts with a deterministic TF×IDF-style score.
func lexicalScores(parts []Object, query string) []ScoredID {
	qTokens := tokenize(query)
	if len(qTokens) == 0 {
		return nil
	}
	type doc struct {
		obj    Object
		tokens map[string]int
		len    int
	}
	df := make(map[string]int)
	docs := make([]doc, 0, len(parts))
	for _, obj := range parts {
		toks := tokenize(obj.searchText())
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
	var scored []ScoredID
	for _, d := range docs {
		var score float64
		for _, qt := range qTokens {
			if tf := float64(d.tokens[qt]); tf > 0 {
				score += (tf / float64(d.len)) * math.Log(1+n/float64(1+df[qt]))
			}
		}
		if score > 0 {
			scored = append(scored, d.obj.scored(score))
		}
	}
	sortScored(scored)
	return scored
}

// vectorScores scores parts embedded by model by cosine similarity to vec.
func vectorScores(parts []Object, model string, vec []float32) []ScoredID {
	var scored []ScoredID
	for _, obj := range parts {
		if obj.EmbeddingModel != model || len(obj.Embedding) != len(vec) {
			continue
		}
		if sim := cosine(vec, obj.Embedding); sim > 0 {
			scored = append(scored, obj.scored(float64(sim)))
		}
	}
	sortScored(scored)
	return scored
}

// trigramScores scores parts by case-fold substring, else trigram overlap.
func trigramScores(parts []Object, query string) []ScoredID {
	q := strings.ToLower(strings.TrimSpace(query))
	qTri := trigrams(q)
	var scored []ScoredID
	for _, obj := range parts {
		text := strings.ToLower(obj.searchText())
		score := trigramOverlap(qTri, trigrams(text))
		if text != "" && strings.Contains(text, q) {
			score = 1
		}
		if score > 0 {
			scored = append(scored, obj.scored(score))
		}
	}
	sortScored(scored)
	return scored
}

// titleFirst moves hits whose title is the query to the front.
func titleFirst(hits []ScoredID, query string) []ScoredID {
	q := strings.ToLower(strings.TrimSpace(query))
	out := make([]ScoredID, 0, len(hits))
	var rest []ScoredID
	for _, h := range hits {
		if strings.ToLower(strings.TrimSpace(h.Title)) == q {
			out = append(out, h)
		} else {
			rest = append(rest, h)
		}
	}
	return append(out, rest...)
}

// candidateRows are the live rows scope can read whose record passes plan:
// a record itself, or a part whose parent does.
func (s *MemoryStore) candidateRows(scope Scope, plan FilterPlan) []Object {
	out := make([]Object, 0)
	for _, obj := range s.objects {
		if obj.DeletedAt != nil || !scope.CanRead(obj) {
			continue
		}
		record := obj
		if obj.ParentID != nil {
			record = s.objects[*obj.ParentID]
		}
		if !plan.Match(record) {
			continue
		}
		out = append(out, obj)
	}
	return out
}

// listRecords is the records under scope that plan matches, newest first,
// at most limit.
func (s *MemoryStore) listRecords(scope Scope, plan FilterPlan, limit int) []ScoredID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var records []Object
	for _, obj := range s.objects {
		if obj.ParentID == nil && obj.DeletedAt == nil && scope.CanRead(obj) && plan.Match(obj) {
			records = append(records, obj)
		}
	}
	slices.SortFunc(records, func(a, b Object) int {
		return cmp.Or(b.UpdatedAt.Compare(a.UpdatedAt), cmpUUID(a.ID, b.ID))
	})
	out := make([]ScoredID, 0, min(len(records), max(limit, 0)))
	for _, obj := range records[:min(len(records), max(limit, 0))] {
		out = append(out, obj.scored(0))
	}
	return out
}

func (obj Object) scored(score float64) ScoredID {
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

func (o Object) searchText() string {
	return strings.TrimSpace(o.Title + " " + o.Summary + " " + o.Content)
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
	return float64(inter) / float64(len(a)+len(b)-inter)
}

func (o Object) clone() Object {
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
	cp.Readers = slices.Clone(o.Readers)
	cp.Namespace = o.Namespace.Clone()
	return cp
}
