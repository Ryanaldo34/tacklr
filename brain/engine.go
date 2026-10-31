package brain

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Reranker optionally reorders/filters hydrated rich objects after search or find_objects.
// Host-owned product scoring; default nil leaves engine ranking unchanged.
type Reranker interface {
	Rerank(ctx context.Context, objects []RichObject) ([]RichObject, error)
}

// EngineConfig holds engine-owned knobs (not tool arguments). Ranking,
// fusion, recency decay and embedding are each backend's own options.
// SnippetCap is the evidence quote length in runes. 0 selects 240.
// CandidateK is how many hits Engine asks a backend for before it promotes,
// reranks and pages them. FailOnGraphError false (default) lets expand fall
// back to containment when the graph fails; true surfaces the error.
type EngineConfig struct {
	CandidateK       int
	EvidenceN        int
	SnippetCap       int
	DefaultLimit     int
	MaxLimit         int
	ExpandInlineMax  int
	SiblingRadius    int
	GraphNeighborK   int // max neighbors one expand walk returns (default 50)
	MaxExpandHops    int // max MaxHops on expand (default 4)
	MaxPathDepth     int // max MaxDepth on shortest path (default 6)
	MaxResultSetSize int
	FailOnGraphError bool
	Now              func() time.Time
}

// DefaultEngineConfig returns the engine defaults. A zero count on
// EngineConfig takes the matching field from here.
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		CandidateK:       40,
		EvidenceN:        3,
		SnippetCap:       defaultSnippetCap,
		DefaultLimit:     10,
		MaxLimit:         50,
		ExpandInlineMax:  20,
		SiblingRadius:    5,
		GraphNeighborK:   50,
		MaxExpandHops:    4,
		MaxPathDepth:     6,
		MaxResultSetSize: 1000,
		Now:              func() time.Time { return time.Now().UTC() },
	}
}

func (c EngineConfig) withDefaults() EngineConfig {
	d := DefaultEngineConfig()
	c.CandidateK = cmp.Or(c.CandidateK, d.CandidateK)
	c.EvidenceN = cmp.Or(c.EvidenceN, d.EvidenceN)
	c.SnippetCap = cmp.Or(c.SnippetCap, d.SnippetCap)
	c.DefaultLimit = cmp.Or(c.DefaultLimit, d.DefaultLimit)
	c.MaxLimit = cmp.Or(c.MaxLimit, d.MaxLimit)
	c.ExpandInlineMax = cmp.Or(c.ExpandInlineMax, d.ExpandInlineMax)
	c.SiblingRadius = cmp.Or(c.SiblingRadius, d.SiblingRadius)
	c.GraphNeighborK = cmp.Or(c.GraphNeighborK, d.GraphNeighborK)
	c.MaxExpandHops = cmp.Or(c.MaxExpandHops, d.MaxExpandHops)
	c.MaxPathDepth = cmp.Or(c.MaxPathDepth, d.MaxPathDepth)
	c.MaxResultSetSize = cmp.Or(c.MaxResultSetSize, d.MaxResultSetSize)
	if c.Now == nil {
		c.Now = d.Now
	}
	return c
}

// EngineOption configures NewEngine.
type EngineOption func(*Engine)

// IndexTextFunc returns the text that represents obj in the store and the
// graph (Indexed.Text), which each backend indexes and embeds its own way.
// defaultText is Engine's index text for this object (SkipIndex already applied).
// Return defaultText to keep it, or any rewrite that should occupy the vector
// space (prefix, replace, drop the body, join related titles, …). Empty means
// do not index. Title, Summary, Content, and Properties on the stored object
// are not modified.
type IndexTextFunc func(obj Object, defaultText string) string

// WithIndexText sets how Put builds an object's index text.
func WithIndexText(fn IndexTextFunc) EngineOption {
	return func(eng *Engine) { eng.indexTextFn = fn }
}

// WithConfig sets the engine configuration (normalized by NewEngine).
func WithConfig(cfg EngineConfig) EngineOption {
	return func(eng *Engine) { eng.cfg = cfg }
}

// WithIndex adds an index that receives every committed write of a
// first-class object. NewKnowledgeGraph adds its graph the same way.
func WithIndex(ix Index) EngineOption {
	return func(eng *Engine) { eng.indexes = append(eng.indexes, ix) }
}

// WithReranker sets an optional post-hydrate reranker for search and find_objects.
func WithReranker(r Reranker) EngineOption {
	return func(eng *Engine) { eng.reranker = r }
}

// WithExpandRecipes registers host-named expand views at construct time.
// Each recipe is a named ExpandRequest template (ObjectID filled at call time).
// Invalid recipes (empty name) fail NewEngine.
func WithExpandRecipes(recipes ...ExpandRecipe) EngineOption {
	return func(eng *Engine) {
		for _, r := range recipes {
			if err := eng.RegisterExpandRecipe(r); err != nil && eng.bootErr == nil {
				eng.bootErr = err
			}
		}
	}
}

// WithKinds registers host-defined object kinds at construct time.
// Invalid specs cause NewEngine to fail. Kinds are host/user-owned for determinism.
func WithKinds(specs ...KindSpec) EngineOption {
	return func(eng *Engine) {
		if eng.bootErr != nil {
			return
		}
		eng.bootErr = eng.catalog.register(specs...)
	}
}

// Engine is the boundary over a Store and its indexes: it validates
// requests, shapes index text from the kind catalog, writes the store and
// then every index, promotes and pages results, and audits. The backends own
// retrieval. Graph features are on KnowledgeGraph.
type Engine struct {
	store       Store
	indexes     []Index
	reranker    Reranker
	audit       AuditFunc
	recipeMu    sync.RWMutex
	recipes     map[string]ExpandRecipe // guarded by recipeMu
	cfg         EngineConfig
	catalog     *KindCatalog // always non-nil; empty ⇒ open mode
	bootErr     error        // set by WithKinds when registration fails
	indexTextFn IndexTextFunc
}

// NewEngine builds an Engine over a Store. store must be non-nil.
func NewEngine(store Store, opts ...EngineOption) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("brain: store is required")
	}
	e := &Engine{
		store:   store,
		cfg:     DefaultEngineConfig(),
		catalog: newKindCatalog(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	if e.bootErr != nil {
		return nil, e.bootErr
	}
	e.cfg = e.cfg.withDefaults()
	return e, nil
}

// RegisterKinds merges host kind definitions into the process catalog.
// Re-registering an existing kind name replaces that kind. Fails if the catalog is frozen.
func (e *Engine) RegisterKinds(_ context.Context, specs ...KindSpec) error {
	return e.catalog.register(specs...)
}

// FreezeCatalog rejects further RegisterKinds / LoadKindsFromStore.
// Also auto-frozen on first search/find_exact when the catalog is non-empty.
func (e *Engine) FreezeCatalog() {
	e.catalog.Freeze()
}

// Catalog returns the process kind catalog for inspection and host filter validation
// (e.g. ValidateFiltersAgainst). Empty means open mode. Prefer store.Setup plus
// LoadKindsFromStore, or ApplyKinds; do not mutate catalog fields directly.
func (e *Engine) Catalog() *KindCatalog {
	return e.catalog
}

// Read returns the full rich object for id under scope.
// The store returns a session_messages row when SessionID is set and the id is not an object.
func (e *Engine) Read(ctx context.Context, scope Scope, id uuid.UUID) (RichObject, error) {
	obj, err := e.Get(ctx, scope, id)
	if err != nil {
		return RichObject{}, err
	}
	return RichFromObject(obj, true), nil
}

// Schema returns kind documentation. Empty kind lists all registered kinds.
// When the process catalog is non-empty it is the source of truth; otherwise the store registry is used.
func (e *Engine) Schema(ctx context.Context, kind string) (SchemaResult, error) {
	kind = strings.TrimSpace(kind)
	if !e.catalog.Empty() {
		return schemaFromCatalog(e.catalog, kind)
	}
	return schemaFromStore(ctx, e.store, kind)
}

func schemaFromCatalog(cat *KindCatalog, kind string) (SchemaResult, error) {
	fu := DefaultFilterUsage()
	if kind != "" {
		spec, ok := cat.Get(kind)
		if !ok {
			return SchemaResult{}, fmt.Errorf("%w: kind %q", ErrNotFound, kind)
		}
		return SchemaResult{Kinds: []ObjectKindInfo{KindInfoFromSpec(spec)}, FilterUsage: fu}, nil
	}
	all := cat.All()
	out := make([]ObjectKindInfo, len(all))
	for i, spec := range all {
		out[i] = KindInfoFromSpec(spec)
	}
	return SchemaResult{Kinds: out, FilterUsage: fu}, nil
}

func schemaFromStore(ctx context.Context, store Store, kind string) (SchemaResult, error) {
	fu := DefaultFilterUsage()
	if kind != "" {
		k, err := store.GetKind(ctx, kind)
		if err != nil {
			return SchemaResult{}, err
		}
		return SchemaResult{Kinds: []ObjectKindInfo{kindInfoFromObjectKind(k)}, FilterUsage: fu}, nil
	}
	kinds, err := store.ListKinds(ctx)
	if err != nil {
		return SchemaResult{}, err
	}
	out := make([]ObjectKindInfo, len(kinds))
	for i, k := range kinds {
		out[i] = kindInfoFromObjectKind(k)
	}
	return SchemaResult{Kinds: out, FilterUsage: fu}, nil
}

// ListChildren returns ordered children for a parent visible under scope.
func (e *Engine) ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]RichObject, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return nil, err
	}
	if parentID == uuid.Nil {
		return nil, fmt.Errorf("brain: parent id is required")
	}
	if _, err := e.store.Get(ctx, scope, parentID); err != nil {
		return nil, err
	}
	parts, err := e.store.ListChildren(ctx, scope, parentID)
	if err != nil {
		return nil, err
	}
	out := make([]RichObject, 0, len(parts))
	for _, p := range parts {
		out = append(out, RichFromObject(p, false))
	}
	if err := e.recordRead(ctx, scope, richIDs(out)); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *Engine) normalizeLimit(limit int) int {
	if limit <= 0 {
		return e.cfg.DefaultLimit
	}
	return min(limit, e.cfg.MaxLimit)
}

// maxListByKind is the hard cap on ListByKind.
const maxListByKind = 500

// ListByKind returns first-class objects of kind (parent_id unset), newest-title order left to the store.
func (e *Engine) ListByKind(ctx context.Context, scope Scope, kind string, limit int) ([]Object, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > maxListByKind {
		limit = maxListByKind
	}
	objs, err := e.store.ListByKind(ctx, scope, kind, limit)
	if err != nil {
		return nil, err
	}
	if err := e.recordRead(ctx, scope, objectIDs(objs...)); err != nil {
		return nil, err
	}
	return objs, nil
}

// GetByProperty returns the first live object whose properties[key] equals value.
func (e *Engine) GetByProperty(ctx context.Context, scope Scope, key, value string) (Object, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Object{}, err
	}
	obj, err := e.store.GetByProperty(ctx, scope, key, value)
	if err != nil {
		return Object{}, err
	}
	if err := e.recordRead(ctx, scope, objectIDs(obj)); err != nil {
		return Object{}, err
	}
	return obj, nil
}

// KindsWithObjects lists distinct parent-kind names that already have objects under scope.
func (e *Engine) KindsWithObjects(ctx context.Context, scope Scope) ([]string, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return nil, err
	}
	return e.store.KindsWithObjects(ctx, scope)
}

// Get returns the stored object (including Content) under scope.
// The store returns a session_messages row when SessionID is set and the id is not an object.
func (e *Engine) Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return Object{}, err
	}
	if id == uuid.Nil {
		return Object{}, fmt.Errorf("%w: object id is required", ErrInvalid)
	}
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return Object{}, err
	}
	if err := e.recordRead(ctx, scope, objectIDs(obj)); err != nil {
		return Object{}, err
	}
	return obj, nil
}
