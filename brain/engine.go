package brain

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// QueryEmbedder embeds a query string for the dense search channel.
// When nil on the Engine, search runs lexical-only.
type QueryEmbedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// Reranker optionally reorders/filters hydrated rich objects after search or find_objects.
// Host-owned product scoring; default nil leaves engine ranking unchanged.
type Reranker interface {
	Rerank(ctx context.Context, objects []RichObject) ([]RichObject, error)
}

// EngineConfig holds engine-owned ranking knobs (not tool arguments).
// Lambda nil → default mild decay; explicit 0 disables temporal bias.
// FailOn* false (default) soft-degrades embedder/graph failures; true surfaces errors.
type EngineConfig struct {
	CandidateK          int
	RRFk                int
	Lambda              *float64
	EvidenceN           int
	DefaultLimit        int
	MaxLimit            int
	ExpandInlineMax     int
	SiblingRadius       int
	GraphNeighborK      int
	MaxExpandHops       int // max MaxHops on expand (default 4)
	MaxGraphExpandRPCs  int // cap Neighbors calls per multi-hop expand (default 64)
	MaxResultSetSize    int
	FailOnEmbedderError bool
	FailOnGraphError    bool
	Now                 func() time.Time
}

// DefaultEngineConfig returns mild production defaults.
func DefaultEngineConfig() EngineConfig {
	lam := 0.02
	return EngineConfig{
		CandidateK:         40,
		RRFk:               60,
		Lambda:             &lam,
		EvidenceN:          3,
		DefaultLimit:       10,
		MaxLimit:           50,
		ExpandInlineMax:    20,
		SiblingRadius:      5,
		GraphNeighborK:     50,
		MaxExpandHops:      4,
		MaxGraphExpandRPCs: 64,
		MaxResultSetSize:   1000,
		Now:                func() time.Time { return time.Now().UTC() },
	}
}

func (c EngineConfig) withDefaults() EngineConfig {
	d := DefaultEngineConfig()
	c.CandidateK = posOr(c.CandidateK, d.CandidateK)
	c.RRFk = posOr(c.RRFk, d.RRFk)
	c.EvidenceN = posOr(c.EvidenceN, d.EvidenceN)
	c.DefaultLimit = posOr(c.DefaultLimit, d.DefaultLimit)
	c.MaxLimit = posOr(c.MaxLimit, d.MaxLimit)
	c.ExpandInlineMax = posOr(c.ExpandInlineMax, d.ExpandInlineMax)
	c.SiblingRadius = posOr(c.SiblingRadius, d.SiblingRadius)
	c.GraphNeighborK = posOr(c.GraphNeighborK, d.GraphNeighborK)
	c.MaxExpandHops = posOr(c.MaxExpandHops, d.MaxExpandHops)
	c.MaxGraphExpandRPCs = posOr(c.MaxGraphExpandRPCs, d.MaxGraphExpandRPCs)
	c.MaxResultSetSize = posOr(c.MaxResultSetSize, d.MaxResultSetSize)
	if c.Lambda == nil {
		c.Lambda = d.Lambda
	}
	if c.Now == nil {
		c.Now = d.Now
	}
	return c
}

func (c EngineConfig) allowEmbedderDegrade() bool { return !c.FailOnEmbedderError }
func (c EngineConfig) allowGraphDegrade() bool    { return !c.FailOnGraphError }

func posOr(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func (c EngineConfig) lambdaValue() float64 {
	return *c.Lambda
}

// EngineOption configures NewEngine.
type EngineOption func(*Engine)

// WithEmbedder sets the optional query embedder for hybrid search.
func WithEmbedder(e QueryEmbedder) EngineOption {
	return func(eng *Engine) { eng.embedder = e }
}

// WithConfig sets ranking configuration (normalized by NewEngine).
func WithConfig(cfg EngineConfig) EngineOption {
	return func(eng *Engine) { eng.cfg = cfg }
}

// WithGraph sets the optional non-containment graph backend (Helix or MemoryGraph).
// Writer and object-search capabilities are resolved once here (not re-asserted per call).
func WithGraph(g GraphReader) EngineOption {
	return func(eng *Engine) {
		eng.graph = g
		eng.graphW, _ = g.(GraphWriter)
		eng.graphS, _ = g.(GraphObjectSearcher)
		eng.graphE, _ = g.(GraphEdgeSearcher)
	}
}

// WithObserver sets retrieval observability (default no-op).
func WithObserver(o Observer) EngineOption {
	return func(eng *Engine) { eng.observer = o }
}

// WithReranker sets an optional post-hydrate reranker for search and find_objects.
func WithReranker(r Reranker) EngineOption {
	return func(eng *Engine) { eng.reranker = r }
}

// WithExpandRecipes registers host-named expand views at construct time.
// Each recipe is a named ExpandRequest template (ObjectID filled at call time).
// Invalid recipes (empty name) are ignored here; use RegisterExpandRecipe for errors.
func WithExpandRecipes(recipes ...ExpandRecipe) EngineOption {
	return func(eng *Engine) {
		for _, r := range recipes {
			_ = eng.RegisterExpandRecipe(r)
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

// Engine is the retrieval facade over a Store.
type Engine struct {
	store    Store
	embedder QueryEmbedder
	graph    GraphReader         // optional expand Neighbors
	graphW   GraphWriter         // optional dual-write / Link (resolved in WithGraph)
	graphS   GraphObjectSearcher // optional find_objects (resolved in WithGraph)
	graphE   GraphEdgeSearcher   // optional edge text search (resolved in WithGraph)
	reranker Reranker
	recipeMu sync.RWMutex
	recipes  map[string]ExpandRecipe // guarded by recipeMu
	observer Observer
	cfg      EngineConfig
	catalog  *KindCatalog // always non-nil; empty ⇒ open mode
	bootErr  error        // set by WithKinds when registration fails
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
	e.observer = observerOrNoop(e.observer)
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

// Catalog returns the process kind catalog (empty = open mode).
func (e *Engine) Catalog() *KindCatalog {
	return e.catalog
}

// Read returns the full rich object for id under scope.
func (e *Engine) Read(ctx context.Context, scope Scope, id uuid.UUID) (RichObject, error) {
	if id == uuid.Nil {
		return RichObject{}, ErrObjectIDRequired
	}
	obj, err := e.store.Get(ctx, scope, id)
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

func schemaFromStore(ctx context.Context, store KindReader, kind string) (SchemaResult, error) {
	fu := DefaultFilterUsage()
	if kind != "" {
		k, err := store.GetKind(ctx, kind)
		if err != nil {
			return SchemaResult{}, err
		}
		return SchemaResult{Kinds: []ObjectKindInfo{KindInfoFrom(k)}, FilterUsage: fu}, nil
	}
	kinds, err := store.ListKinds(ctx)
	if err != nil {
		return SchemaResult{}, err
	}
	out := make([]ObjectKindInfo, len(kinds))
	for i, k := range kinds {
		out[i] = KindInfoFrom(k)
	}
	return SchemaResult{Kinds: out, FilterUsage: fu}, nil
}

// ListChildren returns ordered children for a parent visible under scope.
func (e *Engine) ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]RichObject, error) {
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
	return out, nil
}

func (e *Engine) normalizeLimit(limit int) int {
	if limit <= 0 {
		return e.cfg.DefaultLimit
	}
	return min(limit, e.cfg.MaxLimit)
}
