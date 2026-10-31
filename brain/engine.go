package brain

import (
	"cmp"
	"context"
	"fmt"
	"sync"
	"time"
)

// EngineConfig holds engine-owned knobs (not tool arguments). Ranking,
// fusion within a backend, recency decay and embedding are each backend's
// own options. SnippetCap is the evidence quote length in runes (0 selects
// 240). CandidateK is how many hits Engine asks each source for before it
// fuses, reranks and pages them. FailOnGraphError false (default) lets
// Related fall back to the children when the graph fails; true surfaces the
// error.
type EngineConfig struct {
	CandidateK       int
	EvidenceN        int
	SnippetCap       int
	DefaultLimit     int
	MaxLimit         int
	SiblingRadius    int
	GraphNeighborK   int // max neighbors one graph walk returns (default 50)
	MaxExpandHops    int // max Depth on Related (default 4)
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
		SiblingRadius:    5,
		GraphNeighborK:   50,
		MaxExpandHops:    4,
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
	c.SiblingRadius = cmp.Or(c.SiblingRadius, d.SiblingRadius)
	c.GraphNeighborK = cmp.Or(c.GraphNeighborK, d.GraphNeighborK)
	c.MaxExpandHops = cmp.Or(c.MaxExpandHops, d.MaxExpandHops)
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

// WithIndexText sets how Save builds an object's index text.
func WithIndexText(fn IndexTextFunc) EngineOption {
	return func(eng *Engine) { eng.indexTextFn = fn }
}

// WithConfig sets the engine configuration (normalized by NewEngine).
func WithConfig(cfg EngineConfig) EngineOption {
	return func(eng *Engine) { eng.cfg = cfg }
}

// WithGraph adds the knowledge graph: it receives every committed write as
// an Index, ranks objects and links for Search, and walks relations for
// Related. It also enables Link.
func WithGraph(g Graph) EngineOption {
	return func(eng *Engine) {
		eng.graph = g
		eng.indexes = append(eng.indexes, g)
	}
}

// WithIndex adds an index that receives every committed write of a
// first-class object (a second search system, a change feed).
func WithIndex(ix Index) EngineOption {
	return func(eng *Engine) { eng.indexes = append(eng.indexes, ix) }
}

// RerankFunc reorders the objects at the head of a search for query, for
// example with a cross-encoder. It returns the objects to keep, in order;
// an object it leaves out is dropped from the results.
type RerankFunc func(ctx context.Context, query string, objects []RichObject) ([]RichObject, error)

// WithReranker reranks the head of every search with fn. Without it, Search
// keeps the fused order.
func WithReranker(fn RerankFunc) EngineOption {
	return func(eng *Engine) {
		eng.rerank = func(ctx context.Context, scope Scope, query string, refs []ref) ([]ref, error) {
			return eng.rerankHead(ctx, scope, query, refs, fn)
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

// Engine is the boundary over a Store, an optional Graph and other
// indexes. It validates requests, shapes index text from the kind catalog,
// writes the store and then every index, fuses the backends' ranked lists,
// reads every result from the store, pages, and audits. The backends own
// retrieval.
type Engine struct {
	store   Store
	graph   Graph // optional; also in indexes
	indexes []Index
	// rerank orders a search's fused refs: keepOrder unless WithReranker.
	rerank      func(ctx context.Context, scope Scope, query string, refs []ref) ([]ref, error)
	audit       AuditFunc
	cfg         EngineConfig
	catalog     *KindCatalog // always non-nil; empty ⇒ open mode
	bootErr     error        // set by WithKinds when registration fails
	indexTextFn IndexTextFunc
	// registry is the store's kind registry, read once by the first Open in
	// open mode (a failed read is retried by the next).
	registry struct {
		sync.Mutex
		kinds map[string]ObjectKind
	}
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
		rerank:  keepOrder,
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

// HasGraph reports whether the engine has a graph, so the harness registers
// the link tool.
func (e *Engine) HasGraph() bool { return e.graph != nil }

// RegisterKinds merges host kind definitions into the process catalog.
// Re-registering an existing kind name replaces that kind. Fails if the catalog is frozen.
func (e *Engine) RegisterKinds(_ context.Context, specs ...KindSpec) error {
	return e.catalog.register(specs...)
}

// FreezeCatalog rejects further RegisterKinds / LoadKindsFromStore.
// Also auto-frozen on the first search when the catalog is non-empty.
func (e *Engine) FreezeCatalog() {
	e.catalog.Freeze()
}

// Catalog returns the process kind catalog for inspection. Empty means open
// mode. Prefer store.Setup plus LoadKindsFromStore, or ApplyKinds; do not
// mutate catalog fields directly.
func (e *Engine) Catalog() *KindCatalog {
	return e.catalog
}

func (e *Engine) normalizeLimit(limit int) int {
	if limit <= 0 {
		return e.cfg.DefaultLimit
	}
	return min(limit, e.cfg.MaxLimit)
}
