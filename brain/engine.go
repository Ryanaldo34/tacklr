package brain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// QueryEmbedder embeds a query string for the dense search channel.
// When nil on the Engine, search runs lexical-only.
type QueryEmbedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// EngineConfig holds engine-owned ranking knobs (never tool arguments).
// Lambda is optional: nil uses the default mild decay; explicit 0 disables temporal bias.
type EngineConfig struct {
	CandidateK   int
	RRFk         int
	Lambda       *float64
	EvidenceN    int
	DefaultLimit int
	MaxLimit     int
	Now          func() time.Time
}

// DefaultEngineConfig returns mild production defaults.
func DefaultEngineConfig() EngineConfig {
	lam := 0.02
	return EngineConfig{
		CandidateK:   40,
		RRFk:         60,
		Lambda:       &lam,
		EvidenceN:    3,
		DefaultLimit: 10,
		MaxLimit:     50,
		Now:          func() time.Time { return time.Now().UTC() },
	}
}

func (c EngineConfig) withDefaults() EngineConfig {
	d := DefaultEngineConfig()
	if c.CandidateK <= 0 {
		c.CandidateK = d.CandidateK
	}
	if c.RRFk <= 0 {
		c.RRFk = d.RRFk
	}
	if c.Lambda == nil {
		c.Lambda = d.Lambda
	}
	if c.EvidenceN <= 0 {
		c.EvidenceN = d.EvidenceN
	}
	if c.DefaultLimit <= 0 {
		c.DefaultLimit = d.DefaultLimit
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = d.MaxLimit
	}
	if c.Now == nil {
		c.Now = d.Now
	}
	return c
}

// lambdaValue returns the configured decay rate. withDefaults ensures Lambda is non-nil on Engine.
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

// Engine is the retrieval facade over a Store.
type Engine struct {
	store    Store
	embedder QueryEmbedder
	cfg      EngineConfig
}

// NewEngine builds an Engine over a Store. store must be non-nil.
func NewEngine(store Store, opts ...EngineOption) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("brain: store is required")
	}
	e := &Engine{store: store, cfg: DefaultEngineConfig()}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	e.cfg = e.cfg.withDefaults()
	return e, nil
}

// Read returns the full rich object for id under scope.
func (e *Engine) Read(ctx context.Context, scope Scope, id uuid.UUID) (RichObject, error) {
	if id == uuid.Nil {
		return RichObject{}, fmt.Errorf("brain: object id is required")
	}
	obj, err := e.store.Get(ctx, scope, id)
	if err != nil {
		return RichObject{}, err
	}
	return RichFromObject(obj, true), nil
}

// Schema returns kind documentation. Empty kind lists all registered kinds.
func (e *Engine) Schema(ctx context.Context, kind string) (SchemaResult, error) {
	kind = strings.TrimSpace(kind)
	if kind != "" {
		k, err := e.store.GetKind(ctx, kind)
		if err != nil {
			return SchemaResult{}, err
		}
		return SchemaResult{Kinds: []ObjectKindInfo{KindInfoFrom(k)}}, nil
	}
	kinds, err := e.store.ListKinds(ctx)
	if err != nil {
		return SchemaResult{}, err
	}
	out := make([]ObjectKindInfo, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, KindInfoFrom(k))
	}
	return SchemaResult{Kinds: out}, nil
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
	if limit > e.cfg.MaxLimit {
		return e.cfg.MaxLimit
	}
	return limit
}
