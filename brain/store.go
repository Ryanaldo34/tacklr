package brain

import (
	"context"
	"iter"
	"time"

	"github.com/google/uuid"
)

// Indexed is an object and the text the engine chose to represent it in
// search (entity text for a parent, parent-prefixed text for a part, as
// shaped by WithIndexText). A backend indexes Text its own way: lexical,
// embedded with its own Embedder, or both. Empty Text is not indexed.
type Indexed struct {
	Object
	Text string
}

// PartSearch is one corpus search over a store's parts.
type PartSearch struct {
	Query   string
	Filters Filter // applied to the part row
	// Exact asks for literal matching only (find_exact): no semantic
	// channel, and an exact title match ranks first.
	Exact bool
	Limit int
}

// Store is the knowledge store Engine runs on: the source of truth for
// objects and the corpus search over their parts. A backend owns its
// retrieval: its Embedder, channels, ranking and fusion, filters, and the
// scope and readers rules below. MemoryStore and brain/postgres.Store
// implement it, and brain/storetest is the contract a store must pass. A
// backend without a feature returns ErrUnsupported from that method.
//
// Reads return only rows the scope can read (Scope.CanRead) that are not
// soft-deleted, and omit the rest as if missing. When Scope.SessionID is
// set, Get, GetMany and Search also reach that session's saved messages.
//
// Every write is guarded by scope: the object's namespace must lie inside
// scope, and a row that already has the id must be visible under scope.
// Otherwise the write changes nothing and returns ErrNotFound, so one tenant
// never overwrites another tenant's row by reusing its id.
type Store interface {
	Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error)
	// GetMany returns the objects for ids in the same order, omitting ids
	// it cannot return.
	GetMany(ctx context.Context, scope Scope, ids []uuid.UUID) ([]Object, error)
	// ListChildren returns parts ordered by position.
	ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]Object, error)
	// ListByKind returns up to limit first-class objects of kind.
	ListByKind(ctx context.Context, scope Scope, kind string, limit int) ([]Object, error)
	// GetByProperty returns a live object whose string property key is value.
	GetByProperty(ctx context.Context, scope Scope, key, value string) (Object, error)
	// KindsWithObjects lists the kinds that have first-class objects.
	KindsWithObjects(ctx context.Context, scope Scope) ([]string, error)

	// Put stores obj, indexes its Text, and returns the stored object. An
	// embedding failure fails the Put.
	Put(ctx context.Context, scope Scope, obj Indexed) (Object, error)
	// PutTree upserts parent and makes parts its only parts, in one atomic
	// commit: until then reads see the previous parts, and a failure leaves
	// them. Parts arrive in order with content-derived ids, ParentID, the
	// parent's namespace and readers, and Position set. A part whose id is
	// live keeps its row and index, so only new parts are indexed, and
	// memory stays bounded whatever the number of parts (Embedding.
	// EmbedBatches is a pipeline for this). When another PutTree of the
	// same parent commits first, PutTree returns ErrConflict and changes
	// nothing. It returns the stored parent.
	PutTree(ctx context.Context, scope Scope, parent Indexed, parts iter.Seq2[Indexed, error]) (Object, error)
	// SoftDelete marks id and its parts deleted.
	SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error
	// PurgeDeleted removes objects under scope soft-deleted before cutoff
	// and returns how many it removed. Readers do not limit it.
	PurgeDeleted(ctx context.Context, scope Scope, before time.Time) (int, error)
	// Erase removes every object and saved session message under scope and
	// returns the ids of the first-class objects it removed. Readers do not
	// limit it.
	Erase(ctx context.Context, scope Scope) ([]uuid.UUID, error)

	// Search ranks parts (hits carry ParentID) best first, at most
	// q.Limit, over the channels the store chooses, with the session's
	// messages when Scope.SessionID is set.
	Search(ctx context.Context, scope Scope, q PartSearch) ([]ScoredID, error)

	GetKind(ctx context.Context, kind string) (ObjectKind, error)
	ListKinds(ctx context.Context) ([]ObjectKind, error)
	PutKind(ctx context.Context, k ObjectKind) error

	// SaveSessionMessages writes one context window under
	// NamespaceForSession(host, sessionID) and returns its generation. A
	// repeated generationKey returns the first save's generation. The
	// store indexes each message's SearchText; an embedding failure does
	// not fail the save.
	SaveSessionMessages(ctx context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error)
	// DeleteSessionMessages removes the session's rows under host.
	DeleteSessionMessages(ctx context.Context, host Namespace, sessionID string) error
	// PurgeSessionMessages removes rows under host saved before cutoff and
	// returns how many it removed.
	PurgeSessionMessages(ctx context.Context, host Namespace, before time.Time) (int, error)
}
