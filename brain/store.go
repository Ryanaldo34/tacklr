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

// Tree is an object read with its family in one call: a record with its
// parts, or a part with its record and that record's parts.
type Tree struct {
	Object Object
	// Parent is a part's record; nil for a record.
	Parent *Object
	// Parts are the record's parts by position; for a part, its record's,
	// itself included.
	Parts []Object
}

// RecordSearch is one search over a store's records and their parts.
type RecordSearch struct {
	Query string
	// Filters apply to the record a row belongs to: a part matches when its
	// parent does.
	Filters Filter
	// Exact asks for literal matching only: no semantic channel, and an
	// exact title match ranks first.
	Exact bool
	// Within, when set, keeps only the parts of that record (a part names
	// its record), and leaves out session messages. The store resolves it.
	Within uuid.UUID
	Limit  int
}

// Store is the knowledge store Engine runs on: the source of truth for
// objects and the search over records and their parts. A backend owns its
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
	// GetTree returns the object id names with its family in one read: at
	// most limit parts (every part when limit is not positive), and a
	// part's record. A saved session message has no family.
	GetTree(ctx context.Context, scope Scope, id uuid.UUID, limit int) (Tree, error)
	// GetByProperty returns a live object whose string property key is value.
	GetByProperty(ctx context.Context, scope Scope, key, value string) (Object, error)

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

	// Search ranks records and parts (a part's hit carries ParentID) best
	// first, at most q.Limit, over the channels the store chooses, with the
	// session's messages when Scope.SessionID is set. Filters do not apply
	// to session messages. An empty Query lists the records Filters match,
	// newest first (no parts, no session messages).
	Search(ctx context.Context, scope Scope, q RecordSearch) ([]ScoredID, error)

	// ListKinds and PutKind are the durable kind registry: Engine reads it
	// once (LoadKindsFromStore) and writes it from ApplyKinds and PersistKinds.
	ListKinds(ctx context.Context) ([]ObjectKind, error)
	PutKind(ctx context.Context, k ObjectKind) error

	// SaveSessionMessages writes one context window under
	// host.ForSession(sessionID) and returns its generation. A
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
