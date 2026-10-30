package brain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ObjectReader is the read port for knowledge objects.
// When Scope.SessionID is set, Get and GetMany also return a session_messages
// row for that session if the id is not an object.
type ObjectReader interface {
	Get(ctx context.Context, scope Scope, id uuid.UUID) (Object, error)
	// GetMany returns objects for ids in the same order. Missing/out-of-scope ids are omitted.
	GetMany(ctx context.Context, scope Scope, ids []uuid.UUID) ([]Object, error)
	// ListChildren returns parts ordered by position.
	ListChildren(ctx context.Context, scope Scope, parentID uuid.UUID) ([]Object, error)
}

// KindReader reads durable kind schema rows (schema fallback, LoadKindsFromStore).
type KindReader interface {
	GetKind(ctx context.Context, kind string) (ObjectKind, error)
	ListKinds(ctx context.Context) ([]ObjectKind, error)
}

// KindWriter upserts durable kind schema rows (ApplyKinds / PersistKinds).
// Not required for open-mode or process-only catalogs. Together with KindReader
// this is KindRegistry — implement both for a custom durable backend.
type KindWriter interface {
	PutKind(ctx context.Context, k ObjectKind) error
}

// ObjectWriter persists knowledge objects (Engine.Put / PutTree / SoftDelete).
// MemoryStore and postgres.Store implement it. Custom backends implement
// ObjectWriter for other deployments. Not required for read-only engines.
//
// Every write is guarded by scope: the object's namespace must lie inside
// scope, and a row that already has the id must be visible under scope.
// Otherwise the write changes nothing and returns ErrNotFound, so one tenant
// can never overwrite another tenant's row by reusing its id.
//
// A tree write (Engine.PutTree) has three steps, so no step holds every part
// in memory or one transaction open while parts are embedded:
//
//  1. TreeState reads the parent's tree revision and its live part ids.
//  2. StageParts stores parts in batches for one run. Staged parts are
//     invisible to every read and search.
//  3. CommitTree swaps the run's parts in atomically.
type ObjectWriter interface {
	Put(ctx context.Context, scope Scope, obj Object) error
	// TreeState returns parentID's tree revision and the ids of its live
	// parts under scope. A parent that does not exist has revision 0 and
	// no parts.
	TreeState(ctx context.Context, scope Scope, parentID uuid.UUID) (TreeState, error)
	// StageParts stores parts for run. Every part's namespace must lie
	// inside scope, and part ids must be unique within run.
	StageParts(ctx context.Context, scope Scope, run uuid.UUID, parts []TreePart) error
	// CommitTree upserts parent and makes run's staged parts its only parts,
	// in one atomic write: new parts are inserted, reused parts take their
	// staged position and properties, and every other part is deleted.
	// Parts take the parent's namespace and readers. When rev is not the
	// parent's current tree revision, CommitTree returns ErrConflict and
	// changes nothing.
	CommitTree(ctx context.Context, scope Scope, run uuid.UUID, rev int64, parent Object) error
	// SoftDelete marks id and its parts deleted.
	SoftDelete(ctx context.Context, scope Scope, id uuid.UUID) error
}

// TreeState is where a tree write starts: the parent's tree revision, which
// every commit increments, and the ids of its live parts.
type TreeState struct {
	Rev  int64
	Live map[uuid.UUID]struct{}
}

// TreePart is one staged part of a tree write. A reused part's row is live
// already, so CommitTree only applies its Position and Properties; only ID,
// ParentID, Namespace, Position and Properties are set.
type TreePart struct {
	Object Object
	Reused bool
}

// SessionWriter persists saved context windows in session_messages
// (Engine.SaveSessionMessages / DeleteSessionMessages / PurgeSessionMessages).
// Rows live under NamespaceForSession(host, sessionID).
type SessionWriter interface {
	SaveSessionMessages(ctx context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error)
	// DeleteSessionMessages removes the session's rows under host.
	DeleteSessionMessages(ctx context.Context, host Namespace, sessionID string) error
	// PurgeSessionMessages removes rows under host saved before cutoff and
	// returns how many it removed.
	PurgeSessionMessages(ctx context.Context, host Namespace, before time.Time) (int, error)
}

// Purger hard-deletes rows (retention and erasure requests). Readers do not
// limit it: it acts on every row whose namespace scope covers.
type Purger interface {
	// PurgeDeleted removes objects under scope soft-deleted before cutoff
	// and returns how many it removed. It also drops parts staged before
	// cutoff by tree writes that never committed (they are not counted).
	PurgeDeleted(ctx context.Context, scope Scope, before time.Time) (int, error)
	// Erase removes every object, staged part and saved session message under scope and
	// returns the ids of the first-class objects it removed.
	Erase(ctx context.Context, scope Scope) ([]uuid.UUID, error)
}

// ObjectLister lists first-class objects by kind and looks up a property value
// (used by the Engram Provider for ReadDir / path → id). Parts are omitted.
type ObjectLister interface {
	ListByKind(ctx context.Context, scope Scope, kind string, limit int) ([]Object, error)
	GetByProperty(ctx context.Context, scope Scope, key, value string) (Object, error)
	KindsWithObjects(ctx context.Context, scope Scope) ([]string, error)
}

// PartSearcher is corpus retrieval over the store: parts only (parent_id set).
// Query text matches part title, summary, and content. Filters apply to the
// part row. When Scope.SessionID is set, the same call also ranks that
// session's rows in session_messages and fuses the two lists. Engine.Search
// promotes hits to the parent. Entity search over parent records is
// GraphObjectSearcher (FindObjects), not this port.
type PartSearcher interface {
	SearchLexical(ctx context.Context, scope Scope, query string, filters Filter, k int) ([]ScoredID, error)
	// SearchVector compares embedding only with rows embedded by model.
	SearchVector(ctx context.Context, scope Scope, model string, embedding []float32, filters Filter, k int) ([]ScoredID, error)
	SearchTrigram(ctx context.Context, scope Scope, query string, filters Filter, k int) ([]ScoredID, error)
}

// Store is the full read + search surface required by Engine.
type Store interface {
	ObjectReader
	KindReader
	PartSearcher
}
