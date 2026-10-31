package brain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AuditOp names the operation an AuditEvent records.
type AuditOp string

// Audit operations recorded by Engine.
const (
	AuditRead          AuditOp = "read"
	AuditPut           AuditOp = "put"
	AuditDelete        AuditOp = "delete"
	AuditLink          AuditOp = "link"
	AuditUnlink        AuditOp = "unlink"
	AuditPurge         AuditOp = "purge"
	AuditErase         AuditOp = "erase"
	AuditSessionSave   AuditOp = "session_save"
	AuditSessionDelete AuditOp = "session_delete"
	AuditSessionPurge  AuditOp = "session_purge"
)

// AuditEvent is one brain operation: who (namespace, principals, session),
// what (op), and which objects. Reads list the ids returned to the caller.
type AuditEvent struct {
	Time       time.Time
	Op         AuditOp
	Namespace  Namespace
	Principals []string
	SessionID  string
	ObjectIDs  []uuid.UUID
	// Count is the number of rows a purge or erase removed.
	Count int
}

// auditTimeout bounds one audit record once it no longer follows the caller's ctx.
const auditTimeout = 10 * time.Second

// AuditFunc records one audit event. postgres.Store.Record is one (audit_events).
type AuditFunc func(ctx context.Context, ev AuditEvent) error

// WithAudit records every read and write the Engine serves. An error from
// record fails the operation, so an operation is never served without its event.
// The record runs after the operation, so it does not stop when the caller
// cancels: a committed write still gets its event, within auditTimeout.
func WithAudit(record AuditFunc) EngineOption {
	return func(eng *Engine) { eng.audit = record }
}

// record fills ev's time and caller from scope and passes it to the audit
// func. It is a no-op without WithAudit.
func (e *Engine) record(ctx context.Context, scope Scope, ev AuditEvent) error {
	if e.audit == nil {
		return nil
	}
	ev.Time = e.cfg.Now()
	ev.Namespace = scope.Namespace.Clone()
	ev.Principals = scope.Principals
	ev.SessionID = scope.SessionID
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditTimeout)
	defer cancel()
	if err := e.audit(ctx, ev); err != nil {
		return fmt.Errorf("brain: audit %s: %w", ev.Op, err)
	}
	return nil
}

// recordRead records the ids served to the caller. Nothing served, nothing recorded.
func (e *Engine) recordRead(ctx context.Context, scope Scope, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	return e.record(ctx, scope, AuditEvent{Op: AuditRead, ObjectIDs: ids})
}

// checkScope fails when ctx is done or ns is invalid.
func checkScope(ctx context.Context, ns Namespace) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ns.Validate()
}
