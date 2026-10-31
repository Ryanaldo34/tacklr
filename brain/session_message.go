package brain

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// SessionTailName is the namespace attribute appended for session messages.
	// Tool callers cannot set it.
	SessionTailName = "session"
	// KindSessionMessage is the kind of one saved context message.
	KindSessionMessage = "SessionMessage"
)

// SessionMessage is one exact message saved from a context window before a plan handoff.
// Body is the message JSON. SearchText is the visible text the store indexes
// (and embeds, when it has an Embedder) for retrieval.
type SessionMessage struct {
	Role       string
	Body       []byte
	SearchText string
}

// NamespaceForSession is the host namespace with the session id as the last attribute.
// The session id is stored raw, including ".". Lookup uses the attribute list.
func NamespaceForSession(host Namespace, sessionID string) Namespace {
	out := host.Clone()
	return append(out, Attr{Name: SessionTailName, Value: sessionID})
}

// SaveSessionMessages writes one pre-handoff context window under host.
// The store indexes each message; an embedding failure does not fail the save.
func (e *Engine) SaveSessionMessages(ctx context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error) {
	if err := checkScope(ctx, host); err != nil {
		return 0, err
	}
	gen, err := e.store.SaveSessionMessages(ctx, host, sessionID, generationKey, messages)
	if err != nil {
		return 0, err
	}
	return gen, e.record(ctx, Scope{Namespace: host, SessionID: sessionID}, AuditEvent{Op: AuditSessionSave, Count: len(messages)})
}

// DeleteSessionMessages removes every saved message for sessionID under host.
func (e *Engine) DeleteSessionMessages(ctx context.Context, host Namespace, sessionID string) error {
	if err := checkScope(ctx, host); err != nil {
		return err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	if err := e.store.DeleteSessionMessages(ctx, host, sessionID); err != nil {
		return err
	}
	return e.record(ctx, Scope{Namespace: host, SessionID: sessionID}, AuditEvent{Op: AuditSessionDelete})
}

// PurgeSessionMessages removes saved messages under host older than cutoff,
// for every session (retention for sessions that are never closed). It
// returns how many rows it removed.
func (e *Engine) PurgeSessionMessages(ctx context.Context, host Namespace, before time.Time) (int, error) {
	if err := checkScope(ctx, host); err != nil {
		return 0, err
	}
	n, err := e.store.PurgeSessionMessages(ctx, host, before)
	if err != nil {
		return 0, err
	}
	return n, e.record(ctx, Scope{Namespace: host}, AuditEvent{Op: AuditSessionPurge, Count: n})
}

// sessionSearchID is the session whose messages join this search.
// Kind allow-lists injected for objects must not hide those rows, so visibility
// uses the caller's filters, not the prepared object filters.
func sessionSearchID(scope Scope, filters Filter) string {
	id := strings.TrimSpace(scope.SessionID)
	if id == "" || !sessionMessagesVisible(filters) {
		return ""
	}
	return id
}

func sessionMessagesVisible(f Filter) bool {
	if f.Title.set() || f.CreatedAfter != "" || f.CreatedBefore != "" || f.UpdatedAfter != "" || f.UpdatedBefore != "" || len(f.Props) > 0 {
		return false
	}
	if f.Kind.Eq != "" && f.Kind.Eq != KindSessionMessage {
		return false
	}
	return len(f.Kind.In) == 0 || slices.Contains(f.Kind.In, KindSessionMessage)
}

type sessionRow struct {
	id            uuid.UUID
	sessionID     string
	generation    int
	generationKey string
	position      int
	role          string
	body          []byte
	searchText    string
	namespace     Namespace
	embedding     []float32
	model         string
	created       time.Time
	updated       time.Time
}

func (row sessionRow) object() Object {
	pos := row.position
	return Object{
		ID:             row.id,
		Kind:           KindSessionMessage,
		Title:          row.role,
		Content:        string(row.body),
		Properties:     sessionProps(row),
		Position:       &pos,
		Namespace:      row.namespace.Clone(),
		Embedding:      slices.Clone(row.embedding),
		EmbeddingModel: row.model,
		CreatedAt:      row.created,
		UpdatedAt:      row.updated,
	}
}

func sessionProps(row sessionRow) map[string]any {
	return map[string]any{
		"role":       row.role,
		"generation": row.generation,
		"index":      row.position,
	}
}

func (s *MemoryStore) initSessionLocked() {
	if s.sessionMsgs == nil {
		s.sessionMsgs = map[uuid.UUID]sessionRow{}
	}
	if s.sessionKeys == nil {
		s.sessionKeys = map[string]map[string]int{}
	}
}

// SaveSessionMessages implements Store. It embeds each message's SearchText
// in batches; a failed batch is logged and saves without vectors.
func (s *MemoryStore) SaveSessionMessages(ctx context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error) {
	sessionID = strings.TrimSpace(sessionID)
	generationKey = strings.TrimSpace(generationKey)
	if sessionID == "" {
		return 0, fmt.Errorf("%w: session id is required", ErrInvalid)
	}
	if generationKey == "" {
		return 0, fmt.Errorf("%w: generation key is required", ErrInvalid)
	}
	if len(messages) == 0 {
		return 0, fmt.Errorf("%w: window is empty", ErrInvalid)
	}
	if err := host.Validate(); err != nil {
		return 0, err
	}
	vecs, err := s.embedMessages(ctx, messages)
	if err != nil {
		return 0, err
	}
	ns := NamespaceForSession(host, sessionID)
	key := ns.Key()
	now := time.Now().UTC()
	model := s.embedding.Model()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initSessionLocked()
	if gen, ok := s.sessionKeys[key][generationKey]; ok {
		return gen, nil
	}
	gen := 1
	for _, row := range s.sessionMsgs {
		if row.sessionID == sessionID && row.generation >= gen {
			gen = row.generation + 1
		}
	}
	for i, msg := range messages {
		id := uuid.New()
		var rowModel string
		if vecs[i] != nil {
			rowModel = model
		}
		s.sessionMsgs[id] = sessionRow{
			id:            id,
			sessionID:     sessionID,
			generation:    gen,
			generationKey: generationKey,
			position:      i,
			role:          msg.Role,
			body:          slices.Clone(msg.Body),
			searchText:    msg.SearchText,
			namespace:     ns.Clone(),
			embedding:     vecs[i],
			model:         rowModel,
			created:       now,
			updated:       now,
		}
	}
	if s.sessionKeys[key] == nil {
		s.sessionKeys[key] = map[string]int{}
	}
	s.sessionKeys[key][generationKey] = gen
	return gen, nil
}

// embedMessages returns one vector per message (nil when it has no text or
// its batch failed). Only a done ctx fails it.
func (s *MemoryStore) embedMessages(ctx context.Context, messages []SessionMessage) ([][]float32, error) {
	vecs := make([][]float32, len(messages))
	if !s.embedding.Enabled() {
		return vecs, nil
	}
	var pending []int
	for i, m := range messages {
		if strings.TrimSpace(m.SearchText) != "" {
			pending = append(pending, i)
		}
	}
	for batch := range slices.Chunk(pending, cmp.Or(s.embedding.BatchSize, 128)) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		texts := make([]string, len(batch))
		for j, i := range batch {
			texts[j] = strings.TrimSpace(messages[i].SearchText)
		}
		got, err := s.embedding.Embed(ctx, texts)
		if err != nil {
			slog.ErrorContext(ctx, "session message embed failed", "error", err)
			continue
		}
		for j, i := range batch {
			vecs[i] = got[j]
		}
	}
	return vecs, nil
}

// appendSessionParts adds this session's messages to an object candidate list.
// Caller holds s.mu. Filters already decided whether SessionID is set.
func (s *MemoryStore) appendSessionParts(scope Scope, parts []Object) []Object {
	for _, row := range s.sessionRows(scope) {
		obj := row.object()
		id := obj.ID
		obj.Content = row.searchText
		obj.ParentID = &id
		parts = append(parts, obj)
	}
	return parts
}

// DeleteSessionMessages implements Store.
func (s *MemoryStore) DeleteSessionMessages(_ context.Context, host Namespace, sessionID string) error {
	if err := host.Validate(); err != nil {
		return err
	}
	ns := NamespaceForSession(host, strings.TrimSpace(sessionID))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initSessionLocked()
	for id, row := range s.sessionMsgs {
		if row.namespace.Equal(ns) {
			delete(s.sessionMsgs, id)
		}
	}
	delete(s.sessionKeys, ns.Key())
	return nil
}

// PurgeSessionMessages implements Store.
func (s *MemoryStore) PurgeSessionMessages(_ context.Context, host Namespace, before time.Time) (int, error) {
	if err := host.Validate(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initSessionLocked()
	n := 0
	for id, row := range s.sessionMsgs {
		if host.Covers(row.namespace) && row.created.Before(before) {
			delete(s.sessionMsgs, id)
			n++
		}
	}
	return n, nil
}

func (s *MemoryStore) sessionRows(scope Scope) []sessionRow {
	if s.sessionMsgs == nil || strings.TrimSpace(scope.SessionID) == "" {
		return nil
	}
	ns := NamespaceForSession(scope.Namespace, scope.SessionID)
	out := make([]sessionRow, 0)
	for _, row := range s.sessionMsgs {
		if row.namespace.Equal(ns) {
			out = append(out, row)
		}
	}
	return out
}
