package brain

import (
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
// Body is the message JSON. SearchText is the visible text used for retrieval.
// Embedding is optional. A failed embed leaves it nil and the row still saves.
type SessionMessage struct {
	Role       string
	Body       []byte
	SearchText string
	Embedding  []float32
}

// NamespaceForSession is the host namespace with the session id as the last attribute.
// The session id is stored raw, including ".". Lookup uses the attribute list.
func NamespaceForSession(host Namespace, sessionID string) Namespace {
	out := host.Clone()
	return append(out, Attr{Name: SessionTailName, Value: sessionID})
}

// SaveSessionMessages writes one pre-handoff context window through ObjectWriter.
// Embed errors do not fail the save. A store that cannot write returns ErrUnsupported.
func (e *Engine) SaveSessionMessages(ctx context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	w, err := e.objectWriter()
	if err != nil {
		return 0, err
	}
	if e.embedder != nil {
		for i := range messages {
			text := strings.TrimSpace(messages[i].SearchText)
			if text == "" || len(messages[i].Embedding) > 0 {
				continue
			}
			vec, err := e.embedder.Embed(ctx, text)
			if err != nil {
				slog.ErrorContext(ctx, "session message embed failed", "error", err)
				continue
			}
			messages[i].Embedding = slices.Clone(vec)
		}
	}
	return w.SaveSessionMessages(ctx, host, sessionID, generationKey, messages)
}

// DeleteSessionMessages removes every saved message for sessionID.
// A store that cannot write returns nil.
func (e *Engine) DeleteSessionMessages(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	w, ok := e.store.(ObjectWriter)
	if !ok {
		return nil
	}
	return w.DeleteSessionMessages(ctx, sessionID)
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
	created       time.Time
	updated       time.Time
}

func (row sessionRow) object() Object {
	pos := row.position
	return Object{
		ID:         row.id,
		Kind:       KindSessionMessage,
		Title:      row.role,
		Content:    string(row.body),
		Properties: sessionProps(row),
		Position:   &pos,
		Namespace:  row.namespace.Clone(),
		Embedding:  slices.Clone(row.embedding),
		CreatedAt:  row.created,
		UpdatedAt:  row.updated,
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

// SaveSessionMessages implements ObjectWriter.
func (s *MemoryStore) SaveSessionMessages(_ context.Context, host Namespace, sessionID, generationKey string, messages []SessionMessage) (int, error) {
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
	ns := NamespaceForSession(host, sessionID)
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initSessionLocked()
	if gen, ok := s.sessionKeys[sessionID][generationKey]; ok {
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
			embedding:     slices.Clone(msg.Embedding),
			created:       now,
			updated:       now,
		}
	}
	if s.sessionKeys[sessionID] == nil {
		s.sessionKeys[sessionID] = map[string]int{}
	}
	s.sessionKeys[sessionID][generationKey] = gen
	return gen, nil
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

// DeleteSessionMessages implements ObjectWriter.
func (s *MemoryStore) DeleteSessionMessages(_ context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initSessionLocked()
	for id, row := range s.sessionMsgs {
		if row.sessionID == sessionID {
			delete(s.sessionMsgs, id)
		}
	}
	delete(s.sessionKeys, sessionID)
	return nil
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
