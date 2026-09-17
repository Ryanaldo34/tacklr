package tacklr

import (
	"context"
	"log/slog"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const (
	KindEpisode      = "Episode"
	KindEpisodeChunk = "EpisodeChunk"

	triggerHandoff    = "handoff"
	triggerCompress   = "compress"
	triggerSpecialist = "specialist"
	maxDiscardedMsgs  = 20
	maxDiscardedBytes = 32 << 10
)

// EpisodeKinds are SDK-owned kinds for collapse residue. Hosts with a non-empty
// catalog should pass these to store.Setup / ApplyKinds (same pattern as
// vfsindex.MountIndexKinds).
func EpisodeKinds() []brain.KindSpec {
	return []brain.KindSpec{
		{
			Kind:        KindEpisode,
			Description: "Notes kept from earlier work in this session. Not a curated durable record.",
			IsParent:    true,
			Fields: []brain.FieldSpec{
				{Name: "trigger", Type: brain.FieldTypeString},
				{Name: "session_id", Type: brain.FieldTypeString},
			},
		},
		{
			Kind:        KindEpisodeChunk,
			Description: "Searchable part of an Episode",
			IsPart:      true,
		},
	}
}

type collapseEvent struct {
	Trigger   string
	Body      string
	Discarded []*Message
}

func (a *TurnManager) retainCollapse(ctx context.Context, ev collapseEvent) {
	if a == nil || a.brain == nil {
		return
	}
	body := strings.TrimSpace(ev.Body)
	if body == "" && len(ev.Discarded) == 0 {
		return
	}
	ns, _ := a.session.Search.Namespace()
	ns = ns.Clone()
	if sid := a.sessionId; sid != "" && !strings.Contains(sid, ".") {
		ns = append(ns, brain.Attr{Name: "session", Value: sid})
	}
	scope := brain.Scope{Namespace: ns}
	id := episodeID(a.sessionId, ev.Trigger, body)
	obj := brain.Object{
		ID:        id,
		Kind:      KindEpisode,
		Title:     ev.Trigger,
		Summary:   ev.Trigger,
		Content:   body,
		Namespace: ns,
		Properties: map[string]any{
			"trigger":    ev.Trigger,
			"session_id": a.sessionId,
		},
	}
	if _, err := a.brain.Put(ctx, scope, obj); err != nil {
		slog.ErrorContext(ctx, "collapse retain put failed", "area", telemetry.AreaModelTasks, "error", err)
		return
	}
	parts := episodeParts(ns, body, ev.Discarded)
	if len(parts) == 0 {
		return
	}
	if err := a.brain.ReplaceParts(ctx, scope, id, parts); err != nil {
		slog.ErrorContext(ctx, "collapse retain parts failed", "area", telemetry.AreaModelTasks, "error", err)
	}
}

func episodeID(session, trigger, body string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("tacklr.episode:"+session+":"+trigger+":"+body))
}

func episodeParts(ns brain.Namespace, body string, discarded []*Message) []brain.Object {
	out := make([]brain.Object, 0, 8)
	for _, chunk := range splitEpisodeBody(body) {
		out = append(out, brain.Object{
			Kind:      KindEpisodeChunk,
			Title:     chunk.title,
			Content:   chunk.body,
			Namespace: ns,
		})
	}
	n, nbytes := 0, 0
	for _, m := range discarded {
		if m == nil {
			continue
		}
		c := strings.TrimSpace(m.Content)
		if c == "" {
			continue
		}
		if n >= maxDiscardedMsgs {
			break
		}
		if nbytes+len(c) > maxDiscardedBytes {
			remain := maxDiscardedBytes - nbytes
			if remain <= 0 {
				break
			}
			c = c[:remain]
		}
		out = append(out, brain.Object{
			Kind:      KindEpisodeChunk,
			Title:     string(m.Role),
			Content:   c,
			Namespace: ns,
		})
		n++
		nbytes += len(c)
		if nbytes >= maxDiscardedBytes {
			break
		}
	}
	return out
}

type episodeChunk struct {
	title, body string
}

var episodeHeads = []string{
	"Objective:",
	"Completed Work:",
	"Key Decisions:",
	"State Changes:",
	"Discoveries:",
	"Constraints:",
	"Remaining Work:",
	"Validation:",
	"Relevant Context for Remaining Todos:",
}

func splitEpisodeBody(body string) []episodeChunk {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	type cut struct {
		i    int
		name string
		head string
	}
	cuts := make([]cut, 0, len(episodeHeads))
	for _, h := range episodeHeads {
		i := strings.Index(body, h)
		if i < 0 {
			continue
		}
		cuts = append(cuts, cut{i: i, name: strings.TrimSuffix(h, ":"), head: h})
	}
	if len(cuts) == 0 {
		return []episodeChunk{{title: "episode", body: body}}
	}
	slices.SortFunc(cuts, func(a, b cut) int { return a.i - b.i })
	out := make([]episodeChunk, 0, len(cuts))
	for i, c := range cuts {
		start := c.i + len(c.head)
		end := len(body)
		if i+1 < len(cuts) {
			end = cuts[i+1].i
		}
		text := strings.TrimSpace(body[start:end])
		if text == "" {
			continue
		}
		out = append(out, episodeChunk{title: c.name, body: text})
	}
	if len(out) == 0 {
		return []episodeChunk{{title: "episode", body: body}}
	}
	return out
}

func handoffBodyFromWindow(window []*Message) string {
	start := protectedPrefixLen(window)
	if start >= len(window) || window[start] == nil {
		return ""
	}
	return window[start].Content
}
