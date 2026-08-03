package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain"
	session "github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/streaming"
)

const brainToolTimeout = 30 * time.Second

// brainTools closes over the engine and session for knowledge builtins.
type brainTools struct {
	engine *brain.Engine
	sm     *session.SessionManager
}

func (b brainTools) scopeFromSession() brain.Scope {
	if id, ok := b.sm.SearchNamespace(); ok {
		cp := id
		return brain.Scope{Namespace: &cp}
	}
	return brain.Scope{}
}

type readObjectArgs struct {
	ObjectID string `json:"object_id" desc:"UUID of the object to read in full."`
}

const readObjectToolDescription = `Read the full contents of a knowledge-base object by id.

Use after search, find_exact, or expand when you need the complete body (content) of a known object. Pass the object UUID from a prior rich result. Do not invent ids.`

func (b brainTools) newReadObjectTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read Object",
		Description: readObjectToolDescription,
		Category:    streaming.ToolCategoryRead,
		Access:      ToolReadAccess,
		Timeout:     brainToolTimeout,
		Handler: func(ctx context.Context, args readObjectArgs, runtime HarnessRuntime) (string, error) {
			return b.runReadObject(ctx, args, runtime)
		},
	})
}

func (b brainTools) runReadObject(ctx context.Context, args readObjectArgs, runtime HarnessRuntime) (string, error) {
	if b.engine == nil {
		return "", fmt.Errorf("read: brain engine is not configured")
	}
	id, err := parseObjectID(args.ObjectID)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	runtime.EmitUpdate("Reading knowledge object…")
	obj, err := b.engine.Read(ctx, b.scopeFromSession(), id)
	if err != nil {
		if errors.Is(err, brain.ErrNotFound) {
			return "", fmt.Errorf("read: object %s not found", id)
		}
		return "", fmt.Errorf("read: %w", err)
	}
	return formatBrainJSON(obj)
}

type schemaArgs struct {
	Kind string `json:"kind,omitempty" desc:"Optional object kind to describe (e.g. Document, Chunk). Omit to list all registered kinds."`
}

const schemaToolDescription = `Discover structured filter fields and kind documentation for the knowledge base.

Call with a kind to see filterable fields, types, and operators for that kind. Call with no kind to list registered kinds. Prefer this before inventing property names in filters.`

func (b brainTools) newSchemaTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "schema",
		DisplayName: "Knowledge Schema",
		Description: schemaToolDescription,
		Category:    streaming.ToolCategoryThink,
		Access:      ToolReadAccess,
		Timeout:     brainToolTimeout,
		Handler: func(ctx context.Context, args schemaArgs, runtime HarnessRuntime) (string, error) {
			return b.runSchema(ctx, args, runtime)
		},
	})
}

func (b brainTools) runSchema(ctx context.Context, args schemaArgs, runtime HarnessRuntime) (string, error) {
	if b.engine == nil {
		return "", fmt.Errorf("schema: brain engine is not configured")
	}
	runtime.EmitUpdate("Loading knowledge schema…")
	res, err := b.engine.Schema(ctx, args.Kind)
	if err != nil {
		if errors.Is(err, brain.ErrNotFound) {
			return "", fmt.Errorf("schema: kind %q not found", strings.TrimSpace(args.Kind))
		}
		return "", fmt.Errorf("schema: %w", err)
	}
	return formatBrainJSON(res)
}

func formatBrainJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("brain tool: marshal result: %w", err)
	}
	return string(b), nil
}

func parseObjectID(raw string) (uuid.UUID, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return uuid.Nil, fmt.Errorf("object_id is required")
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("object_id must be a UUID: %w", err)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("object_id must not be the nil UUID")
	}
	return id, nil
}

func newBrainTools(engine *brain.Engine, sm *session.SessionManager) []*Tool {
	if engine == nil || sm == nil {
		return nil
	}
	b := brainTools{engine: engine, sm: sm}
	return []*Tool{
		b.newReadObjectTool(),
		b.newSchemaTool(),
	}
}
