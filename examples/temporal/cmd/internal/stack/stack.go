// Package stack opens what the example's two programs share, from the
// environment: the Temporal connection, the Postgres pool and the session
// stores on it, the model client, and the key of the workers' live
// endpoints. The host and the worker must agree on all of it.
package stack

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	sessionpg "github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
)

// Stack is the shared part of both programs. The session events go to the
// Postgres EventLog (DisableStreams on both halves), so they need no
// Temporal feature beyond workflows and activities.
type Stack struct {
	Temporal  client.Client
	Pool      *pgxpool.Pool
	Store     session.Store
	Secrets   session.SecretStorage
	Events    *sessionpg.EventLog
	Model     *openai.Model
	TaskQueue string
	LiveKey   []byte
}

// Open reads the environment:
//
//	DATABASE_URL      Postgres (required)
//	SEAL_KEY          32 bytes in base64: seals users' tokens (required)
//	LIVE_KEY          base64: signs live delta streams (required)
//	OPENAI_API_KEY    the model's key (required)
//	MODEL             the model id (required)
//	OPENAI_BASE_URL   an OpenAI-compatible endpoint (optional)
//	TEMPORAL_ADDRESS  default localhost:7233
//	TASK_QUEUE        default tacklr
func Open(ctx context.Context) (*Stack, error) {
	seal, err := base64.StdEncoding.DecodeString(os.Getenv("SEAL_KEY"))
	if err != nil {
		return nil, fmt.Errorf("SEAL_KEY: %w", err)
	}
	sealer, err := session.NewAESGCM(seal)
	if err != nil {
		return nil, fmt.Errorf("SEAL_KEY: %w", err)
	}
	live, err := base64.StdEncoding.DecodeString(os.Getenv("LIVE_KEY"))
	if err != nil || len(live) == 0 {
		return nil, errors.New("LIVE_KEY must be set, in base64")
	}
	if os.Getenv("OPENAI_API_KEY") == "" || os.Getenv("MODEL") == "" {
		return nil, errors.New("OPENAI_API_KEY and MODEL must be set")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL: %w", err)
	}
	db, err := sessionpg.New(pool, sessionpg.Options{})
	if err == nil {
		err = db.Setup(ctx)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	c, err := temporal.Dial(client.Options{HostPort: cmp.Or(os.Getenv("TEMPORAL_ADDRESS"), "localhost:7233")})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("temporal: %w", err)
	}
	model := openai.New(&http.Client{Timeout: 5 * time.Minute}).WithApiKey(os.Getenv("OPENAI_API_KEY")).WithModel(os.Getenv("MODEL"))
	if url := os.Getenv("OPENAI_BASE_URL"); url != "" {
		model = model.WithURL(url)
	}
	return &Stack{
		Temporal:  c,
		Pool:      pool,
		Store:     db.Store(),
		Secrets:   db.Secrets(sealer),
		Events:    db.EventLog(),
		Model:     model,
		TaskQueue: cmp.Or(os.Getenv("TASK_QUEUE"), "tacklr"),
		LiveKey:   live,
	}, nil
}

// Close releases the connections.
func (s *Stack) Close() {
	s.Events.Close()
	s.Temporal.Close()
	s.Pool.Close()
}
