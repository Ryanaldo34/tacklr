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
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/anthropic"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	sessionpg "github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/telemetry/export"
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
	Model     tacklr.Model
	TaskQueue string
	LiveKey   []byte
	// flush sends what telemetry holds and stops it.
	flush func(context.Context) error
}

// Open reads the environment:
//
//	DATABASE_URL      Postgres (required)
//	SEAL_KEY          32 bytes in base64: seals users' tokens (required)
//	LIVE_KEY          base64: signs live delta streams (required)
//	MODEL             the model id (required)
//	ANTHROPIC_API_KEY the key of a Claude model, or
//	OPENAI_API_KEY    the key of an OpenAI-compatible model (one is required)
//	ANTHROPIC_BASE_URL, OPENAI_BASE_URL
//	                  the model's endpoint (optional)
//	TEMPORAL_ADDRESS  default localhost:7233
//	TASK_QUEUE        default tacklr
//	OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL
//	                  where traces, metrics and logs go (optional)
//
// service names the program in its telemetry. The program's slog records
// go to stderr and, with the trace and span of their context, to the
// OpenTelemetry logs.
func Open(ctx context.Context, service string) (*Stack, error) {
	// Telemetry first: the Temporal client and the stores pick up its
	// providers.
	flush, err := export.Init(ctx, export.Config{ServiceName: service})
	if err != nil {
		return nil, err
	}
	slog.SetDefault(slog.New(slog.NewMultiHandler(slog.NewTextHandler(os.Stderr, nil), export.SlogHandler())))
	st, err := open(ctx)
	if err != nil {
		_ = flush(ctx)
		return nil, err
	}
	st.flush = flush
	return st, nil
}

func open(ctx context.Context) (*Stack, error) {
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
	model, err := newModel()
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL: %w", err)
	}
	// Each statement is a span of the turn that runs it.
	sessionpg.Configure(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
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
	c, err := temporal.Dial(client.Options{
		HostPort: cmp.Or(os.Getenv("TEMPORAL_ADDRESS"), "localhost:7233"),
		Logger:   temporallog.NewStructuredLogger(slog.Default()),
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("temporal: %w", err)
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

// Close releases the connections, and sends what telemetry holds.
func (s *Stack) Close() {
	s.Events.Close()
	s.Temporal.Close()
	s.Pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.flush(ctx)
}

// newModel is the model the environment names: a Claude model with
// ANTHROPIC_API_KEY, else an OpenAI-compatible one with OPENAI_API_KEY.
func newModel() (tacklr.Model, error) {
	id := os.Getenv("MODEL")
	httpClient := &http.Client{Timeout: 5 * time.Minute}
	switch {
	case id == "":
		return nil, errors.New("MODEL must be set")
	case os.Getenv("ANTHROPIC_API_KEY") != "":
		m := anthropic.New(httpClient).WithApiKey(os.Getenv("ANTHROPIC_API_KEY")).WithModel(id)
		if url := os.Getenv("ANTHROPIC_BASE_URL"); url != "" {
			m = m.WithURL(url)
		}
		return m, nil
	case os.Getenv("OPENAI_API_KEY") != "":
		m := openai.New(httpClient).WithApiKey(os.Getenv("OPENAI_API_KEY")).WithModel(id)
		if url := os.Getenv("OPENAI_BASE_URL"); url != "" {
			m = m.WithURL(url)
		}
		return m, nil
	}
	return nil, errors.New("ANTHROPIC_API_KEY or OPENAI_API_KEY must be set")
}
