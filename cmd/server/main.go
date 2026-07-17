package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/inference"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/telemetry"
)

const defaultSystemPrompt = "You are a helpful assistant with access to a get_weather tool. When asked about the weather, use the tool to look it up before responding."

func main() {
	cfg := loadServerConfig()
	level := slog.LevelInfo
	switch strings.ToLower(cfg.logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	var store stores.BaseStore = stores.NewInMemoryStore()
	if cfg.supabaseDBURL != "" {
		conn, err := pgx.Connect(context.Background(), cfg.supabaseDBURL)
		if err != nil {
			slog.Warn("failed to connect to supabase", "error", err)
		} else {
			store = stores.NewPostgresStore(conn)
			slog.Info("connected to supabase database")
		}
	}

	strategy := inference.NewOpenAIInferenceStrategy(http.DefaultClient).
		WithURL(cfg.openAIBaseURL).
		WithModel(cfg.openAIModel).
		WithApiKey(cfg.openAIAPIKey)

	var streamer tacklr.StreamingStrategy
	if cfg.agentStreamingStrategy == "buffered" {
		streamer = streaming.New()
	}

	registry := server.NewRegistry()
	registry.Register("default", server.AgentSpec{
		Config: tacklr.Config{
			MaxWindowSize: cfg.agentMaxWindowSize,
			SystemPrompt:  defaultSystemPrompt,
		},
		Model:             strategy,
		WatchDog:          telemetry.New(),
		StreamingStrategy: streamer,
		Store:             store,
	})

	srv := server.New(store, registry)

	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.port),
		Handler: srv.Handler(),
	}
	slog.Info("starting server", "port", cfg.port)
	if err := s.ListenAndServe(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
