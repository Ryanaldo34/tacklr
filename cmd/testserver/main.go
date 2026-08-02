// Command testserver is a local ACP harness for exercising Tacklr’s built-in
// agent tooling (plan/todos, ask_user_choice, web_search/web_fetch when EXA_API_KEY is set,
// skills when configured) over HTTP or stdio — not a product demo with toy tools.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/inference"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func main() {
	// Stderr first so early loadDotEnv / Init failures are visible; after Init we
	// dual-write to OTLP logs when a collector is configured.
	baseLog := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(telemetry.NewLogger(baseLog))
	loadDotEnv(".env")
	if d := execDir(); d != "" {
		loadDotEnv(filepath.Join(filepath.Dir(d), ".env"))
	}
	// Default OTLP endpoint for local collectors; set OTEL_SDK_DISABLED=true to off.
	applyDefaultOTELEnv()

	// OTLP traces + metrics + logs (default localhost:4317 / grpc).
	// ACP HTTP on PORT (default 3000).
	serviceName := envOr("OTEL_SERVICE_NAME", "tacklr-testserver")
	otelShutdown, err := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName: serviceName,
		Insecure:    true,
	})
	if err != nil {
		slog.Error("otel init failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = otelShutdown(context.Background()) }()

	if disabledOTEL() {
		// Keep stderr-only when OTEL is explicitly off.
		slog.SetDefault(telemetry.NewLogger(baseLog))
		slog.Info("otel exporters disabled (OTEL_SDK_DISABLED=true)")
	} else {
		// Dual-write slog → stderr + OTLP logs.
		telemetry.InstallDefaultWithOTLP(baseLog, telemetry.NewOTLPSlogHandler(serviceName))
		slog.Info("otel exporters enabled",
			"endpoint", envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
			"protocol", envOr("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
			"service", serviceName,
			"signals", "traces,metrics,logs",
		)
	}

	store := stores.NewInMemoryStore()

	model := inference.NewOpenAIInferenceStrategy(&http.Client{
		Timeout: 120 * time.Second,
	})
	model.WithURL(os.Getenv("OPENAI_BASE_URL")).
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	// Reasoning models (GPT Luna, o-series, …) only stream client-visible thought
	// when the Responses request asks for a summary. Without this, Foundry may
	// still emit reasoning items (needed for multi-turn tool pairing) but no
	// reasoning_summary_text.delta — so Zed never gets agent_thought_chunk.
	// Defaults: summary=auto. Override with OPENAI_REASONING_SUMMARY; set
	// OPENAI_REASONING_EFFORT for effort (also implies summary=auto when unset).
	if effort := strings.TrimSpace(os.Getenv("OPENAI_REASONING_EFFORT")); effort != "" {
		model.WithReasoningLevel(effort)
	}
	if summary := strings.TrimSpace(os.Getenv("OPENAI_REASONING_SUMMARY")); summary != "" {
		model.WithReasoningSummary(summary)
	} else if strings.TrimSpace(os.Getenv("OPENAI_REASONING_EFFORT")) == "" {
		model.WithReasoningSummary("auto")
	}
	// Avoid bare response.incomplete after large tool turns (web_search + plan).
	// Override with MAX_OUTPUT_TOKENS; default high enough for reasoning summaries.
	maxOut := 32_768
	if v := strings.TrimSpace(os.Getenv("MAX_OUTPUT_TOKENS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxOut = n
		}
	}
	if maxOut > 0 {
		model.WithMaxOutputTokens(maxOut)
	}

	// Context window budget (tokens) for pressure/compress.
	maxWindow := 1_000_000
	if v := strings.TrimSpace(os.Getenv("MAX_WINDOW_SIZE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxWindow = n
		}
	}

	// Optional skills directories (colon-separated, like PATH).
	var skillDirs []string
	if raw := strings.TrimSpace(os.Getenv("SKILL_DIRECTORIES")); raw != "" {
		for _, p := range strings.Split(raw, string(os.PathListSeparator)) {
			p = strings.TrimSpace(p)
			if p != "" {
				skillDirs = append(skillDirs, p)
			}
		}
	}

	// Host tools intentionally empty: the harness injects plan builtins,
	// ask_user_choice, and web_search/web_fetch (when EXA_API_KEY / ExaAPIKey is set).
	exaKey := strings.TrimSpace(os.Getenv("EXA_API_KEY"))

	defaultAgent := "test-agent"
	reg := server.NewRegistry(store, defaultAgent)
	reg.Register(defaultAgent, server.AgentSpec{
		Name: "Tacklr",
		Config: tacklr.Config{
			MaxWindowSize: maxWindow,
			// Empty: rely on harness Adaptive Case Management system prompt only.
			SystemPrompt:     "",
			SkillDirectories: skillDirs,
		},
		Model:     model,
		Tools:     nil,
		ExaAPIKey: exaKey,
	})

	slog.Info("harness showcase",
		"max_window_size", maxWindow,
		"skill_dirs", len(skillDirs),
		"web_tools", exaKey != "",
		"host_tools", 0,
	)

	srv := server.NewServer(reg, server.ACP)

	if len(os.Args) > 1 && os.Args[1] == "--stdio" {
		slog.Info("starting acp test server", "mode", "stdio")
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := srv.ServeStdio(ctx, os.Stdin, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("stdio mode error", "error", err)
			os.Exit(1)
		}
		return
	}

	port := 3000
	if p := os.Getenv("PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}

	addr := fmt.Sprintf(":%d", port)
	slog.Info("starting acp test server", "addr", addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.ServeHTTP(ctx, addr); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// execDir returns the directory containing the running binary.
func execDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func disabledOTEL() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")))
	return v == "true" || v == "1"
}

// applyDefaultOTELEnv fills unset OTEL_* vars so a local collector can be used
// without extra agent env (e.g. Zed ACP). Override via environment or .env.
func applyDefaultOTELEnv() {
	if disabledOTEL() {
		// Explicit disable: clear endpoint so telemetry.Init installs no-ops.
		_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
		return
	}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" {
		_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")) == "" {
		_ = os.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	}
	if strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")) == "" {
		_ = os.Setenv("OTEL_SERVICE_NAME", "tacklr-testserver")
	}
}

// loadDotEnv reads a .env file and sets each KEY=VALUE pair as an environment
// variable. Existing variables are not overwritten.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		slog.Debug("no .env file", "path", path)
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}
