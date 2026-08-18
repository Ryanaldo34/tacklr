// Command testserver is a local ACP harness for exercising Tacklr’s built-in
// agent tooling (plan/todos, ask_user_choice, web_search/web_fetch when EXA_API_KEY is set,
// skills when configured, VFS at /work). Default transport is stdio; pass --http for HTTP/WS.
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
	"github.com/ryanaldo34/tacklr/vfs"
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
		WithModel(os.Getenv("OPENAI_MODEL")).
		WithLocalTokenFallback()

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

	// Optional host skill directories (colon-separated, like PATH). Each
	// directory is a union member; the agent sees one /skills mount.
	var skillHostDirs []string
	if raw := strings.TrimSpace(os.Getenv("SKILL_DIRECTORIES")); raw != "" {
		for _, p := range strings.Split(raw, string(os.PathListSeparator)) {
			p = strings.TrimSpace(p)
			if p != "" {
				skillHostDirs = append(skillHostDirs, p)
			}
		}
	}

	// Host tools intentionally empty: the harness injects plan builtins,
	// ask_user_choice, web_search/web_fetch (when EXA_API_KEY is set), and
	// VFS tools when FSRegistry + FSBootstrap are set (Registry owns the MountSession).
	exaKey := strings.TrimSpace(os.Getenv("EXA_API_KEY"))

	// Local VFS: virtual /work → host jail /tmp/tacklr. Registry starts FUSE.
	const vfsJail = "/tmp/tacklr"
	if err := os.MkdirAll(vfsJail, 0o750); err != nil {
		slog.Error("vfs mkdir failed", "path", vfsJail, "error", err)
		os.Exit(1)
	}
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: vfsJail, Skills: "skills"}); err != nil {
		slog.Error("vfs register failed", "error", err)
		os.Exit(1)
	}
	vfsAuth := vfs.NewSessionAuth()
	if err := fsReg.Register(vfs.DriveFactory{ID: "gdrive", Auth: vfsAuth}); err != nil {
		slog.Error("vfs gdrive register failed", "error", err)
		os.Exit(1)
	}
	for i, hostDir := range skillHostDirs {
		info, err := os.Stat(hostDir)
		if err != nil || !info.IsDir() {
			slog.Error("skill directory missing", "path", hostDir, "error", err)
			os.Exit(1)
		}
		id := fmt.Sprintf("skills%d", i+1)
		if err := fsReg.Register(vfs.LocalFactory{ID: id, Base: hostDir, Skills: "."}); err != nil {
			slog.Error("vfs skills register failed", "path", hostDir, "error", err)
			os.Exit(1)
		}
	}

	defaultAgent := "test-agent"
	var vfsOpts []server.RegistryOption
	vfsOpts = append(vfsOpts, server.WithVFSAuth(vfsAuth))
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("TACKLR_VFS_DIRECT"))); v == "1" || v == "true" {
		vfsOpts = append(vfsOpts, server.WithVFSProjection(server.DirectProjection{}))
		slog.Info("vfs projection", "mode", "direct")
	}
	reg := server.NewRegistry(store, defaultAgent, vfsOpts...)
	reg.Register(defaultAgent, server.AgentSpec{
		Name: "Tacklr",
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize: maxWindow,
				// Empty: rely on harness Adaptive Case Management system prompt only.
				SystemPrompt: "",
			},
			Model:     model,
			ExaAPIKey: exaKey,
		},
		FSRegistry:  fsReg,
		FSBootstrap: []vfs.MountSpec{{Point: "/work", Profile: "local"}},
	})

	slog.Info("harness showcase",
		"max_window_size", maxWindow,
		"skill_dirs", len(skillHostDirs),
		"web_tools", exaKey != "",
		"host_tools", 0,
		"vfs_mount", "/work",
		"vfs_jail", vfsJail,
	)

	// Process-local ACP (memory wire store). For durable session/load across
	// restarts: server.NewACPServerPostgres(reg, conn).
	srv := server.NewACPServer(reg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Default: stdio (Zed / CLI ACP). Opt in to HTTP with --http.
	if len(os.Args) > 1 && os.Args[1] == "--http" {
		srv.AllowAnonymousNetwork()
		port := 3000
		if p := os.Getenv("PORT"); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
		addr := fmt.Sprintf(":%d", port)
		slog.Info("starting acp test server",
			"mode", "http",
			"addr", addr,
			"websocket", fmt.Sprintf("ws://localhost:%d/acp", port),
			"streamable_http", fmt.Sprintf("http://localhost:%d/acp", port),
			"sse", "POST /",
		)
		if err := srv.ServeHTTP(ctx, addr); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
		return
	}

	slog.Info("starting acp test server", "mode", "stdio")
	if err := srv.ServeStdio(ctx, os.Stdin, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("stdio mode error", "error", err)
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
