package main

import (
	"bufio"
	"context"
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
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	loadDotEnv(".env")
	if d := execDir(); d != "" {
		loadDotEnv(filepath.Join(filepath.Dir(d), ".env"))
	}

	defaultAgent := "test-agent"

	store := stores.NewInMemoryStore()

	model := inference.NewOpenAIInferenceStrategy(&http.Client{
		Timeout: 120 * time.Second,
	})
	model.WithURL(os.Getenv("OPENAI_BASE_URL")).
		WithApiKey(os.Getenv("OPENAI_API_KEY")).
		WithModel(os.Getenv("OPENAI_MODEL"))

	// Azure OpenAI / Foundry reasoning models need reasoning.effort + summary
	// so thought text streams as reasoning_summary_text (→ agent_thought_chunk).
	if effort := os.Getenv("OPENAI_REASONING_EFFORT"); effort != "" {
		model.WithReasoningLevel(effort)
	}
	if summary := os.Getenv("OPENAI_REASONING_SUMMARY"); summary != "" {
		model.WithReasoningSummary(summary)
	}

	model.SetSystemPrompt(defaultSystemPrompt)

	tools := []*tacklr.Tool{echoTool, getTimeTool, progressTool}

	reg := server.NewRegistry(store, defaultAgent)
	reg.Register(defaultAgent, server.AgentSpec{
		Name: "Tacklr Test Agent",
		Config: tacklr.Config{
			MaxWindowSize: 128000,
			SystemPrompt:  defaultSystemPrompt,
		},
		Model: model,
		Tools: tools,
	})

	srv := server.NewServer(reg, server.ACP)

	if len(os.Args) > 1 && os.Args[1] == "--stdio" {
		slog.Info("starting acp test server", "mode", "stdio")
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := srv.ServeStdio(ctx, os.Stdin, os.Stdout); err != nil && err != context.Canceled {
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
	if err := srv.ServeHTTP(ctx, addr); err != nil && err != context.Canceled {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

var echoTool = tacklr.NewTool(tacklr.ToolConfig{
	Name:        "echo",
	Description: "Echoes back whatever message you send. Use this to verify tool calling works.",
	Handler: func(ctx context.Context, args struct {
		Message string `json:"message" desc:"Message to echo back"`
	}) (string, error) {
		return args.Message, nil
	},
})

var getTimeTool = tacklr.NewTool(tacklr.ToolConfig{
	Name:        "get_time",
	Description: "Returns the current date and time. Use this when you need to know what time it is.",
	Handler: func(ctx context.Context) (string, error) {
		return time.Now().Format(time.RFC1123), nil
	},
})

var progressTool = tacklr.NewTool(tacklr.ToolConfig{
	Name:        "progress_demo",
	Description: "Demonstrates in-progress streaming updates by emitting progress messages during execution. Use this to verify that tool_call_update events are streamed correctly.",
	Handler: func(ctx context.Context, _ struct{}, runtime *tacklr.HarnessRuntime) (string, error) {
		runtime.EmitUpdate("starting work...")
		runtime.EmitUpdate("processing...")
		runtime.EmitUpdate("almost done")
		return "task complete!", nil
	},
})

var defaultSystemPrompt = strings.TrimSpace(`You are a helpful assistant running in an ACP test harness.
You have access to tools that let you echo messages, check the current time, and demonstrate progress streaming.
Use the echo tool when asked to repeat or echo something.
Use the get_time tool when asked about the current date or time.
Use the progress_demo tool when asked to demonstrate progress updates or tool_call_update streaming.`)

// execDir returns the directory containing the running binary.
func execDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
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
