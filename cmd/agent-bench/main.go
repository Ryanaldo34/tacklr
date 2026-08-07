// Command agent-bench drives a real tacklr agent through multi-turn scenarios
// aligned with industry agent/memory/tool evaluation shapes.
//
// Environment (same as cmd/testserver):
//
//	OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL — required unless -dry-run
//	OPENAI_EMBEDDING_MODEL — dense model (default text-embedding-3-small); same base URL/key
//	EXA_API_KEY — optional; web_augmented cases skip when unset
//
// Hybrid retrieval: seed Puts and agent saves embed via OpenAI-compatible /embeddings
// unless -lexical-only is set.
//
// Example:
//
//	go run ./cmd/agent-bench -suite all -out /tmp/bench.json
//	go run ./cmd/agent-bench -dry-run -list
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/internal/agentbench"
)

func main() {
	suite := flag.String("suite", "all", "suite id or all: "+strings.Join(agentbench.AllSuites, ","))
	caseID := flag.String("case", "", "run a single case id")
	outPath := flag.String("out", "", "write JSON scorecard to this path")
	timeout := flag.Duration("timeout", 4*time.Minute, "per-case timeout")
	dryRun := flag.Bool("dry-run", false, "validate/register cases without calling a model")
	list := flag.Bool("list", false, "list cases and exit")
	embedModel := flag.String("embed-model", "", "embedding model (default OPENAI_EMBEDDING_MODEL or text-embedding-3-small)")
	lexicalOnly := flag.Bool("lexical-only", false, "disable dense channel (no embeddings)")
	flag.Parse()

	if *list {
		agentbench.ListCases(os.Stdout, *suite)
		return
	}

	embModel := strings.TrimSpace(*embedModel)
	if embModel == "" {
		embModel = strings.TrimSpace(os.Getenv("OPENAI_EMBEDDING_MODEL"))
	}

	cfg := agentbench.Config{
		CaseFilter:  strings.TrimSpace(*caseID),
		ModelURL:    strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")),
		ModelAPIKey: strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		ModelName:   strings.TrimSpace(os.Getenv("OPENAI_MODEL")),
		EmbedModel:  embModel,
		LexicalOnly: *lexicalOnly,
		ExaAPIKey:   strings.TrimSpace(os.Getenv("EXA_API_KEY")),
		Timeout:     *timeout,
		DryRun:      *dryRun,
	}
	if *suite == "all" {
		cfg.Suites = agentbench.AllSuites
	} else {
		cfg.Suites = []string{*suite}
	}

	if !*dryRun {
		if cfg.ModelAPIKey == "" || cfg.ModelName == "" {
			fmt.Fprintln(os.Stderr, "agent-bench: OPENAI_API_KEY and OPENAI_MODEL are required (or pass -dry-run)")
			os.Exit(2)
		}
		if cfg.ModelURL == "" {
			cfg.ModelURL = "https://api.openai.com/v1"
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rep, err := agentbench.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent-bench: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(agentbench.FormatMarkdown(rep))

	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "write out: %v\n", err)
			os.Exit(1)
		}
		if err := agentbench.WriteJSON(f, rep); err != nil {
			_ = f.Close()
			fmt.Fprintf(os.Stderr, "encode out: %v\n", err)
			os.Exit(1)
		}
		_ = f.Close()
		fmt.Fprintf(os.Stderr, "wrote %s\n", *outPath)
	}

	if !*dryRun && !rep.GatesOK {
		os.Exit(1)
	}
}
