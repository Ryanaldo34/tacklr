.PHONY: test test-short brain-pg-image helix-image require-docker vet lint fmt cover coverage check testserver

# Docker CLI is often missing from PATH when Docker Desktop is installed via the
# app bundle only. Prefer PATH, then the standard macOS Desktop location.
DOCKER ?= $(shell command -v docker 2>/dev/null || \
	([ -x /Applications/Docker.app/Contents/Resources/bin/docker ] && \
		echo /Applications/Docker.app/Contents/Resources/bin/docker) || \
	([ -x "$$HOME/.docker/bin/docker" ] && echo "$$HOME/.docker/bin/docker") || \
	true)

# Race detector + per-package coverage in test output. Requires Docker for brain
# Postgres + Helix integration tests. -covermode=atomic is required with -race.
test: brain-pg-image helix-image
	go test -race -count=1 -covermode=atomic ./...

# Fast loop without Testcontainers / live backends (still race + coverage).
test-short:
	go test -short -race -count=1 -covermode=atomic ./...

# pgvector + pg_textsearch image used by brain PostgresStore integration tests.
brain-pg-image: require-docker
	$(DOCKER) build -f brain/testdata/Dockerfile.postgres -t tacklr-pg-brain:test brain/testdata

# Helix enterprise-dev (in-memory) for helixgraph integration tests.
# https://docs.helix-db.com/database/local-development
helix-image: require-docker
	$(DOCKER) pull ghcr.io/helixdb/enterprise-dev:latest

require-docker:
	@if [ -z "$(DOCKER)" ]; then \
		echo "docker CLI not found. Install Docker Desktop (or Colima) and either:"; \
		echo "  - ensure 'docker' is on PATH, or"; \
		echo "  - open Docker Desktop so /Applications/Docker.app/.../bin/docker exists"; \
		echo "  - or: make DOCKER=/path/to/docker <target>"; \
		exit 1; \
	fi

vet:
	go vet ./...

# Format all packages (gofmt + goimports if available).
fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/ryanaldo34/tacklr . || true

# Static analysis via golangci-lint (config: .golangci.yml).
lint:
	golangci-lint run ./...

# Profile + threshold gate (writes coverage.out for tools; no HTML).
cover: brain-pg-image helix-image
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@go run github.com/vladopajic/go-test-coverage/v2@latest --config=./.testcoverage.yml
	@./scripts/coverage-summary.sh coverage.out
	@./scripts/update-coverage-badge.sh coverage.out docs/badges/coverage.json

# Alias for cover (CI-style thresholds).
coverage: cover

# Full local gate: format check + lint + vet + race tests + coverage thresholds.
check: vet lint cover
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

# Local ACP harness (reads OTEL_* / .env; see cmd/testserver).
testserver:
	go run ./cmd/testserver

# Multi-turn agent harness benchmarks (OPENAI_*; optional EXA_API_KEY). See cmd/agent-bench.
agent-bench:
	go run ./cmd/agent-bench -suite all

agent-bench-dry:
	go run ./cmd/agent-bench -dry-run -list
