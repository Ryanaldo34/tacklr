.PHONY: test vet lint fmt cover coverage check lgtm-up lgtm-down lgtm-logs lgtm-testserver testserver

# Race-enabled tests (same as CI).
test:
	go test -race -count=1 ./...

vet:
	go vet ./...

# Format all packages (gofmt + goimports if available).
fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/ryanaldo34/tacklr . || true

# Static analysis via golangci-lint (config: .golangci.yml).
lint:
	golangci-lint run ./...

# Generate coverage.out + HTML report (atomic mode; no -race — matches CI coverage step).
cover:
	go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"

# Enforce package/total thresholds from .testcoverage.yml (same gate as CI).
coverage: cover
	@go run github.com/vladopajic/go-test-coverage/v2@latest --config=./.testcoverage.yml
	@./scripts/coverage-summary.sh coverage.out
	@./scripts/update-coverage-badge.sh coverage.out docs/badges/coverage.json

# Full local gate matching CI (format + lint + vet + race tests + coverage thresholds).
check: vet lint test coverage
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

# --- Local LGTM (Apple container runtime + grafana/otel-lgtm) ---
# Pulls Loki/Grafana/Tempo/Prometheus(+Mimir path)/Pyroscope + OTel Collector in one container.
lgtm-up:
	@chmod +x scripts/lgtm-up.sh scripts/lgtm-down.sh scripts/lgtm-testserver.sh
	./scripts/lgtm-up.sh

lgtm-down:
	@chmod +x scripts/lgtm-down.sh
	./scripts/lgtm-down.sh

lgtm-logs:
	@container logs -f $${LGTM_CONTAINER_NAME:-tacklr-lgtm}

# ACP testserver with OTLP defaults aimed at make lgtm-up.
lgtm-testserver:
	@chmod +x scripts/lgtm-testserver.sh
	./scripts/lgtm-testserver.sh

# Testserver without forcing LGTM env (still reads OTEL_* from the environment / .env).
testserver:
	go run ./cmd/testserver
