# Requires Go 1.27 (go.mod).
.PHONY: test vet lint fmt cover coverage check wasm

# The harness core builds for WebAssembly (GOOS=js GOARCH=wasm) so a host can
# run it outside a Linux worker, for example in a Cloudflare Durable Object.
WASM_CORE = . ./message ./session ./session/loop ./brain ./skills ./vfs/mount ./telemetry ./openai ./web ./server ./server/acp ./session/durableobject

# The whole suite. It needs Linux, FUSE and Docker: Testcontainers starts
# Postgres (pgvector), Helix and Temporal. -covermode=atomic is required with -race.
test:
	go test -race -count=1 -covermode=atomic ./...

vet:
	go vet ./...

wasm:
	GOOS=js GOARCH=wasm go build $(WASM_CORE)

# Format all packages (gofmt + goimports if available).
fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/ryanaldo34/tacklr . || true

# Static analysis via golangci-lint (config: .golangci.yml).
lint:
	golangci-lint run ./...

# Profile + threshold gate (writes coverage.out for tools; no HTML).
cover:
	go test -race -count=1 -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
	@go run github.com/vladopajic/go-test-coverage/v2@latest --config=./.testcoverage.yml
	@./scripts/coverage-summary.sh coverage.out
	@./scripts/update-coverage-badge.sh coverage.out docs/badges/coverage.json

# Alias for cover (CI-style thresholds).
coverage: cover

# Full local gate: format check + lint + vet + race tests + coverage thresholds.
check: vet wasm lint cover
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

