# Requires Go 1.27 (go.mod).
.PHONY: test vet lint fmt cover coverage check wasm vet-wasm cloudflare-e2e deadcode

# The harness core builds for WebAssembly (GOOS=js GOARCH=wasm) so a host can
# run it outside a Linux worker, for example in a Cloudflare Durable Object.
WASM_CORE = . ./message ./session ./session/orchestrator ./brain ./skills ./vfs/mount ./telemetry ./telemetry/otlpjson ./openai ./anthropic ./web ./server ./server/acp ./session/durableobject

# The whole suite. It needs Linux, FUSE and Docker: Testcontainers starts
# Postgres (pgvector), Helix and Temporal. -covermode=atomic is required with -race.
test:
	go test -race -count=1 -covermode=atomic ./...

vet:
	go vet ./...

# The Cloudflare bridge and its example are the only packages with a build
# tag (js && wasm), so the Linux targets skip them.
WASM_TAGGED = ./session/durableobject/cloudflare ./examples/cloudflare

wasm:
	GOOS=js GOARCH=wasm go build $(WASM_CORE) $(WASM_TAGGED)

vet-wasm:
	GOOS=js GOARCH=wasm go vet $(WASM_TAGGED)
	GOOS=js GOARCH=wasm golangci-lint run $(WASM_TAGGED)

# The example Worker in workerd (wrangler dev) against a scripted model,
# then the size gate on its program. Needs Bun and Node.
cloudflare-e2e:
	cd examples/cloudflare/worker && bun install --frozen-lockfile && bun test e2e
	./scripts/wasm-size.sh examples/cloudflare/worker/build/tacklr.wasm

# Format all packages (gofmt + goimports if available).
fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/ryanaldo34/tacklr . || true

# Functions nothing reaches: not the tests, not a main. Any finding fails.
# The Linux packages use the tests as roots; the js/wasm bridge uses the
# example Worker's main.
# The tool is built for this machine first: GOOS=js on `go run` would build
# the tool itself for js.
DEADCODE = $(CURDIR)/.bin/deadcode
deadcode:
	@GOBIN=$(CURDIR)/.bin go install golang.org/x/tools/cmd/deadcode@v0.52.0
	@out=$$($(DEADCODE) -test ./...; GOOS=js GOARCH=wasm $(DEADCODE) -filter 'session/durableobject/cloudflare|examples/cloudflare' ./examples/cloudflare); \
	if [ -n "$$out" ]; then echo "$$out"; echo "unreachable code: delete it or cover it"; exit 1; fi

# Static analysis via golangci-lint (config: .golangci.yml).
lint:
	golangci-lint run ./...

# Profile + threshold gate (writes coverage.out for tools; no HTML).
cover:
	go test -race -count=1 -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
	@go run github.com/vladopajic/go-test-coverage/v2@v2.21.0 --config=./.testcoverage.yml
	@./scripts/coverage-summary.sh coverage.out
	@./scripts/update-coverage-badge.sh coverage.out docs/badges/coverage.json

# Alias for cover (CI-style thresholds).
coverage: cover

# Full local gate: format check + lint + vet + race tests + coverage thresholds.
check: vet wasm vet-wasm lint deadcode cover
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

