# Requires Go 1.27 (go.mod).
.PHONY: test vet lint fmt cover coverage check wasm vet-wasm cloudflare-e2e deadcode rust-fmt-check rust-lint rust-test rust-cover sandbox-packages sandbox-worker

# The harness core builds for WebAssembly (GOOS=js GOARCH=wasm) so a host can
# run it outside a Linux worker, for example in a Cloudflare Durable Object.
WASM_CORE = . ./message ./session ./session/orchestrator ./brain ./skills ./vfs/mount ./telemetry ./telemetry/otlpjson ./openai ./anthropic ./web ./server ./server/acp ./session/durableobject

# The whole suite. It needs Linux, Docker and cargo: Testcontainers starts
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

# Format all packages (gofmt + goimports if available) and the sandbox worker.
fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/ryanaldo34/tacklr . || true
	cd $(WORKER) && cargo fmt

# The sandbox worker (Rust). cargo runs in the crate directory, so rustup
# uses the crate's rust-toolchain.toml. The Go sandbox tests build the
# worker themselves (make sandbox-worker).
WORKER = sandbox/worker

# The WASIX packages (bash, coreutils, grep, sed, findutils) pinned by
# SHA-256 in sandbox/worker/packages/packages.txt, compiled ahead of time
# for this CPU architecture when the pins or the Wasmer version change.
PACKAGES = $(WORKER)/packages
sandbox-packages:
	./scripts/sandbox-packages.sh
	@$(MAKE) --no-print-directory $(PACKAGES)/compiled/.stamp

$(PACKAGES)/compiled/.stamp: $(PACKAGES)/packages.txt $(WORKER)/Cargo.lock
	rm -rf $(PACKAGES)/compiled
	cd $(WORKER) && cargo run --locked --release --features compile --bin tacklr-sandbox-compile -- packages
	touch $@

# The release worker binary and its compiled packages, as the Go sandbox
# tests run them.
sandbox-worker: sandbox-packages
	cd $(WORKER) && cargo build --locked --release

rust-fmt-check:
	cd $(WORKER) && cargo fmt --check

rust-lint: sandbox-packages
	cd $(WORKER) && cargo clippy --locked --all-targets -- -D warnings
	cd $(WORKER) && cargo clippy --locked --all-targets --all-features -- -D warnings

rust-test: sandbox-packages
	cd $(WORKER) && cargo test --locked

# jail.rs and main.rs run only in sandboxed processes, and Landlock stops
# those processes from writing coverage data. tests/jail.rs tests each
# sandbox layer in a child process, and the Go supervisor tests run the
# whole worker. Every other line must be covered.
rust-cover: sandbox-packages
	cd $(WORKER) && cargo llvm-cov --locked --ignore-filename-regex '(jail|main)\.rs$$' --fail-under-lines 100

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
check: vet wasm vet-wasm lint deadcode rust-fmt-check rust-lint rust-cover cover
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

