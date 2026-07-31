.PHONY: test vet lint fmt cover coverage check

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
