#!/usr/bin/env bash
# Print a markdown coverage summary from a go coverprofile.
# Usage: scripts/coverage-summary.sh [coverage.out]
set -euo pipefail

PROFILE="${1:-coverage.out}"
if [[ ! -f "$PROFILE" ]]; then
  echo "coverage profile not found: $PROFILE" >&2
  exit 1
fi

# Keep in sync with exclude.paths in .testcoverage.yml / update-coverage-badge.sh
EXCLUDE_REGEX='cmd/|internal/testkit|internal/agentbench|internal/temporallive|telemetry/otel\.go'

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

go tool cover -func="$PROFILE" >"$tmp"

# Statement total with same excludes as CI badge (library surface, not mains/bench).
# -coverpkg=./... writes each block once per tested package. Merge by taking
# the max hit count per block so the summary matches go-test-coverage.
total="$(
  awk -v excl="$EXCLUDE_REGEX" '
    NR == 1 { next }
    {
      loc = $1
      n = split(loc, a, ":")
      file = a[1]
      if (file ~ excl) next
      stmts = $(NF-1) + 0
      count = $NF + 0
      if (!(loc in seen) || count > cnt[loc]) {
        seen[loc] = 1
        cnt[loc] = count
        st[loc] = stmts
      }
    }
    END {
      covered = 0; total = 0
      for (k in st) {
        total += st[k]
        if (cnt[k] > 0) covered += st[k]
      }
      if (total == 0) { print "0.0%"; exit }
      printf "%.1f%%", 100.0 * covered / total
    }
  ' "$PROFILE"
)"

echo "### Coverage summary"
echo
echo "| Package | Coverage |"
echo "|---------|----------|"

# Aggregate per package (directory path under module); skip excluded paths.
awk -v excl="$EXCLUDE_REGEX" '
  /^total:/ { next }
  {
    n = split($1, a, ":")
    path = a[1]
    if (path ~ excl) next
    sub(/\/[^\/]+\.go$/, "", path)
    sub(/^github.com\/ryanaldo34\/tacklr\/?/, "", path)
    if (path == "" || path == "github.com/ryanaldo34/tacklr") path = "(root)"
    pct = $3
    gsub(/%/, "", pct)
    sum[path] += pct
    cnt[path]++
  }
  END {
    for (p in sum) {
      avg = sum[p] / cnt[p]
      printf "%s\t%.1f\n", p, avg
    }
  }
' "$tmp" | sort | while IFS=$'\t' read -r pkg pct; do
  printf "| \`%s\` | %s%% |\n" "$pkg" "$pct"
done

echo
echo "**Total (library statements, excludes cmd/testkit/agentbench/temporallive/otel):** ${total}"
