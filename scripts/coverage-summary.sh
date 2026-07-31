#!/usr/bin/env bash
# Print a markdown coverage summary from a go coverprofile.
# Usage: scripts/coverage-summary.sh [coverage.out]
set -euo pipefail

PROFILE="${1:-coverage.out}"
if [[ ! -f "$PROFILE" ]]; then
  echo "coverage profile not found: $PROFILE" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

go tool cover -func="$PROFILE" >"$tmp"

total="$(awk '/^total:/{print $3}' "$tmp")"

echo "### Coverage summary"
echo
echo "| Package | Coverage |"
echo "|---------|----------|"

# Aggregate per package (directory path under module).
awk '
  /^total:/ { next }
  {
    # file path: github.com/ryanaldo34/tacklr/.../file.go:line:func
    n = split($1, a, ":")
    path = a[1]
    sub(/\/[^\/]+\.go$/, "", path)
    # strip module prefix for readability
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
echo "**Total (statements):** ${total}"
