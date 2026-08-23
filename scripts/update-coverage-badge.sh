#!/usr/bin/env bash
# Write a shields.io endpoint JSON badge from a go coverprofile.
# Applies the same path excludes as .testcoverage.yml so the badge matches CI gates.
#
# Usage: scripts/update-coverage-badge.sh [coverage.out] [docs/badges/coverage.json]
set -euo pipefail

PROFILE="${1:-coverage.out}"
OUT="${2:-docs/badges/coverage.json}"

if [[ ! -f "$PROFILE" ]]; then
  echo "coverage profile not found: $PROFILE" >&2
  exit 1
fi

# Keep in sync with exclude.paths in .testcoverage.yml
EXCLUDE_REGEX='cmd/|internal/testkit|internal/agentbench|internal/temporallive|stores/postgres\.go'

# -coverpkg=./... writes each block once per tested package. Merge by taking
# the max hit count per block so the badge matches go-test-coverage.
pct="$(
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
      if (total == 0) { print "0.0"; exit }
      printf "%.1f", 100.0 * covered / total
    }
  ' "$PROFILE"
)"

color="red"
if awk -v t="$pct" 'BEGIN{exit !(t+0 >= 95)}'; then
  color="brightgreen"
elif awk -v t="$pct" 'BEGIN{exit !(t+0 >= 90)}'; then
  color="green"
elif awk -v t="$pct" 'BEGIN{exit !(t+0 >= 80)}'; then
  color="yellow"
fi

mkdir -p "$(dirname "$OUT")"
cat >"$OUT" <<EOF
{
  "schemaVersion": 1,
  "label": "coverage",
  "message": "${pct}%",
  "color": "${color}"
}
EOF

echo "wrote $OUT (${pct}% / $color)"
