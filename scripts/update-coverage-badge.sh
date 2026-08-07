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
EXCLUDE_REGEX='cmd/|internal/testkit|internal/agentbench|stores/postgres\.go'

pct="$(
  awk -v excl="$EXCLUDE_REGEX" '
    BEGIN { covered = 0; total = 0 }
    NR == 1 { next }
    {
      # file:line.col,line.col numStmt count
      n = split($1, a, ":")
      file = a[1]
      if (file ~ excl) next
      stmts = $(NF-1) + 0
      count = $NF + 0
      total += stmts
      if (count > 0) covered += stmts
    }
    END {
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
