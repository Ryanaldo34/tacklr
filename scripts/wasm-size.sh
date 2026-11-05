#!/bin/sh
# Fails when a Worker's Go program is over its budget. Workers allow 64 MiB
# uncompressed; the budget is lower so growth shows long before the limit.
set -eu
file=${1:?usage: wasm-size.sh FILE.wasm}
budget=$((24 * 1024 * 1024))
size=$(wc -c <"$file" | tr -d ' ')
gz=$(gzip -c "$file" | wc -c | tr -d ' ')
echo "$file: $size bytes ($gz gzipped); budget $budget bytes"
if [ "$size" -gt "$budget" ]; then
	echo "over the 24 MiB budget" >&2
	exit 1
fi
