#!/bin/sh
# Builds the example's Go program for js/wasm and copies Go's wasm_exec.js
# next to it, into build/ (not committed).
set -eu
cd "$(dirname "$0")"
mkdir -p build
GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o build/tacklr.wasm ..
install -m 0644 "$(go env GOROOT)/lib/wasm/wasm_exec.js" build/wasm_exec.js
