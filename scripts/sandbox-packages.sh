#!/bin/sh
# Downloads the WASIX packages pinned in sandbox/worker/packages/packages.txt
# from the Wasmer CDN and checks each one against its SHA-256.
set -eu
dir=$(dirname "$0")/../sandbox/worker/packages
while read -r name version hash; do
	file="$dir/$(basename "$name").webc"
	if [ -f "$file" ] && echo "$hash  $file" | sha256sum -c --quiet 2>/dev/null; then
		continue
	fi
	curl -sSfLo "$file.part" "https://cdn.wasmer.io/webcimages/$hash.webc"
	echo "$hash  $file.part" | sha256sum -c --quiet
	mv "$file.part" "$file"
	echo "$name $version"
done < "$dir/packages.txt"
