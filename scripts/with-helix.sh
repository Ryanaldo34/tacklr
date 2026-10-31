#!/usr/bin/env bash
# with-helix.sh runs a command against one Helix server: it starts the
# container, sets TACKLR_HELIX_URL for every test package the command runs,
# and removes the container when the command ends.
set -euo pipefail

docker=${DOCKER:-docker}
image=${HELIX_IMAGE:-ghcr.io/helixdb/helixdb:v0.0.12}

id=$("$docker" run -d --rm -p 127.0.0.1::8080 "$image")
trap '"$docker" rm -f "$id" >/dev/null' EXIT
url="http://$("$docker" port "$id" 8080/tcp | head -1)"
for _ in $(seq 120); do
	if curl -fsS "$url/readyz" >/dev/null 2>&1; then
		TACKLR_HELIX_URL=$url "$@"
		exit
	fi
	sleep 1
done
echo "helix at $url was not ready after 120s" >&2
exit 1
