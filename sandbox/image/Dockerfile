# The root file system of a sandbox microVM: Debian with the tools that
# agent scripts use, the agent user, and the worker as init. The last
# stage is the ext4 image alone.
FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0 AS root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		bash coreutils findutils grep sed gawk diffutils patch file less procps \
		git python3 jq xz-utils zip unzip ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& useradd --uid 1000 --user-group --no-create-home --home-dir /home/agent --shell /bin/bash agent \
	&& mkdir -p /workspace /home/agent
COPY tacklr-sandbox-worker /sbin/tacklr-sandbox-worker

FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0 AS image
RUN apt-get update \
	&& apt-get install -y --no-install-recommends e2fsprogs \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=root / /rootfs
RUN size=$(( $(du -sm /rootfs | cut -f1) * 5 / 4 + 64 )) \
	&& mkfs.ext4 -q -L root -d /rootfs /rootfs.ext4 "${size}M"

FROM scratch
COPY --from=image /rootfs.ext4 /
