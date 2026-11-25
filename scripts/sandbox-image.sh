#!/bin/sh
# Builds the guest image of the sandbox in sandbox/image/out: Firecracker,
# the guest kernel (Linux with Firecracker's guest configuration and the 9P
# options of sandbox/image/kernel.config), and the root file system
# (sandbox/image/Dockerfile, with the worker, sandbox/worker, as init). Each download is checked against its SHA-256. A part whose
# inputs did not change is not built again.
set -eu

firecracker_version=v1.18.0
linux_version=6.18.65
linux_sha=c92606f956f09dc50b5a0e2af9e86763fce27ada1647de97f8d1ac8cfc95801d

arch=$(uname -m)
case "$arch" in
x86_64)
	firecracker_sha=1ac8eec866dd5eea14efecf6323e22b23eea24054545338a5b34bc5bca6623cd
	config_sha=ba22401a0c7292a4c024ebcd10a562d4a1f1bfd2faed671406d3b159c0cf5215
	kernel_target=vmlinux
	kernel_image=vmlinux
	;;
aarch64)
	firecracker_sha=2a6130ff0338901ff9d7fbd73ce046c6b1c6123889dc84a34d678aa53bd0c0c2
	config_sha=35cce8e8b754a84523ca20dc068fc3f8f03a15523be36a8623bad54e11972e2a
	kernel_target=Image
	kernel_image=arch/arm64/boot/Image
	;;
*)
	echo "sandbox-image: $arch is not supported" >&2
	exit 1
	;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
image=$root/sandbox/image
out=$image/out
work=$out/build
mkdir -p "$work"

# fetch URL FILE SHA downloads URL to FILE unless FILE has SHA.
fetch() {
	if [ -f "$2" ] && echo "$3  $2" | sha256sum -c --quiet 2>/dev/null; then
		return
	fi
	curl -sSfLo "$2.part" "$1"
	echo "$3  $2.part" | sha256sum -c --quiet
	mv "$2.part" "$2"
}

# fresh NAME STAMP is true when out/NAME was built from STAMP.
fresh() {
	[ -f "$out/$1" ] && [ "$(cat "$out/$1.stamp" 2>/dev/null)" = "$2" ]
}

stamp="$firecracker_version $arch"
if ! fresh firecracker "$stamp"; then
	tarball=$work/firecracker-$firecracker_version-$arch.tgz
	fetch "https://github.com/firecracker-microvm/firecracker/releases/download/$firecracker_version/firecracker-$firecracker_version-$arch.tgz" "$tarball" "$firecracker_sha"
	tar -xzf "$tarball" -C "$work"
	cp "$work/release-$firecracker_version-$arch/firecracker-$firecracker_version-$arch" "$out/firecracker"
	echo "$stamp" >"$out/firecracker.stamp"
	echo "sandbox-image: firecracker $firecracker_version"
fi

stamp="$linux_version $firecracker_version $arch $(sha256sum "$image/kernel.config" | cut -c1-64)"
if ! fresh vmlinux "$stamp"; then
	tarball=$work/linux-$linux_version.tar.xz
	fetch "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$linux_version.tar.xz" "$tarball" "$linux_sha"
	base=$work/microvm-kernel-$arch-6.18.config
	fetch "https://raw.githubusercontent.com/firecracker-microvm/firecracker/$firecracker_version/resources/guest_configs/microvm-kernel-ci-$arch-6.18.config" "$base" "$config_sha"
	rm -rf "$work/linux-$linux_version"
	tar -xJf "$tarball" -C "$work"
	cd "$work/linux-$linux_version"
	cp "$base" .config
	scripts/kconfig/merge_config.sh -m .config "$image/kernel.config" >/dev/null
	make -s olddefconfig
	while read -r option; do
		grep -qx "$option" .config || {
			echo "sandbox-image: the kernel configuration lost $option" >&2
			exit 1
		}
	done <"$image/kernel.config"
	make -s -j"$(nproc)" "$kernel_target"
	cp "$kernel_image" "$out/vmlinux"
	cd "$root"
	rm -rf "$work/linux-$linux_version"
	echo "$stamp" >"$out/vmlinux.stamp"
	echo "sandbox-image: linux $linux_version"
fi

worker=$work/tacklr-sandbox-worker
(cd "$root" && CGO_ENABLED=0 go build -trimpath -o "$worker" ./sandbox/worker/cmd/tacklr-sandbox-worker)
stamp="$(sha256sum "$image/Dockerfile" "$worker" | cut -c1-64 | tr '\n' ' ')"
if ! fresh rootfs.ext4 "$stamp"; then
	context=$work/rootfs
	rm -rf "$context"
	mkdir -p "$context"
	cp "$image/Dockerfile" "$worker" "$context/"
	tag=tacklr-sandbox-rootfs:build
	docker build -q -t "$tag" "$context" >/dev/null
	container=$(docker create "$tag" /rootfs.ext4)
	docker cp "$container:/rootfs.ext4" "$out/rootfs.ext4"
	docker rm "$container" >/dev/null
	docker rmi "$tag" >/dev/null
	echo "$stamp" >"$out/rootfs.ext4.stamp"
	echo "sandbox-image: rootfs.ext4"
fi
