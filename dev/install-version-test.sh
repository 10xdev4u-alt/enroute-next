#!/bin/sh
# Checks the glibc gate in install.sh, which decides whether the Linux release
# binaries can run here. Run it from the repository root.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

# install.sh calls main on its last line, so drop that line and read the rest,
# which only defines functions.
script=$(sed '$d' "$root/install.sh")
eval "$script"

check() {
	expected=$1
	version=$2
	if glibc_at_least_2_36 "$version"; then actual=ok; else actual=old; fi
	if [ "${expected}" != "${actual}" ]; then
		echo "glibc ${version}: expected ${expected}, got ${actual}" >&2
		exit 1
	fi
	echo "glibc ${version} -> ${expected}"
}

check ok 2.36
check ok 2.37
check ok 2.40.1
check ok 3.0
check ok 3.1.4
check old 2.35
check old 2.9
check old 1.2
check old nonsense
check old ""

echo "install version gate: ok"
