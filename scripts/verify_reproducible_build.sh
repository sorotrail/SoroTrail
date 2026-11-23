#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# verify_reproducible_build.sh — build the same commit twice in independent
# environments and assert the resulting binaries are byte-for-byte identical.
#
# Reproducibility is what makes provenance and checksums verifiable by a third
# party: if two builds of the same commit produce different bytes, a consumer
# cannot confirm that a published release binary came from the tagged source
# without trusting the builder.
#
# This script guards against the common sources of Go build non-determinism:
#   1. Working-directory path leaks (guarded by -trimpath, tested by building
#      from two temporary trees at different directory depths).
#   2. Environment leaks such as HOSTNAME, USER, HOME, TZ, and locale.
#   3. Wall-clock timestamps in ldflags (resolved by deriving BUILD_DATE from
#      the HEAD commit timestamp in UTC rather than `date -u`).
#   4. Automatic VCS status metadata variation (guarded by -buildvcs=false).
#
# Usage:
#   ./scripts/verify_reproducible_build.sh [--target <pkg>] [--inject-nondeterminism <mode>]
#
# Modes for --inject-nondeterminism (used by automated tests to prove the check
# fails loudly and explains where the binaries diverged):
#   timestamp       Stamp different BUILD_DATE values into build A and build B
#   untrimmed-path  Omit -trimpath on build B so the deeper workdir path leaks
#   hostname        Stamp the builder hostname into buildinfo.Version on build B
# ---------------------------------------------------------------------------
set -euo pipefail

TARGET_PKG="./cmd/sorotrail"
INJECT_MODE=""
SOURCE_DIR=""

usage() {
	cat >&2 <<'EOF'
usage: verify_reproducible_build.sh [--target <pkg>] [--source <dir>] [--inject-nondeterminism <timestamp|untrimmed-path|hostname>]
EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
	--target)
		shift
		TARGET_PKG="${1:-}"
		;;
	--source)
		shift
		SOURCE_DIR="${1:-}"
		;;
	--inject-nondeterminism)
		shift
		INJECT_MODE="${1:-}"
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage
		exit 2
		;;
	esac
	shift
done

if [ -z "$SOURCE_DIR" ]; then
	SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
	SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

cd "$SOURCE_DIR"

# Resolve deterministic build metadata from the commit (or explicit overrides).
VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo "")}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo "")}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct HEAD 2>/dev/null || echo "")}"
BUILD_DATE="${BUILD_DATE:-$(TZ=UTC git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD 2>/dev/null || echo "")}"

for field_name in VERSION COMMIT SOURCE_DATE_EPOCH BUILD_DATE; do
	field_val="${!field_name}"
	if [ -z "$field_val" ] || [ "$field_val" = "unknown" ]; then
		printf '::error::deterministic build metadata %s resolved to %q\n' "$field_name" "${field_val:-<empty>}" >&2
		exit 1
	fi
done

WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/sorotrail-repro.XXXXXX")"
cleanup() {
	rm -rf "$WORK_ROOT"
}
trap cleanup EXIT

# Two build roots at deliberately different filesystem depths so any path leak
# shifts both string contents and string table offsets.
DIR_A="$WORK_ROOT/alpha/src"
DIR_B="$WORK_ROOT/bravo/nested/deeper/worktree/src"
mkdir -p "$DIR_A" "$DIR_B" "$WORK_ROOT/home-a" "$WORK_ROOT/home-b" "$WORK_ROOT/tmp-a" "$WORK_ROOT/tmp-b"

# Copy tracked/working source without copying temporary build outputs.
tar -C "$SOURCE_DIR" \
	--exclude='./bin' \
	--exclude='./dist' \
	--exclude='./coverage.out' \
	-cf - . | tar -C "$DIR_A" -xf -

tar -C "$SOURCE_DIR" \
	--exclude='./bin' \
	--exclude='./dist' \
	--exclude='./coverage.out' \
	-cf - . | tar -C "$DIR_B" -xf -

BIN_A="$WORK_ROOT/sorotrail-a"
BIN_B="$WORK_ROOT/sorotrail-b"

IMPORT_PREFIX="github.com/sorotrail/sorotrail/internal/buildinfo"

VERSION_A="$VERSION"
VERSION_B="$VERSION"
BUILD_DATE_A="$BUILD_DATE"
BUILD_DATE_B="$BUILD_DATE"
FLAGS_A=(-trimpath -buildvcs=false)
FLAGS_B=(-trimpath -buildvcs=false)

case "$INJECT_MODE" in
"") ;;
timestamp)
	BUILD_DATE_B="2099-12-31T23:59:59Z"
	;;
untrimmed-path)
	FLAGS_B=(-buildvcs=false)
	;;
hostname)
	VERSION_B="${VERSION}-host-bravo"
	;;
*)
	printf 'unknown --inject-nondeterminism mode: %s\n' "$INJECT_MODE" >&2
	exit 2
	;;
esac

LDFLAGS_A="-s -w -X ${IMPORT_PREFIX}.Version=${VERSION_A} -X ${IMPORT_PREFIX}.Commit=${COMMIT} -X ${IMPORT_PREFIX}.BuildDate=${BUILD_DATE_A}"
LDFLAGS_B="-s -w -X ${IMPORT_PREFIX}.Version=${VERSION_B} -X ${IMPORT_PREFIX}.Commit=${COMMIT} -X ${IMPORT_PREFIX}.BuildDate=${BUILD_DATE_B}"

SHARED_GOMODCACHE="${GOMODCACHE:-$(go env GOMODCACHE)}"
SHARED_GOCACHE="${GOCACHE:-$(go env GOCACHE)}"

printf '[repro] building artefact A in %s (HOSTNAME=builder-alpha.sorotrail.internal, TZ=UTC)\n' "$DIR_A"
(
	cd "$DIR_A"
	env \
		HOSTNAME="builder-alpha.sorotrail.internal" \
		USER="builder-alpha" \
		HOME="$WORK_ROOT/home-a" \
		GOMODCACHE="$SHARED_GOMODCACHE" \
		GOCACHE="$SHARED_GOCACHE" \
		GOTMPDIR="$WORK_ROOT/tmp-a" \
		TZ="UTC" \
		LANG="C" \
		LC_ALL="C" \
		CGO_ENABLED="0" \
		SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" \
		go build "${FLAGS_A[@]}" -ldflags="$LDFLAGS_A" -o "$BIN_A" "$TARGET_PKG"
)

printf '[repro] building artefact B in %s (HOSTNAME=builder-bravo.prod.example.org, TZ=America/New_York)\n' "$DIR_B"
(
	cd "$DIR_B"
	env \
		HOSTNAME="builder-bravo.prod.example.org" \
		USER="builder-bravo" \
		HOME="$WORK_ROOT/home-b" \
		GOMODCACHE="$SHARED_GOMODCACHE" \
		GOCACHE="$SHARED_GOCACHE" \
		GOTMPDIR="$WORK_ROOT/tmp-b" \
		TZ="America/New_York" \
		LANG="en_US.UTF-8" \
		CGO_ENABLED="0" \
		SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" \
		go build "${FLAGS_B[@]}" -ldflags="$LDFLAGS_B" -o "$BIN_B" "$TARGET_PKG"
)

# Verify buildinfo stamps on the produced binary when building cmd/sorotrail.
if [ "$TARGET_PKG" = "./cmd/sorotrail" ]; then
	chmod +x "$BIN_A"
	version_out="$("$BIN_A" version)"
	printf '[repro] stamped version output:\n%s\n' "$version_out"
	for field in version commit build-date; do
		val="$(printf '%s\n' "$version_out" | awk -F'\t' -v f="$field" '$1==f {print $2; found=1} END {exit !found}')" || {
			printf '::error::stamped binary missing %s field in `sorotrail version` output\n' "$field" >&2
			exit 1
		}
		if [ -z "$val" ] || [ "$val" = "unknown" ]; then
			printf '::error::stamped binary field %s is %q (expected commit-derived value)\n' "$field" "${val:-<empty>}" >&2
			exit 1
		fi
	done
fi

sha256_file() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

HASH_A="$(sha256_file "$BIN_A")"
HASH_B="$(sha256_file "$BIN_B")"

if [ "$HASH_A" = "$HASH_B" ] && cmp -s "$BIN_A" "$BIN_B"; then
	printf '[PASS] reproducible build verified: sha256=%s version=%s commit=%s build-date=%s\n' \
		"$HASH_A" "$VERSION" "$COMMIT" "$BUILD_DATE"
	exit 0
fi

# ---------------------------------------------------------------------------
# Detailed diagnostic diff when reproducibility fails
# ---------------------------------------------------------------------------
printf '::error::[FAIL] reproducible build mismatch between independent build environments\n' >&2
printf '  artefact A: %s (sha256=%s, size=%s bytes, workdir=%s)\n' \
	"$BIN_A" "$HASH_A" "$(wc -c <"$BIN_A" | tr -d ' ')" "$DIR_A" >&2
printf '  artefact B: %s (sha256=%s, size=%s bytes, workdir=%s)\n' \
	"$BIN_B" "$HASH_B" "$(wc -c <"$BIN_B" | tr -d ' ')" "$DIR_B" >&2

if [ "$TARGET_PKG" = "./cmd/sorotrail" ]; then
	chmod +x "$BIN_A" "$BIN_B"
	"$BIN_A" version >"$WORK_ROOT/ver-a.txt" 2>/dev/null || true
	"$BIN_B" version >"$WORK_ROOT/ver-b.txt" 2>/dev/null || true
	if ! diff -u "$WORK_ROOT/ver-a.txt" "$WORK_ROOT/ver-b.txt" >"$WORK_ROOT/ver.diff" 2>&1; then
		printf '\n--- Stamped version output diff (sorotrail version) ---\n' >&2
		cat "$WORK_ROOT/ver.diff" >&2
	fi
fi

if command -v go >/dev/null 2>&1; then
	BUILDID_A="$(go tool buildid "$BIN_A" 2>/dev/null || echo "<unavailable>")"
	BUILDID_B="$(go tool buildid "$BIN_B" 2>/dev/null || echo "<unavailable>")"
	printf '\n--- Go buildid comparison ---\n  A: %s\n  B: %s\n' "$BUILDID_A" "$BUILDID_B" >&2

	go version -m "$BIN_A" >"$WORK_ROOT/meta-a.txt" 2>/dev/null || true
	go version -m "$BIN_B" >"$WORK_ROOT/meta-b.txt" 2>/dev/null || true
	if ! diff -u "$WORK_ROOT/meta-a.txt" "$WORK_ROOT/meta-b.txt" >"$WORK_ROOT/meta.diff" 2>&1; then
		printf '\n--- Embedded Go build info diff (go version -m) ---\n' >&2
		cat "$WORK_ROOT/meta.diff" >&2
	fi
fi

if command -v objdump >/dev/null 2>&1; then
	objdump -h "$BIN_A" | awk 'NR>3 {print $2, $3, $4}' >"$WORK_ROOT/sections-a.txt" 2>/dev/null || true
	objdump -h "$BIN_B" | awk 'NR>3 {print $2, $3, $4}' >"$WORK_ROOT/sections-b.txt" 2>/dev/null || true
	if ! diff -u "$WORK_ROOT/sections-a.txt" "$WORK_ROOT/sections-b.txt" >"$WORK_ROOT/sections.diff" 2>&1; then
		printf '\n--- Binary section header diff (objdump -h) ---\n' >&2
		cat "$WORK_ROOT/sections.diff" >&2
	fi
fi

if command -v strings >/dev/null 2>&1; then
	strings -n 6 "$BIN_A" >"$WORK_ROOT/strings-a.txt" 2>/dev/null || true
	strings -n 6 "$BIN_B" >"$WORK_ROOT/strings-b.txt" 2>/dev/null || true
	if ! diff -u "$WORK_ROOT/strings-a.txt" "$WORK_ROOT/strings-b.txt" >"$WORK_ROOT/strings.diff" 2>&1; then
		printf '\n--- Printable string table diff (first 40 lines) ---\n' >&2
		head -n 40 "$WORK_ROOT/strings.diff" >&2
	fi
fi

printf '\n--- First 20 differing byte offsets (cmp -l) ---\n' >&2
cmp -l "$BIN_A" "$BIN_B" 2>&1 | head -n 20 >&2 || true

exit 1
