#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# check_lint_toolchain.sh — fail fast when go.mod targets a Go version newer
# than the pinned golangci-lint can analyse.
#
# golangci-lint is compiled with one Go version and refuses to start when the
# repository targets a newer one:
#
#   the Go language version (go1.26) used to build golangci-lint is lower than
#   the targeted Go version (1.27.1)
#
# The "targeted" version is go.mod's `toolchain` directive when it has one and
# its `go` directive otherwise. A `toolchain` line is easy to acquire by
# accident — any `go get` / `go mod tidy` run under a newer local toolchain can
# add one — and because the pinned *CI* binary only ever runs in CI, a
# contributor can see a green `make lint` locally while the lint job fails. This
# guard compares go.mod against the Go version recorded for the pinned release in
# .golangci-lint-version, so the mismatch is caught before the push. It needs no
# network and does not depend on which linter the contributor happens to have
# installed.
#
# Usage:
#   scripts/check_lint_toolchain.sh [--root DIR] [--verify-binary]
#   scripts/check_lint_toolchain.sh --print-version
#
# --verify-binary additionally checks that an installed golangci-lint matching
# the pinned version reports the Go version recorded in .golangci-lint-version,
# so the record cannot rot. It skips (rather than fails) when no matching binary
# is on PATH.
#
# Exit code: 0 when compatible, 1 on drift or a malformed pin, with the fix
# spelled out in the failure message.
# ---------------------------------------------------------------------------
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$script_dir/.." && pwd)"
verify_binary=0
print_version=0

while [ $# -gt 0 ]; do
    case "$1" in
        --root)
            [ $# -ge 2 ] || { echo "[FAIL] --root needs a directory" >&2; exit 1; }
            root="$2"
            shift 2
            ;;
        --verify-binary)
            verify_binary=1
            shift
            ;;
        --print-version)
            print_version=1
            shift
            ;;
        -h|--help)
            sed -n '2,40p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            echo "[FAIL] unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

pin_file="$root/.golangci-lint-version"
gomod_file="$root/go.mod"

if [ ! -f "$pin_file" ]; then
    echo "[FAIL] $pin_file is missing; it records the pinned golangci-lint release and the Go version it was built with." >&2
    exit 1
fi

# The first non-comment, non-blank line is "<version> <go-built-with>".
pin_line="$(grep -vE '^[[:space:]]*(#|$)' "$pin_file" | head -n1 || true)"
lint_version="$(printf '%s\n' "$pin_line" | awk '{print $1}')"
lint_go="$(printf '%s\n' "$pin_line" | awk '{print $2}')"

if [ -z "$lint_version" ] || [ -z "$lint_go" ]; then
    echo "[FAIL] $pin_file must hold '<version> <go-built-with>' on one line, e.g. 'v2.12.2 go1.26.2'." >&2
    exit 1
fi

if [ "$print_version" -eq 1 ]; then
    printf '%s\n' "$lint_version"
    exit 0
fi

# minor_of normalises "go1.26.2" or "1.25.0" to a sortable major*1000+minor so
# two versions can be compared as integers. Patch releases within a minor share
# a language version, which is exactly the granularity golangci-lint compares.
minor_of() {
    local v="${1#go}"
    local major="${v%%.*}"
    local rest="${v#*.}"
    local minor="${rest%%.*}"
    case "${major}${minor}" in
        ''|*[!0-9]*)
            echo "[FAIL] cannot parse Go version '$1'" >&2
            return 1
            ;;
    esac
    printf '%s' "$(( major * 1000 + minor ))"
}

if [ ! -f "$gomod_file" ]; then
    echo "[FAIL] $gomod_file not found (expected the repository root at $root)." >&2
    exit 1
fi

go_directive="$(awk '$1 == "go" { print $2; exit }' "$gomod_file")"
toolchain_directive="$(awk '$1 == "toolchain" { print $2; exit }' "$gomod_file")"

if [ -z "$go_directive" ] && [ -z "$toolchain_directive" ]; then
    echo "[FAIL] $gomod_file has neither a 'go' nor a 'toolchain' directive." >&2
    exit 1
fi

# golangci-lint follows the toolchain when one is set, so the effective target is
# the higher of the two directives.
target=""
target_key=0
for v in "$go_directive" "$toolchain_directive"; do
    [ -n "$v" ] || continue
    key="$(minor_of "$v")"
    if [ "$key" -gt "$target_key" ]; then
        target_key="$key"
        target="$v"
    fi
done
lint_key="$(minor_of "$lint_go")"

if [ "$target_key" -gt "$lint_key" ]; then
    directives="go ${go_directive:-<unset>}"
    [ -n "$toolchain_directive" ] && directives="$directives, toolchain $toolchain_directive"
    # "1.27" — the language version the linter compares, without the patch.
    target_minor="$(( target_key / 1000 )).$(( target_key % 1000 ))"
    lint_minor="$(( lint_key / 1000 )).$(( lint_key % 1000 ))"
    cat >&2 <<EOF
[FAIL] go.mod targets Go ${target_minor} but the pinned golangci-lint ${lint_version} was built with ${lint_go}.

golangci-lint cannot analyse code whose go.mod targets a newer Go than the one it
was compiled with, so the lint job would fail with:

  the Go language version (go${lint_minor}) used to build golangci-lint is lower than the targeted Go version (${target#go})

go.mod directives: ${directives}.

How to fix:
  * Unintended bump? A newer local Go toolchain adds a 'toolchain go...' line to
    go.mod as a side effect of 'go get' / 'go mod tidy'. If the project is not
    moving to Go ${target_minor} yet, delete that line and keep the 'go' directive
    as it was.
  * Intentional bump? Move the pin too: set .golangci-lint-version to a
    golangci-lint release built with Go >= ${target_minor}, update the Go version
    recorded beside it, then run 'make lint' again.

See CONTRIBUTING.md#lint-toolchain-drift.
EOF
    exit 1
fi

if [ "$verify_binary" -eq 1 ]; then
    bin="$(command -v golangci-lint || true)"
    if [ -z "$bin" ]; then
        echo "[SKIP] golangci-lint is not on PATH; cannot verify the Go version recorded in $pin_file."
    else
        actual_version="$("$bin" --version | sed -n 's/.*version[[:space:]]\{1,\}\([0-9][^ ]*\).*/\1/p' | head -n1)"
        actual_go="$("$bin" --version | sed -n 's/.*built with \([^ ]*\).*/\1/p' | head -n1)"
        if [ "v${actual_version}" != "$lint_version" ]; then
            echo "[SKIP] installed golangci-lint is ${actual_version:-unknown}, not the pinned ${lint_version}; skipping the record check."
        elif [ "$(minor_of "$actual_go")" -ne "$lint_key" ]; then
            echo "[FAIL] $pin_file records ${lint_go} for ${lint_version}, but that binary was built with ${actual_go}." >&2
            echo "       Update the recorded Go version in $pin_file." >&2
            exit 1
        else
            echo "[PASS] ${lint_version} was built with ${actual_go}, matching $pin_file."
        fi
    fi
fi

echo "[PASS] go.mod targets Go ${target#go}; pinned golangci-lint ${lint_version} (built with ${lint_go}) supports it."
