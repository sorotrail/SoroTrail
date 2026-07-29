#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# check_env_sync.sh — CI lint: ensure every env tag in the Config struct has a
# matching entry in .env.example.
#
# Usage:
#   scripts/check_env_sync.sh
#
# Exit code: 0 when all variables are documented, 1 when some are missing.
# ---------------------------------------------------------------------------
set -euo pipefail

CONFIG_FILE="internal/config/config.go"
ENV_EXAMPLE=".env.example"

# Extract env variable names from `env:"VAR_NAME"` tags in the Config struct.
# Grep for lines matching `env:"VAR_NAME"` and capture VAR_NAME.
mapfile -t CODE_VARS < <(grep -oP 'env:"\K[^"]+' "$CONFIG_FILE")

# Read variable names from .env.example — lines matching `^VAR_NAME=` or
# `^# VAR_NAME=` (commented defaults).
mapfile -t EXAMPLE_VARS < <(grep -oP '^#?\s*\K[A-Z][A-Z0-9_]+(?==)' "$ENV_EXAMPLE" || true)

EXIT_CODE=0
for var in "${CODE_VARS[@]}"; do
    found=0
    for ev in "${EXAMPLE_VARS[@]}"; do
        if [[ "$var" == "$ev" ]]; then
            found=1
            break
        fi
    done
    if [[ $found -eq 0 ]]; then
        echo "[FAIL] $var is read by code but missing from $ENV_EXAMPLE"
        EXIT_CODE=1
    fi
done

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[PASS] All env vars in Config struct are documented in $ENV_EXAMPLE"
fi
exit $EXIT_CODE
