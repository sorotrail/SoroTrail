# Configuration Constraints

Every runtime parameter is loaded from environment variables at boot via `Load()`.
All checks run collectively through `Config.MustValidate()` so no single missing
field halts the process mid-flight; every problem is printed before `os.Exit(1)`.

## Required
| Variable       | Rule                        |
|----------------|-----------------------------|
| `DATABASE_URL` | Must be a non-empty string. |

## URL format
| Variable   | Rule                                 |
|------------|--------------------------------------|
| `RPC_URL`  | Must be a valid absolute URL with scheme and host. |

## Duration format
| Variable              | Rule                                        |
|-----------------------|---------------------------------------------|
| `POLL_INTERVAL`       | Must be a positive Go duration (e.g. `5s`). |
| `AUDIT_POLL_INTERVAL` | Must be a positive Go duration (e.g. `30s`).|

## Numeric ranges
| Variable                    | Rule                    |
|-----------------------------|-------------------------|
| `RETENTION_LEDGERS`         | > 0                     |
| `PARTITION_LEDGER_SPAN`     | > 0                     |
| `AUDIT_BATCH_LEDGERS`       | > 0                     |
| `AUDIT_LAG_THRESHOLD`       | > 0                     |
| `AUDIT_BUDGET_SHARE`        | [0, 1]                  |
| `AUDIT_MAX_RPS`             | > 0                     |
| `AUDIT_MAX_REPAIR_ATTEMPTS` | > 0                     |
| `AUDIT_FINDING_MAX_LEDGERS` | > 0                     |
| `RATE_LIMIT_RPS`            | >= 0                    |
| `RATE_LIMIT_BURST`          | >= 0                    |

## Allowed values
| Variable    | Allowed values               |
|-------------|------------------------------|
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` |

## Mutual dependency
- `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST`: must both be set or both be unset
  (zero). Setting only one silently disables the rate limiter, which is
  almost certainly an operator mistake.

## Contract ID format
`WATCHED_CONTRACTS` entries must be valid Soroban strkeys (C-prefix, 56 base32
characters). Only the shape is checked; the checksum is not verified.

## Redaction
Sensitive variables (`DATABASE_URL`) have their credentials redacted in
validation error output so secrets are never echoed to logs.
