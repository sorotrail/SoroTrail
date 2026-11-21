# Configuration Constraints

Every runtime parameter is loaded from the process environment at boot via
`Load()`. `Load()` does not read a `.env` file itself. A `.env` file can be
provided to the process by the shell, Docker Compose, or another orchestrator;
the resulting process environment is what SoroTrail reads. When using a shell,
source `.env` before exporting any values that should override it. Docker
Compose uses its documented variable-interpolation precedence, with explicitly
exported shell variables taking precedence over values from the local `.env`.

All checks run collectively through `Config.MustValidate()` so no single missing
field halts the process mid-flight; every problem is printed before `os.Exit(1)`.

## Required
| Variable       | Rule                        |
|----------------|-----------------------------|
| `DATABASE_URL` | Must be a non-empty string. |

`NETWORK` selects `testnet` (the default), `mainnet`, or `futurenet`. When
`RPC_URL` is not set, the selected network supplies its standard Soroban RPC
endpoint and Stellar network passphrase. Ingestion state and persisted events
are keyed by this network, so changing networks does not reuse cursors.

`RPC_URL` sets a custom Soroban RPC endpoint URL. When not set, defaults to the standard RPC endpoint corresponding to the configured `NETWORK`. Example: `RPC_URL=https://soroban-testnet.stellar.org`.

## URL format
| Variable   | Rule                                 |
|------------|--------------------------------------|
| `RPC_URL`  | Must be a valid absolute URL with scheme and host. |

`RPC_URL` specifies the Soroban RPC endpoint used for ledger and event ingestion. When unset, SoroTrail defaults to the public RPC URL for the selected `NETWORK`. Default: unset (uses selected network's standard RPC). Example: `RPC_URL=https://soroban-testnet.stellar.org:443`.

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
| `INGEST_PAGE_SIZE`          | > 0                     |
| `INGEST_BATCH_SIZE`         | > 0                     |
| `AUDIT_BATCH_LEDGERS`       | > 0                     |
| `AUDIT_LAG_THRESHOLD`       | > 0                     |
| `AUDIT_BUDGET_SHARE`        | [0, 1]                  |
| `AUDIT_MAX_RPS`             | > 0                     |
| `AUDIT_MAX_REPAIR_ATTEMPTS` | > 0                     |
| `AUDIT_FINDING_MAX_LEDGERS` | > 0                     |
| `RPC_RATE_LIMIT`            | > 0                     |
| `RATE_LIMIT_RPS`            | >= 0                    |
| `RATE_LIMIT_BURST`          | >= 0                    |

`PARTITION_LEDGER_SPAN` sets the ledger span per events-table partition (~7 days at 5s/ledger). Partitions are created automatically on migration and at ingest time. Default: `120960`. Example: `PARTITION_LEDGER_SPAN=120960`.

## Allowed values
| Variable     | Allowed values                   |
|--------------|----------------------------------|
| `LOG_LEVEL`  | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `text`, `json`                   |

`LOG_LEVEL` sets the logging verbosity level (`debug`, `info`, `warn`, or `error`). Hot-reloadable at runtime via SIGHUP. Default: `info`. Example: `LOG_LEVEL=debug`.

`LOG_FORMAT` selects the structured log output format (`text` or `json`). Default: `text`. Example: `LOG_FORMAT=json`.

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
