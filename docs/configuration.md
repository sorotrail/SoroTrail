# Configuration Reference

SoroTrail is configured via environment variables. Below is the complete reference of all supported variables.

## Environment Variables

| Variable | Description | Default | Required |
| --- | --- | --- | --- |
| `RPC_URL` | Stellar RPC endpoint URL used for network queries and ledger ingestion | — | **Yes** |
| `DATABASE_URL` | PostgreSQL connection string for event storage and state | — | **Yes** |
| `PORT` | HTTP server listening port | `8080` | No |
| `LOG_LEVEL` | Logging verbosity (`debug`, `info`, `warn`, `error`) | `info` | No |
| `POLL_INTERVAL_MAX` | Upper bound for adaptive poll interval when the chain is idle | unset | No |

### RPC_URL

The `RPC_URL` environment variable points SoroTrail to a running Stellar RPC or Soroban RPC provider. SoroTrail uses this endpoint to stream ledgers, inspect contract specifications, and verify transaction metadata.

### POLL_INTERVAL_MAX

The `POLL_INTERVAL_MAX` environment variable sets the upper bound for the adaptive poll interval. Once caught up with the chain head, the ingester dynamically increases its poll sleep interval toward `POLL_INTERVAL_MAX` on idle cycles (when no new events are found), preventing unnecessary RPC queries against quiet networks.

- **Default**: unset (adaptive interval disabled; ingester polls at fixed `POLL_INTERVAL`)
- **Example**: `POLL_INTERVAL_MAX=30s`
