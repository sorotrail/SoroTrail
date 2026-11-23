# Configuration Reference

SoroTrail is configured via environment variables. Below is the complete reference of all supported variables.

## Environment Variables

| Variable | Description | Default | Required |
| --- | --- | --- | --- |
| `RPC_URL` | Stellar RPC endpoint URL used for network queries and ledger ingestion | — | **Yes** |
| `DATABASE_URL` | PostgreSQL connection string for event storage and state | — | **Yes** |
| `PORT` | HTTP server listening port | `8080` | No |
| `LOG_LEVEL` | Logging verbosity (`debug`, `info`, `warn`, `error`) | `info` | No |

### RPC_URL

The `RPC_URL` environment variable points SoroTrail to a running Stellar RPC or Soroban RPC provider. SoroTrail uses this endpoint to stream ledgers, inspect contract specifications, and verify transaction metadata.
