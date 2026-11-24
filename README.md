# SoroTrail

SoroTrail is a high-performance event indexing and streaming service for Stellar Soroban smart contracts. It captures ledger data and contract events from Stellar RPC, decodes and indexes them, and exposes robust query capabilities via REST, GraphQL, and WebSockets.

## Quickstart

Get up and running with SoroTrail in a few simple steps.

1. Set your environment variables:
   ```bash
   export DATABASE_URL="postgres://user:password@localhost:5432/sorotrail"
   export RPC_URL="https://soroban-testnet.stellar.org"
   export PORT=8080
   ```

2. Run the SoroTrail service:
   ```bash
   sorotrail serve
   ```

3. Query events using a simple curl command:
   ```bash
   curl -s "http://localhost:8080/events?limit=5" | jq .
   ```

## Documentation

For full operational guides, configuration parameters, architecture notes, and troubleshooting, see the [Documentation Directory](docs/README.md).
