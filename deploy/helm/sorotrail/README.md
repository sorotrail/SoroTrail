# SoroTrail Helm Chart

Deploys [SoroTrail](https://github.com/sorotrail/sorotrail) — a Stellar/Soroban contract-event indexer — to Kubernetes.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.10+
- An external PostgreSQL instance (see [Database](#database))

## Database

The chart **does not bundle PostgreSQL**. SoroTrail is a stateless indexer; bundling a database would couple its lifecycle to the pod and complicate backups, HA, and upgrades. Provision Postgres separately and pass the connection string via `database.url` or an existing Secret.

Recommended operators:
- [CloudNativePG](https://cloudnative-pg.io) — production-grade, actively maintained
- [Zalando Postgres Operator](https://github.com/zalando/postgres-operator)

## Installing

```bash
helm install sorotrail ./deploy/helm/sorotrail \
  --set database.url="postgres://user:pass@host:5432/sorotrail?sslmode=require"
```

Using an existing Secret:

```bash
# Secret must have a key DATABASE_URL
helm install sorotrail ./deploy/helm/sorotrail \
  --set database.existingSecret=my-db-secret
```

## Configuration

See [values.yaml](values.yaml) — every field is commented.

Key values:

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/sorotrail/sorotrail` | Image repository |
| `image.tag` | `""` (uses appVersion) | Image tag |
| `config.rpcUrl` | testnet | Stellar RPC endpoint |
| `config.watchedContracts` | `""` | Comma-separated contract IDs; empty = all |
| `database.url` | `""` | Postgres connection string |
| `database.existingSecret` | `""` | Name of existing Secret with `DATABASE_URL` |
| `serviceMonitor.enabled` | `false` | Create a Prometheus ServiceMonitor |

## Local kind cluster validation

### 1. Install kind and create a cluster

```bash
# Install kind: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
kind create cluster --name sorotrail-dev
```

### 2. Start Postgres (simplest: port-forward from docker-compose)

```bash
docker compose up -d postgres
# Postgres is now reachable at localhost:5432
```

### 3. Install the chart

```bash
helm install sorotrail ./deploy/helm/sorotrail \
  --set database.url="postgres://sorotrail:sorotrail@host.docker.internal:5432/sorotrail?sslmode=disable" \
  --set config.rpcUrl="https://soroban-testnet.stellar.org"
```

> On Linux replace `host.docker.internal` with the host's docker bridge IP (usually `172.17.0.1`).

### 4. Verify the pod is healthy

```bash
kubectl get pods
kubectl logs -l app.kubernetes.io/name=sorotrail
kubectl port-forward svc/sorotrail 8080:80
curl http://localhost:8080/health
# {"status":"ok","checks":{"database":"ok","rpc":"ok"}}
```

### 5. Tear down

```bash
helm uninstall sorotrail
kind delete cluster --name sorotrail-dev
```

## Prometheus metrics

Set `serviceMonitor.enabled=true` to create a `ServiceMonitor` resource (requires [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) CRDs). The `/metrics` endpoint must be exposed by the application for scraping to succeed.

## Upgrading

```bash
helm upgrade sorotrail ./deploy/helm/sorotrail --reuse-values
```
