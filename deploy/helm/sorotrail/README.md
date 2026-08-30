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

## Upgrading

```bash
helm upgrade sorotrail ./deploy/helm/sorotrail --reuse-values
```

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `replicaCount` | `1` | Number of pod replicas. |
| `image.repository` | `ghcr.io/sorotrail/sorotrail` | Container image repository. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `image.tag` | `""` (uses `appVersion`) | Image tag override. |
| `imagePullSecrets` | `[]` | Secrets for pulling images from private registries. |
| `nameOverride` | `""` | Override the chart name. |
| `fullnameOverride` | `""` | Override the full release name. |
| `serviceAccount.create` | `true` | Create a dedicated ServiceAccount for the pod. |
| `serviceAccount.annotations` | `{}` | Annotations for the ServiceAccount. |
| `serviceAccount.name` | `""` | Use an existing ServiceAccount instead of creating one. |
| `podAnnotations` | `{}` | Annotations applied to the pod. |
| `podSecurityContext.runAsNonRoot` | `true` | Run the container as a non-root user. |
| `podSecurityContext.runAsUser` | `10001` | UID the container runs as. |
| `securityContext.allowPrivilegeEscalation` | `false` | Disallow privilege escalation. |
| `securityContext.readOnlyRootFilesystem` | `true` | Mount the root filesystem as read-only. |
| `securityContext.capabilities.drop` | `[ALL]` | Drop all Linux capabilities. |
| `service.type` | `ClusterIP` | Kubernetes Service type. |
| `service.port` | `80` | Service port. |
| `service.targetPort` | `8080` | Container port to forward to. |
| `resources.requests.cpu` | `100m` | CPU request. |
| `resources.requests.memory` | `128Mi` | Memory request. |
| `resources.limits.cpu` | `500m` | CPU limit. |
| `resources.limits.memory` | `256Mi` | Memory limit. |
| `nodeSelector` | `{}` | Node selector for pod scheduling. |
| `tolerations` | `[]` | Tolerations for pod scheduling. |
| `affinity` | `{}` | Affinity rules for pod scheduling. |
| `pdb.enabled` | `false` | Create a PodDisruptionBudget. |
| `pdb.minAvailable` | `1` | Minimum pods that must remain available. |
| `pdb.maxUnavailable` | _(unset)_ | Maximum pods that may be unavailable. |
| `config.rpcUrl` | `https://soroban-testnet.stellar.org` | Stellar RPC endpoint (JSON-RPC 2.0). |
| `config.pollInterval` | `5s` | How often to poll for new events once caught up. |
| `config.httpAddr` | `:8080` | HTTP API listen address. Must match the container port (8080). |
| `config.watchedContracts` | `""` | Comma-separated contract IDs (`C...`). Empty = ingest all. |
| `config.startLedger` | `""` | Force ingestion to start from this ledger sequence number. |
| `config.retentionLedgers` | `17280` | Cold-start reach-back in ledgers (~24h at 5s/ledger). |
| `config.partitionLedgerSpan` | `120960` | Partition width for the events table, in ledgers (~7 days). |
| `config.logLevel` | `info` | Log verbosity: `debug`, `info`, `warn`, `error`. |
| `config.corsAllowedOrigins` | `""` | Comma-separated browser origins for CORS. Empty = disabled. |
| `config.rateLimitRps` | `""` | Per-client-IP rate limit requests/second. Must be set with `rateLimitBurst`. |
| `config.rateLimitBurst` | `""` | Per-client-IP rate limit burst size. Must be set with `rateLimitRps`. |
| `config.rateLimitTrustedProxy` | `""` | Honor `X-Forwarded-For` for client IP detection. |
| `config.enableMetrics` | `""` | Expose the Prometheus `/metrics` endpoint. |
| `database.existingSecret` | `""` | Name of an existing Secret containing the `DATABASE_URL`. |
| `database.existingSecretKey` | `DATABASE_URL` | Key within the existing secret that holds the connection string. |
| `database.url` | `""` | Inline Postgres connection string. Ignored when `existingSecret` is set. |
| `serviceMonitor.enabled` | `false` | Create a Prometheus ServiceMonitor resource (requires [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) CRDs). |
| `serviceMonitor.namespace` | `""` | Namespace for the ServiceMonitor. Defaults to the release namespace. |
| `serviceMonitor.additionalLabels` | `{}` | Extra labels for the ServiceMonitor. |
| `serviceMonitor.interval` | `30s` | Scrape interval. |
| `serviceMonitor.scrapeTimeout` | `10s` | Scrape timeout. |
| `serviceMonitor.path` | `/metrics` | Metrics endpoint path. |

## Health probes

The chart configures three probes against the application's built-in endpoints:

| Probe | Endpoint | Purpose |
|-------|----------|---------|
| **Startup** | `/health` | Gives the container time to connect to Postgres and run migrations. Fails fast (30s budget) if the process never starts. |
| **Liveness** | `/health` | Restarts the container if the process is deadlocked or unresponsive. |
| **Readiness** | `/readyz` | Removes the pod from the Service if Postgres or the RPC is unreachable — traffic only flows to healthy instances. |

These match the container's own `HEALTHCHECK` directive (which also targets `/health`), so Docker Compose and Kubernetes examine the same signal.

## Pod Disruption Budget

Enable `pdb.enabled=true` when running multiple replicas to prevent voluntary disruptions (node drains, cluster upgrades) from taking down every pod simultaneously:

```bash
helm install sorotrail ./deploy/helm/sorotrail \
  --set database.url="postgres://user:pass@host:5432/sorotrail?sslmode=require" \
  --set replicaCount=3 \
  --set pdb.enabled=true \
  --set pdb.minAvailable=1
```

## Prometheus metrics

Set `config.enableMetrics=true` to expose the `/metrics` endpoint and optionally `serviceMonitor.enabled=true` to create a `ServiceMonitor` resource. The ServiceMonitor requires [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) CRDs.

## Security context

The chart runs the container as a non-root user (`UID 10001`) with:

- `readOnlyRootFilesystem: true` — the container filesystem is immutable at runtime.
- `allowPrivilegeEscalation: false` — the process cannot gain more privileges than its parent.
- `capabilities.drop: [ALL]` — all Linux capabilities are dropped.

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
