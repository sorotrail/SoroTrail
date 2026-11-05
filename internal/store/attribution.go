package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/sorotrail/sorotrail/internal/requestid"
)

// MaxTrackedTenants caps how many distinct opaque tenant buckets the cost
// attribution metrics may materialise before folding additional tenants into
// the "other" bucket, keeping Prometheus series cardinality strictly bounded.
const MaxTrackedTenants = 64

// QueryAttribution identifies the originator of a database operation: the HTTP
// route pattern (e.g. "GET /events/{id}") or background job (e.g. "ingester",
// "pruner", "auditor", "archiver"), and an opaque tenant bucket ("default",
// "t_<8hex>", or "other"). Raw contract IDs, addresses, or credentials are
// never carried here.
type QueryAttribution struct {
	Originator string
	Tenant     string
}

type attributionKey struct{}

var (
	tenantBucketsMu sync.Mutex
	tenantBuckets   = map[string]string{}
)

// OpaqueTenantLabel maps a raw tenant identifier onto a bounded-cardinality,
// non-reversible opaque label ("default", "t_<8hex>", or "other").
func OpaqueTenantLabel(tenantID string) string {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || tenantID == "default" || tenantID == "public" || tenantID == "*" {
		return "default"
	}

	tenantBucketsMu.Lock()
	defer tenantBucketsMu.Unlock()

	if label, ok := tenantBuckets[tenantID]; ok {
		return label
	}
	if len(tenantBuckets) >= MaxTrackedTenants {
		return "other"
	}
	sum := sha256.Sum256([]byte("sorotrail-tenant-cost:" + tenantID))
	label := "t_" + hex.EncodeToString(sum[:4])
	tenantBuckets[tenantID] = label
	return label
}

var contractAddressPattern = regexp.MustCompile(`\b[CG][A-Z2-7]{55}\b`)

// sanitizeOriginator scrubs any accidental strkey contract or account address
// or comment-closing sequence from an originator label.
func sanitizeOriginator(originator string) string {
	originator = strings.TrimSpace(originator)
	if originator == "" {
		return "unknown"
	}
	originator = strings.ReplaceAll(originator, "*/", "")
	originator = strings.ReplaceAll(originator, "/*", "")
	originator = contractAddressPattern.ReplaceAllString(originator, "{address}")
	return originator
}

// WithQueryAttribution attaches originator and tenant attribution to ctx.
func WithQueryAttribution(ctx context.Context, originator, tenantID string) context.Context {
	attr := QueryAttribution{
		Originator: sanitizeOriginator(originator),
		Tenant:     OpaqueTenantLabel(tenantID),
	}
	return context.WithValue(ctx, attributionKey{}, attr)
}

// AttributionFromContext resolves the QueryAttribution for ctx. When no
// explicit attribution was attached, it falls back to the background job ID
// carried by requestid.FromContext (if it matches a known job) or "unknown".
func AttributionFromContext(ctx context.Context) QueryAttribution {
	if ctx == nil {
		return QueryAttribution{Originator: "unknown", Tenant: "default"}
	}
	if attr, ok := ctx.Value(attributionKey{}).(QueryAttribution); ok && attr.Originator != "" {
		if attr.Tenant == "" {
			attr.Tenant = "default"
		}
		return attr
	}
	switch id := requestid.FromContext(ctx); id {
	case requestid.JobIngester, requestid.JobPruner, requestid.JobAuditor, requestid.JobWebhook, "archiver", "reconciler":
		return QueryAttribution{Originator: id, Tenant: "default"}
	default:
		return QueryAttribution{Originator: "unknown", Tenant: "default"}
	}
}

// FormatSQLComment renders a SQL-safe `/*originator=...,tenant=...,op=...*/`
// comment prefix for Postgres statements so pg_stat_statements and pg_stat_activity
// can join database load back to the originating route or background job.
func FormatSQLComment(ctx context.Context, operation string) string {
	attr := AttributionFromContext(ctx)
	op := sanitizeOriginator(operation)
	return fmt.Sprintf("/*originator=%s,tenant=%s,op=%s*/ ", attr.Originator, attr.Tenant, op)
}

// TagSQL prepends FormatSQLComment(ctx, operation) to a SQL statement.
func TagSQL(ctx context.Context, operation, sql string) string {
	return FormatSQLComment(ctx, operation) + sql
}
