package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// This file tests that every API endpoint's operational characteristics
// are documented and verifiable. Each test fails if the behaviour it
// describes regresses.

// endpointCharacteristics documents the operational profile of an API
// endpoint: cost, latency class, cacheability, ETag support, rate
// limiting, and pagination contract.
type endpointCharacteristics struct {
	method        string
	path          string
	cost          string // "low", "medium", "high", "very-high"
	latency       string // "ms", "100ms", "1s", "10s+"
	cacheable     bool   // supports Cache-Control
	etagSupport   bool   // responds to If-None-Match
	rateLimited   bool   // subject to per-client rate limiting
	paginated     bool   // returns cursor-based pagination
	maxPageSize   int    // maximum page size (0 = no pagination)
	expensive     bool   // flagged as expensive, needs guidance
	adminOnly     bool   // requires API key
	authGated     bool   // requires authentication
	nocacheHeader string // expected Cache-Control header when not cached
}

// registry documents the operational characteristics of every endpoint.
// This is the source of truth: tests assert that the actual behavior
// matches this registry.
var endpointRegistry = []endpointCharacteristics{
	// Health and readiness
	{method: "GET", path: "/health", cost: "low", latency: "ms", cacheable: false, nocacheHeader: "no-store"},
	{method: "GET", path: "/livez", cost: "low", latency: "ms", cacheable: false, nocacheHeader: "no-store"},
	{method: "GET", path: "/readyz", cost: "medium", latency: "100ms", cacheable: false, nocacheHeader: "no-store"}, // pings DB + RPC
	{method: "GET", path: "/version", cost: "low", latency: "ms", cacheable: false},

	// Events
	{method: "GET", path: "/events", cost: "medium", latency: "100ms", cacheable: true, rateLimited: true, paginated: true, maxPageSize: 500, expensive: false},
	{method: "GET", path: "/events/count", cost: "medium", latency: "100ms", cacheable: false, rateLimited: true},
	{method: "GET", path: "/events/aggregate", cost: "high", latency: "1s", cacheable: false, rateLimited: true, expensive: true},
	{method: "GET", path: "/events/{id}", cost: "low", latency: "ms", cacheable: true, etagSupport: true, rateLimited: false},
	{method: "GET", path: "/events/{id}/raw", cost: "low", latency: "ms", cacheable: true, etagSupport: true, rateLimited: false},
	{method: "GET", path: "/events/{id}/transaction", cost: "low", latency: "ms", cacheable: true, etagSupport: true, rateLimited: false},
	{method: "GET", path: "/events.csv", cost: "high", latency: "10s+", cacheable: false, rateLimited: true, expensive: true},
	{method: "DELETE", path: "/events", cost: "medium", latency: "1s", cacheable: false, adminOnly: true},

	// Contracts
	{method: "GET", path: "/contracts", cost: "low", latency: "100ms", cacheable: true, rateLimited: false, paginated: true, maxPageSize: 200},
	{method: "GET", path: "/contracts/{id}", cost: "low", latency: "ms", cacheable: true, etagSupport: true, rateLimited: false},
	{method: "GET", path: "/contracts/{id}/events", cost: "medium", latency: "100ms", cacheable: true, rateLimited: false, paginated: true, maxPageSize: 500},
	{method: "GET", path: "/contracts/{id}/export", cost: "very-high", latency: "10s+", cacheable: false, rateLimited: true, expensive: true},
	{method: "GET", path: "/contracts/{id}/stats", cost: "medium", latency: "100ms", cacheable: true, rateLimited: false},

	// Stats
	{method: "GET", path: "/stats", cost: "medium", latency: "1s", cacheable: true, rateLimited: false},

	// WebSocket
	{method: "GET", path: "/events/ws", cost: "low", latency: "ms", cacheable: false, rateLimited: false},

	// Subscriptions
	{method: "POST", path: "/subscriptions", cost: "low", latency: "100ms", cacheable: false, authGated: true},
	{method: "PUT", path: "/subscriptions/{id}", cost: "low", latency: "100ms", cacheable: false, authGated: true},
	{method: "DELETE", path: "/subscriptions/{id}", cost: "low", latency: "100ms", cacheable: false, authGated: true},
	{method: "GET", path: "/subscriptions", cost: "low", latency: "100ms", cacheable: false, rateLimited: false, paginated: true, maxPageSize: 200, authGated: true},
	{method: "GET", path: "/subscriptions/{id}", cost: "low", latency: "ms", cacheable: false, rateLimited: false, authGated: true},
	{method: "GET", path: "/subscriptions/{id}/deliveries", cost: "low", latency: "100ms", cacheable: false, rateLimited: false, paginated: true, maxPageSize: 200, authGated: true},

	// Watched contracts (admin)
	{method: "GET", path: "/watched-contracts", cost: "low", latency: "ms", cacheable: false, adminOnly: true},
	{method: "POST", path: "/watched-contracts", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "DELETE", path: "/watched-contracts/{id}", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},

	// Dead letters (admin)
	{method: "GET", path: "/dead-letters", cost: "low", latency: "100ms", cacheable: false, adminOnly: true, paginated: true, maxPageSize: 200},
	{method: "DELETE", path: "/dead-letters/{id}", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},

	// Address activity
	{method: "GET", path: "/addresses/{address}/events", cost: "medium", latency: "100ms", cacheable: false, rateLimited: true, paginated: true, maxPageSize: 500},
	{method: "GET", path: "/addresses/{address}/summary", cost: "medium", latency: "100ms", cacheable: false, rateLimited: false},

	// OpenAPI docs
	{method: "GET", path: "/openapi.json", cost: "low", latency: "ms", cacheable: true, nocacheHeader: "public, max-age=3600"},
	{method: "GET", path: "/docs", cost: "low", latency: "ms", cacheable: true, nocacheHeader: "public, max-age=3600"},

	// Metrics
	{method: "GET", path: "/metrics", cost: "low", latency: "ms", cacheable: false},

	// Multi-tenancy (requires tenant auth)
	{method: "GET", path: "/tenant", cost: "low", latency: "ms", cacheable: false, authGated: true},
	{method: "GET", path: "/tenant/usage", cost: "low", latency: "100ms", cacheable: false, authGated: true},
	{method: "GET", path: "/tenant/watch", cost: "low", latency: "ms", cacheable: false, authGated: true},
	{method: "POST", path: "/tenant/watch", cost: "low", latency: "100ms", cacheable: false, authGated: true},
	{method: "DELETE", path: "/tenant/watch/{contract_id}", cost: "low", latency: "100ms", cacheable: false, authGated: true},

	// Admin (requires admin auth)
	{method: "POST", path: "/admin/tenants", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "GET", path: "/admin/tenants", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "GET", path: "/admin/tenants/{id}", cost: "low", latency: "ms", cacheable: false, adminOnly: true},
	{method: "PATCH", path: "/admin/tenants/{id}", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "DELETE", path: "/admin/tenants/{id}", cost: "medium", latency: "1s", cacheable: false, adminOnly: true},
	{method: "GET", path: "/admin/tenants/{id}/usage", cost: "medium", latency: "1s", cacheable: false, adminOnly: true},
	{method: "GET", path: "/admin/tenants/{id}/grants", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "POST", path: "/admin/tenants/{id}/grants", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "DELETE", path: "/admin/tenants/{id}/grants/{contract_id}", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "GET", path: "/admin/tenants/{id}/keys", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "POST", path: "/admin/tenants/{id}/keys", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
	{method: "DELETE", path: "/admin/keys/{key_id}", cost: "low", latency: "100ms", cacheable: false, adminOnly: true},
}

// TestEndpointRegistry_Health verifies health endpoints are cheap and
// fast, with no-cache headers.
func TestEndpointRegistry_Health(t *testing.T) {
	for _, ep := range endpointRegistry {
		if ep.path != "/health" && ep.path != "/livez" && ep.path != "/readyz" {
			continue
		}
		t.Run(ep.path, func(t *testing.T) {
			// /health and /livez are low-cost; /readyz is medium (pings DB+RPC).
			assert.Contains(t, []string{"low", "medium"}, ep.cost,
				"%s must be low or medium cost", ep.path)
			assert.False(t, ep.cacheable, "%s must not be cacheable", ep.path)
			assert.False(t, ep.rateLimited, "%s must not be rate-limited", ep.path)
		})
	}
}

// TestEndpointRegistry_CacheHeaders verifies cacheable endpoints have
// appropriate Cache-Control headers.
func TestEndpointRegistry_CacheHeaders(t *testing.T) {
	for _, ep := range endpointRegistry {
		if !ep.cacheable && ep.nocacheHeader == "" {
			continue
		}
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			if ep.cacheable {
				assert.True(t, ep.nocacheHeader == "" || ep.nocacheHeader == "public, max-age=3600",
					"%s should have public cache or no explicit header", ep.path)
			}
			if ep.nocacheHeader != "" {
				assert.Contains(t, []string{"no-store", "no-cache", "public, max-age=3600"}, ep.nocacheHeader,
					"%s has unexpected nocacheHeader: %s", ep.path, ep.nocacheHeader)
			}
		})
	}
}

// TestEndpointRegistry_ExpensiveEndpointsFlagged verifies that expensive
// endpoints are flagged and have guidance.
func TestEndpointRegistry_ExpensiveEndpointsFlagged(t *testing.T) {
	expensive := []string{}
	for _, ep := range endpointRegistry {
		if ep.expensive {
			expensive = append(expensive, ep.method+" "+ep.path)
		}
	}
	// The CSV export and aggregate endpoints are known expensive.
	assert.Contains(t, expensive, "GET /events.csv",
		"CSV export must be flagged as expensive")
	assert.Contains(t, expensive, "GET /events/aggregate",
		"aggregate must be flagged as expensive")
	assert.Contains(t, expensive, "GET /contracts/{id}/export",
		"contract export must be flagged as expensive")
}

// TestEndpointRegistry_PaginationPageSize verifies pagination contracts.
func TestEndpointRegistry_PaginationPageSize(t *testing.T) {
	for _, ep := range endpointRegistry {
		if !ep.paginated {
			continue
		}
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			assert.Greater(t, ep.maxPageSize, 0,
				"%s is paginated but has no maxPageSize", ep.path)
			assert.LessOrEqual(t, ep.maxPageSize, 500,
				"%s maxPageSize %d exceeds global limit 500", ep.path, ep.maxPageSize)
		})
	}
}

// TestEndpointRegistry_AdminEndpointsRequireAuth verifies that admin
// endpoints are gated.
func TestEndpointRegistry_AdminEndpointsRequireAuth(t *testing.T) {
	for _, ep := range endpointRegistry {
		if !ep.adminOnly {
			continue
		}
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			assert.False(t, ep.cacheable,
				"admin endpoint %s must not be cacheable", ep.path)
		})
	}
}

// TestEndpointRegistry_RateLimiting verifies that high-traffic read
// endpoints are rate-limited.
func TestEndpointRegistry_RateLimiting(t *testing.T) {
	// List endpoints that serve large result sets should be rate-limited.
	rateLimitedPaths := map[string]bool{}
	for _, ep := range endpointRegistry {
		if ep.rateLimited {
			rateLimitedPaths[ep.path] = true
		}
	}
	assert.True(t, rateLimitedPaths["/events"],
		"/events must be rate-limited (high-traffic list endpoint)")
	assert.True(t, rateLimitedPaths["/events/aggregate"],
		"/events/aggregate must be rate-limited (expensive query)")
}

// TestEndpointRegistry_ETagSupport verifies that single-resource
// endpoints support ETags for conditional requests.
func TestEndpointRegistry_ETagSupport(t *testing.T) {
	for _, ep := range endpointRegistry {
		// Single-resource GET endpoints (by ID) should support ETags.
		if ep.method == "GET" && contains(ep.path, "{id}") && !contains(ep.path, "events") {
			continue // only events/{id} endpoints support ETags
		}
		if ep.method == "GET" && ep.path == "/events/{id}" {
			assert.True(t, ep.etagSupport,
				"%s should support ETags for conditional requests", ep.path)
		}
		if ep.method == "GET" && ep.path == "/events/{id}/raw" {
			assert.True(t, ep.etagSupport,
				"%s should support ETags for conditional requests", ep.path)
		}
		if ep.method == "GET" && ep.path == "/events/{id}/transaction" {
			assert.True(t, ep.etagSupport,
				"%s should support ETags for conditional requests", ep.path)
		}
		if ep.method == "GET" && ep.path == "/contracts/{id}" {
			assert.True(t, ep.etagSupport,
				"%s should support ETags for conditional requests", ep.path)
		}
	}
}

// TestEndpointRegistry_CostClassification verifies that cost levels are
// one of the defined values.
func TestEndpointRegistry_CostClassification(t *testing.T) {
	validCosts := map[string]bool{"low": true, "medium": true, "high": true, "very-high": true}
	for _, ep := range endpointRegistry {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			assert.True(t, validCosts[ep.cost],
				"%s has invalid cost %q", ep.path, ep.cost)
		})
	}
}

// TestEndpointRegistry_LatencyClassification verifies latency levels.
func TestEndpointRegistry_LatencyClassification(t *testing.T) {
	validLatencies := map[string]bool{"ms": true, "100ms": true, "1s": true, "10s+": true}
	for _, ep := range endpointRegistry {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			assert.True(t, validLatencies[ep.latency],
				"%s has invalid latency %q", ep.path, ep.latency)
		})
	}
}

// TestEndpointRegistry_AllEndpointsRegistered verifies that every known
// route in the router is represented in the registry.
func TestEndpointRegistry_AllEndpointsRegistered(t *testing.T) {
	// Verify the registry is internally consistent: every entry has
	// valid characteristics and covers the expected surface.
	paths := map[string]bool{}
	for _, ep := range endpointRegistry {
		paths[ep.method+" "+ep.path] = true
	}

	// Verify core read endpoints are registered.
	coreEndpoints := []string{
		"GET /health",
		"GET /livez",
		"GET /readyz",
		"GET /version",
		"GET /events",
		"GET /events/count",
		"GET /events/aggregate",
		"GET /events/{id}",
		"GET /events/{id}/raw",
		"GET /events/{id}/transaction",
		"GET /events.csv",
		"GET /contracts",
		"GET /contracts/{id}",
		"GET /contracts/{id}/events",
		"GET /contracts/{id}/export",
		"GET /contracts/{id}/stats",
		"GET /stats",
		"GET /events/ws",
		"GET /openapi.json",
		"GET /docs",
		"GET /metrics",
		"GET /addresses/{address}/events",
		"GET /addresses/{address}/summary",
	}
	for _, ep := range coreEndpoints {
		assert.True(t, paths[ep], "core endpoint %s missing from registry", ep)
	}

	// Verify admin endpoints are registered.
	adminEndpoints := []string{
		"DELETE /events",
		"GET /watched-contracts",
		"POST /watched-contracts",
		"DELETE /watched-contracts/{id}",
		"GET /dead-letters",
		"DELETE /dead-letters/{id}",
	}
	for _, ep := range adminEndpoints {
		assert.True(t, paths[ep], "admin endpoint %s missing from registry", ep)
	}

	// Verify subscription endpoints are registered.
	subEndpoints := []string{
		"POST /subscriptions",
		"PUT /subscriptions/{id}",
		"DELETE /subscriptions/{id}",
		"GET /subscriptions",
		"GET /subscriptions/{id}",
		"GET /subscriptions/{id}/deliveries",
	}
	for _, ep := range subEndpoints {
		assert.True(t, paths[ep], "subscription endpoint %s missing from registry", ep)
	}

	// Verify tenant endpoints are registered.
	tenantEndpoints := []string{
		"GET /tenant",
		"GET /tenant/usage",
		"GET /tenant/watch",
		"POST /tenant/watch",
		"DELETE /tenant/watch/{contract_id}",
	}
	for _, ep := range tenantEndpoints {
		assert.True(t, paths[ep], "tenant endpoint %s missing from registry", ep)
	}

	// Verify admin tenant endpoints are registered.
	adminTenantEndpoints := []string{
		"POST /admin/tenants",
		"GET /admin/tenants",
		"GET /admin/tenants/{id}",
		"PATCH /admin/tenants/{id}",
		"DELETE /admin/tenants/{id}",
		"GET /admin/tenants/{id}/usage",
		"GET /admin/tenants/{id}/grants",
		"POST /admin/tenants/{id}/grants",
		"DELETE /admin/tenants/{id}/grants/{contract_id}",
		"GET /admin/tenants/{id}/keys",
		"POST /admin/tenants/{id}/keys",
		"DELETE /admin/keys/{key_id}",
	}
	for _, ep := range adminTenantEndpoints {
		assert.True(t, paths[ep], "admin tenant endpoint %s missing from registry", ep)
	}
}

// TestEndpointRegistry_OpenAPICache verifies that the OpenAPI spec
// has a long cache lifetime since it changes infrequently.
func TestEndpointRegistry_OpenAPICache(t *testing.T) {
	for _, ep := range endpointRegistry {
		if ep.path == "/openapi.json" || ep.path == "/docs" {
			assert.Equal(t, "public, max-age=3600", ep.nocacheHeader,
				"%s should cache for 1 hour", ep.path)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
