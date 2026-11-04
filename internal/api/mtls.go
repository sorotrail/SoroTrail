package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// ClientCertIdentity holds the verified X.509 client certificate identity
// extracted from a mutual TLS connection.
type ClientCertIdentity struct {
	SubjectCN string
	SANURIs   []string
	SANDNS    []string
	Serial    string
	NotAfter  time.Time
}

// AllIdentities returns the ordered, deduplicated list of candidate identity
// strings carried by the certificate: SAN URIs first (for example SPIFFE IDs
// such as spiffe://cluster.local/ns/prod/sa/caller), then SAN DNS names, then
// Subject CommonName.
func (c ClientCertIdentity) AllIdentities() []string {
	var out []string
	seen := make(map[string]struct{})
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, u := range c.SANURIs {
		add(u)
	}
	for _, d := range c.SANDNS {
		add(d)
	}
	add(c.SubjectCN)
	return out
}

type clientCertCtxKey struct{}
type expectedMTLSTenantCtxKey struct{}

// WithClientCertIdentity returns a new context carrying id.
func WithClientCertIdentity(ctx context.Context, id ClientCertIdentity) context.Context {
	return context.WithValue(ctx, clientCertCtxKey{}, id)
}

// ClientCertIdentityFrom returns the authenticated client certificate identity
// from ctx, if present.
func ClientCertIdentityFrom(ctx context.Context) (ClientCertIdentity, bool) {
	id, ok := ctx.Value(clientCertCtxKey{}).(ClientCertIdentity)
	return id, ok
}

// MTLSConfig configures inbound mutual TLS authentication and authorization
// on the HTTP API server.
type MTLSConfig struct {
	Enabled           bool
	AllowedIdentities []string
	TenantMap         map[string]string
}

var (
	serverMTLSMu      sync.RWMutex
	serverMTLSConfigs = make(map[*Server]MTLSConfig)
)

// SetMTLSConfig configures inbound mTLS enforcement and identity mapping on s.
func (s *Server) SetMTLSConfig(cfg MTLSConfig) {
	serverMTLSMu.Lock()
	serverMTLSConfigs[s] = cfg
	serverMTLSMu.Unlock()
}

// WithMTLS configures inbound mTLS on s and returns s for chaining.
func (s *Server) WithMTLS(cfg MTLSConfig) *Server {
	s.SetMTLSConfig(cfg)
	return s
}

func (s *Server) mtlsConfig() MTLSConfig {
	serverMTLSMu.RLock()
	cfg := serverMTLSConfigs[s]
	serverMTLSMu.RUnlock()
	return cfg
}

func extractClientCertIdentity(r *http.Request) (ClientCertIdentity, bool) {
	if r == nil || r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ClientCertIdentity{}, false
	}
	leaf := r.TLS.PeerCertificates[0]
	now := time.Now()
	if (!leaf.NotBefore.IsZero() && now.Before(leaf.NotBefore)) || (!leaf.NotAfter.IsZero() && now.After(leaf.NotAfter)) {
		return ClientCertIdentity{}, false
	}
	id := ClientCertIdentity{
		SubjectCN: leaf.Subject.CommonName,
		SANDNS:    append([]string(nil), leaf.DNSNames...),
		NotAfter:  leaf.NotAfter,
	}
	if leaf.SerialNumber != nil {
		id.Serial = leaf.SerialNumber.Text(16)
	}
	for _, u := range leaf.URIs {
		if u != nil {
			id.SANURIs = append(id.SANURIs, u.String())
		}
	}
	return id, true
}

func matchesAllowedIdentity(id ClientCertIdentity, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	candidates := id.AllIdentities()
	for _, want := range allowed {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}
		for _, got := range candidates {
			if got == want {
				return true
			}
		}
	}
	return false
}

func resolveCertTenant(id ClientCertIdentity, tenantMap map[string]string) (string, bool) {
	for _, candidate := range id.AllIdentities() {
		if tenantName, ok := tenantMap[candidate]; ok && strings.TrimSpace(tenantName) != "" {
			return strings.TrimSpace(tenantName), true
		}
	}
	return "", false
}

func writeMTLSRejection(w http.ResponseWriter, status int, code ErrorCode, msg string) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Mutual realm="sorotrail"`)
	}
	writeJSON(w, status, errorResponse{
		Error: msg,
		Code:  code,
	})
}

// authenticateMTLS enforces client-certificate presence, allowlist matching,
// and optional multi-tenant certificate-to-tenant resolution before standard
// API key authentication runs.
//
// Returns:
//   - (true, false) when the request was rejected and the error response has
//     already been written;
//   - (true, true) when mTLS fully authenticated the request and installed the
//     Principal on *r;
//   - (false, true) when the request passed mTLS checks and should continue
//     through the remaining authentication flow in authenticate.
func (s *Server) authenticateMTLS(w http.ResponseWriter, r **http.Request) (handled bool, ok bool) {
	req := *r
	certID, hasCert := extractClientCertIdentity(req)
	if hasCert {
		req = req.WithContext(WithClientCertIdentity(req.Context(), certID))
		*r = req
	}

	cfg := s.mtlsConfig()
	if !cfg.Enabled {
		return false, true
	}

	// Kubernetes liveness/readiness probes (/health, /livez, /readyz) do not
	// carry client certificates unless configured on the kubelet; exempting
	// them here lets probes check process health while every API route stays
	// strictly gated.
	if publicPaths[req.URL.Path] {
		return false, true
	}

	if !hasCert {
		writeMTLSRejection(w, http.StatusUnauthorized, CodeUnauthorized, "client certificate required")
		return true, false
	}

	if !matchesAllowedIdentity(certID, cfg.AllowedIdentities) {
		writeMTLSRejection(w, http.StatusForbidden, CodeForbidden, "client certificate identity is not authorized")
		return true, false
	}

	if s.multiTenant && len(cfg.TenantMap) > 0 {
		tenantName, mapped := resolveCertTenant(certID, cfg.TenantMap)
		if !mapped {
			writeMTLSRejection(w, http.StatusForbidden, CodeForbidden, "client certificate identity is not mapped to any tenant")
			return true, false
		}
		tenant, err := s.tenants.GetTenantByName(req.Context(), tenantName)
		if errors.Is(err, store.ErrNotFound) {
			writeMTLSRejection(w, http.StatusForbidden, CodeForbidden, "client certificate tenant not found")
			return true, false
		}
		if err != nil {
			loggerFromContext(req.Context()).Error("resolving mTLS tenant", "tenant", tenantName, "error", err)
			writeMTLSRejection(w, http.StatusInternalServerError, CodeInternalError, "authentication failed")
			return true, false
		}
		if !tenant.Enabled {
			writeMTLSRejection(w, http.StatusForbidden, CodeForbidden, "tenant is disabled")
			return true, false
		}

		// When the caller also presents an API key (or API_KEY_AUTH_ENABLED is
		// true for defence-in-depth), record the expected tenant on context and
		// let s.authenticate verify the API key and confirm both identities
		// agree.
		if credentialFromRequest(req) != "" || s.apiKeyAuth {
			req = req.WithContext(context.WithValue(req.Context(), expectedMTLSTenantCtxKey{}, tenant.ID))
			*r = req
			return false, true
		}

		scope, err := s.tenants.ScopeForTenant(req.Context(), tenant)
		if err != nil {
			loggerFromContext(req.Context()).Error("resolving mTLS tenant scope", "tenant", tenant.ID, "error", err)
			writeMTLSRejection(w, http.StatusInternalServerError, CodeInternalError, "authentication failed")
			return true, false
		}
		ctx := WithPrincipal(req.Context(), Principal{
			Tenant: tenant,
			Scope:  scope,
		})
		*r = req.WithContext(ctx)
		return true, true
	}

	return false, true
}
