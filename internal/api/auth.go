package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/sorotrail/sorotrail/internal/store"
)

// API key authentication (#17) and tenant resolution (#48).
//
// A key looks like:
//
//	st_<prefix>_<secret>
//
// prefix is a non-secret 16-character lookup handle stored in the clear and
// indexed; secret is 32 bytes of CSPRNG output. Only SHA-256 of the whole
// string is persisted, so a database disclosure yields no usable
// credentials, and the presented key is compared against that digest in
// constant time.
//
// The prefix exists because the alternative — looking a key up by its hash —
// forces either an index over secret-derived material or a full scan of the
// key table per request. Splitting lookup (prefix, indexed, public) from
// verification (digest, constant-time) keeps authentication a single indexed
// read without ever making the secret searchable.

const (
	keyScheme = "st"
	// keyPrefixLen is the encoded length of 10 random bytes: ceil(80/5) = 16
	// base32 characters, with no padding and no truncation.
	keyPrefixLen   = 16
	keyPrefixBytes = 10
	keySecretLen   = 32
)

// keyEncoding is unpadded base32 (A-Z, 2-7).
//
// base64url would be the reflexive choice and is wrong here: its alphabet
// contains "_", the same character that separates a key's fields, so a
// generated key could not be reliably parsed back into scheme, prefix and
// secret. base32's alphabet excludes both "_" and "-", which makes the
// separator unambiguous — and it matches the encoding Stellar strkeys
// already use, so keys look at home next to the contract IDs beside them.
var keyEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// GenerateAPIKey mints a new key, returning the plaintext to hand to the
// operator exactly once, plus the prefix and digest to persist. The
// plaintext is never recoverable afterwards.
func GenerateAPIKey() (plaintext, prefix string, digest []byte, err error) {
	prefixBytes := make([]byte, keyPrefixBytes)
	if _, err := rand.Read(prefixBytes); err != nil {
		return "", "", nil, fmt.Errorf("generating key prefix: %w", err)
	}
	secretBytes := make([]byte, keySecretLen)
	if _, err := rand.Read(secretBytes); err != nil {
		return "", "", nil, fmt.Errorf("generating key secret: %w", err)
	}
	prefix = keyEncoding.EncodeToString(prefixBytes)
	secret := keyEncoding.EncodeToString(secretBytes)
	plaintext = keyScheme + "_" + prefix + "_" + secret
	sum := sha256.Sum256([]byte(plaintext))
	return plaintext, prefix, sum[:], nil
}

// parseAPIKey splits a presented key into its prefix and the digest of the
// whole string. It validates shape only — whether the key is real is
// decided by the constant-time digest comparison, never by this function.
func parseAPIKey(presented string) (prefix string, digest []byte, ok bool) {
	parts := strings.Split(presented, "_")
	if len(parts) != 3 || parts[0] != keyScheme {
		return "", nil, false
	}
	if len(parts[1]) != keyPrefixLen || parts[2] == "" {
		return "", nil, false
	}
	sum := sha256.Sum256([]byte(presented))
	return parts[1], sum[:], true
}

// ParseAPIKeyForBootstrap validates an operator-supplied key and returns the
// parts to persist. It is the exported face of parseAPIKey, used only by the
// bootstrap path in main so that the key format has exactly one definition.
func ParseAPIKeyForBootstrap(key string) (prefix string, digest []byte, ok bool) {
	return parseAPIKey(key)
}

// credentialFromRequest extracts a presented key from either the
// Authorization: Bearer header or X-API-Key. Bearer wins when both are
// present.
func credentialFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if after, found := strings.CutPrefix(h, "Bearer "); found {
			return strings.TrimSpace(after)
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

// Principal is the authenticated identity of a request: which tenant it acts
// for and, crucially, what that tenant may read.
//
// Scope is resolved once per request, at authentication time, and every read
// path downstream uses this value rather than re-deriving entitlement. That
// makes the boundary a property of the request rather than a decision each
// handler gets to make (or forget).
type Principal struct {
	Tenant store.Tenant
	Scope  store.Scope
	KeyID  int64
	// Untenanted marks the implicit principal of a MULTI_TENANT=false
	// deployment: full access, no key required, no usage accounting. It
	// exists so downstream code can tell "single-tenant mode" apart from
	// "a real tenant that happens to hold wildcard".
	Untenanted bool
}

type principalCtxKey struct{}

// WithPrincipal returns a context carrying p. Exported for tests, which
// exercise handlers without going through the authentication middleware.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// PrincipalFrom returns the authenticated principal, and whether one was
// present.
//
// A missing principal yields the zero Principal, whose Scope is the zero
// store.Scope — which denies everything. That is the intended behavior for
// a request that somehow reached a handler without passing authentication:
// it reads nothing, rather than reading everything.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	return p, ok
}

// scopeFrom is the single point where a read path obtains its authorization.
// Every event-reading handler funnels through this, so there is exactly one
// line to audit for "where does entitlement come from".
func scopeFrom(ctx context.Context) store.Scope {
	p, ok := PrincipalFrom(ctx)
	if !ok {
		return store.Scope{} // denies everything
	}
	return p.Scope
}

// publicPaths bypass authentication even in multi-tenant mode. These are
// polled by orchestrators that hold no credential, and they report only
// liveness/readiness of the process and its dependencies — no tenant data.
//
// /livez and /readyz belong here for a concrete operational reason: a
// kubelet probe carries no API key, so gating them would leave the pod
// failing readiness forever the moment MULTI_TENANT is turned on. They
// are also the only endpoints where a 401 is indistinguishable from a
// genuinely unhealthy process.
var publicPaths = map[string]bool{
	"/health": true,
	"/livez":  true,
	"/readyz": true,
}

// authenticate resolves the request's principal and rejects unauthenticated
// or unauthorized callers.
//
// In single-tenant mode (the default) it injects a wildcard principal and
// requires no credential, so behavior is bit-for-bit what it was before
// multi-tenancy existed. The middleware is installed unconditionally rather
// than only when multi-tenancy is on, which matters: it guarantees that
// every request arriving at a handler carries a principal, so a handler can
// rely on scopeFrom returning a real answer instead of silently denying.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.multiTenant {
			ctx := WithPrincipal(r.Context(), Principal{
				Scope:      store.WildcardScope(),
				Untenanted: true,
			})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		if publicPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		presented := credentialFromRequest(r)
		if presented == "" {
			writeUnauthorized(w, "missing API key")
			return
		}
		prefix, digest, ok := parseAPIKey(presented)
		if !ok {
			writeUnauthorized(w, "malformed API key")
			return
		}

		key, storedDigest, tenant, err := s.tenants.LookupAPIKey(r.Context(), prefix)
		if errors.Is(err, store.ErrNotFound) {
			writeUnauthorized(w, "unknown or revoked API key")
			return
		}
		if err != nil {
			s.log.Error("looking up api key", "error", err)
			writeError(w, http.StatusInternalServerError, errors.New("authentication failed"))
			return
		}
		// Constant-time: a byte-by-byte comparison would let an attacker
		// who already knows a valid prefix recover the secret through
		// response timing.
		if subtle.ConstantTimeCompare(digest, storedDigest) != 1 {
			writeUnauthorized(w, "unknown or revoked API key")
			return
		}
		if !tenant.Enabled {
			// Distinct from the unknown-key message on purpose: the caller
			// holds a genuine credential and needs to know the account is
			// suspended rather than the key wrong.
			writeError(w, http.StatusForbidden, errors.New("tenant is disabled"))
			return
		}

		scope, err := s.tenants.ScopeForTenant(r.Context(), tenant)
		if err != nil {
			s.log.Error("resolving tenant scope", "tenant", tenant.ID, "error", err)
			writeError(w, http.StatusInternalServerError, errors.New("authentication failed"))
			return
		}

		// Advisory; a failure here must not deny an otherwise valid request.
		if err := s.tenants.TouchAPIKey(r.Context(), key.ID); err != nil {
			s.log.Debug("recording api key use", "key", key.ID, "error", err)
		}

		ctx := WithPrincipal(r.Context(), Principal{
			Tenant: tenant,
			Scope:  scope,
			KeyID:  key.ID,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireAdmin gates the /admin/* routes on the tenant's admin flag.
//
// In single-tenant mode the implicit principal is untenanted and admin
// endpoints are refused outright: there are no tenants to administer, and
// leaving key-minting reachable on an instance that requires no credential
// would hand anyone who can reach the port the ability to provision access
// for when multi-tenancy is switched on.
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFrom(r.Context())
		if !ok || p.Untenanted {
			writeError(w, http.StatusNotFound,
				errors.New("admin API is available only when MULTI_TENANT=true"))
			return
		}
		if !p.Tenant.Admin {
			writeError(w, http.StatusForbidden, errors.New("admin privileges required"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireTenant gates the tenant self-service routes, which need a real
// tenant to be about.
func (s *Server) requireTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFrom(r.Context())
		if !ok || p.Untenanted {
			writeError(w, http.StatusNotFound,
				errors.New("tenant API is available only when MULTI_TENANT=true"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// TenantLimitResolver keys the rate limiter on the authenticated tenant,
// applying the tenant's own overrides where set and the instance-wide
// defaults otherwise.
//
// It returns ok=false for unauthenticated and untenanted requests, and for
// tenants with no override on an instance that configured no default —
// which sends them down the IP-keyed path, preserving exactly the pre-#48
// behavior for deployments that never turn any of this on.
func TenantLimitResolver(defaultRPS float64, defaultBurst int) LimitResolver {
	return func(r *http.Request) (string, float64, int, bool) {
		p, ok := PrincipalFrom(r.Context())
		if !ok || p.Untenanted || p.Tenant.ID == 0 {
			return "", 0, 0, false
		}
		rps, burst := defaultRPS, defaultBurst
		overridden := false
		if p.Tenant.RateLimitRPS != nil {
			rps, overridden = *p.Tenant.RateLimitRPS, true
		}
		if p.Tenant.RateLimitBurst != nil {
			burst, overridden = *p.Tenant.RateLimitBurst, true
		}
		if !overridden && (defaultRPS <= 0 || defaultBurst <= 0) {
			return "", 0, 0, false
		}
		return "tenant:" + strconv.FormatInt(p.Tenant.ID, 10), rps, burst, true
	}
}

func writeUnauthorized(w http.ResponseWriter, msg string) {
	// RFC 7235 §4.1 requires a challenge on 401.
	w.Header().Set("WWW-Authenticate", `Bearer realm="sorotrail"`)
	writeError(w, http.StatusUnauthorized, errors.New(msg))
}
