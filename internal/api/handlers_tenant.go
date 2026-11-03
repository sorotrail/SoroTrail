package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Tenant administration (/admin/*) and self-service (/tenant/*).
//
// Everything here is gated by requireAdmin or requireTenant in Router, so
// none of these handlers re-check authentication. What they do check is
// ownership: a self-service handler acts on the caller's own tenant ID taken
// from the principal, never on an ID from the URL or body, so there is no
// parameter a tenant could tamper with to act on another.

const maxUsageDays = 365

// tenantRequest is the create/update payload. Pointer fields distinguish
// "not supplied" from "set to the zero value", which matters for the quota
// overrides where 0 means "deny" and absent means "inherit".
type tenantRequest struct {
	Name                *string  `json:"name"`
	Wildcard            *bool    `json:"wildcard"`
	Admin               *bool    `json:"admin"`
	Enabled             *bool    `json:"enabled"`
	RateLimitRPS        *float64 `json:"rate_limit_rps"`
	RateLimitBurst      *int     `json:"rate_limit_burst"`
	MaxWatchedContracts *int     `json:"max_watched_contracts"`
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req tenantRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	if req.Name == nil || *req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	t := store.Tenant{
		Name:                *req.Name,
		Enabled:             true,
		RateLimitRPS:        req.RateLimitRPS,
		RateLimitBurst:      req.RateLimitBurst,
		MaxWatchedContracts: req.MaxWatchedContracts,
	}
	if req.Wildcard != nil {
		t.Wildcard = *req.Wildcard
	}
	if req.Admin != nil {
		t.Admin = *req.Admin
	}
	if req.Enabled != nil {
		t.Enabled = *req.Enabled
	}
	if err := validateTenantQuotas(t); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	created, err := s.tenants.CreateTenant(r.Context(), t)
	if errors.Is(err, store.ErrDuplicate) {
		writeError(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("creating tenant", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("creating tenant failed"))
		return
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusCreated, created)
}

func validateTenantQuotas(t store.Tenant) error {
	if t.RateLimitRPS != nil && *t.RateLimitRPS < 0 {
		return errors.New("rate_limit_rps must be non-negative")
	}
	if t.RateLimitBurst != nil && *t.RateLimitBurst < 0 {
		return errors.New("rate_limit_burst must be non-negative")
	}
	if t.MaxWatchedContracts != nil && *t.MaxWatchedContracts < 0 {
		return errors.New("max_watched_contracts must be non-negative")
	}
	// Both halves or neither: a limiter with one of them unset behaves like
	// no limiter at all, which would silently ignore an operator's intent to
	// throttle. Same rule the instance-wide RATE_LIMIT_* pair enforces.
	if (t.RateLimitRPS == nil) != (t.RateLimitBurst == nil) {
		return errors.New("rate_limit_rps and rate_limit_burst must be set together")
	}
	return nil
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := s.tenants.ListTenants(r.Context())
	if err != nil {
		loggerFromContext(r.Context()).Error("listing tenants", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing tenants failed"))
		return
	}
	// The whole list is returned on one page, so the total is just the
	// page size; no separate count query is needed.
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(tenants)))
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleUpdateTenant(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	var req tenantRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	// PATCH semantics: absent fields keep their stored value.
	if req.Name != nil {
		t.Name = *req.Name
	}
	if req.Wildcard != nil {
		t.Wildcard = *req.Wildcard
	}
	if req.Admin != nil {
		t.Admin = *req.Admin
	}
	if req.Enabled != nil {
		t.Enabled = *req.Enabled
	}
	if req.RateLimitRPS != nil {
		t.RateLimitRPS = req.RateLimitRPS
	}
	if req.RateLimitBurst != nil {
		t.RateLimitBurst = req.RateLimitBurst
	}
	if req.MaxWatchedContracts != nil {
		t.MaxWatchedContracts = req.MaxWatchedContracts
	}
	if err := validateTenantQuotas(t); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	updated, err := s.tenants.UpdateTenant(r.Context(), t)
	if errors.Is(err, store.ErrDuplicate) {
		writeError(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("updating tenant", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("updating tenant failed"))
		return
	}
	s.notifyTenantInvalidation(r.Context(), updated.ID)
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "id")
	if !ok {
		return
	}
	p, _ := PrincipalFrom(r.Context())
	if p.Tenant.ID == id {
		// Deleting the tenant you are authenticated as would cascade away
		// the key you are holding, potentially leaving an instance with no
		// admin at all.
		writeError(w, http.StatusConflict, errors.New("a tenant cannot delete itself"))
		return
	}
	err := s.tenants.DeleteTenant(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("tenant %d not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("deleting tenant", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting tenant failed"))
		return
	}
	s.notifyTenantInvalidation(r.Context(), id)
	writeCacheHeaders(w, cacheNoStore, 0, "")
	w.WriteHeader(http.StatusNoContent)
}

type grantRequest struct {
	ContractID string `json:"contract_id"`
}

func (s *Server) handleGrantContract(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	var req grantRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	if !config.ValidContractID(req.ContractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract_id %q", req.ContractID))
		return
	}
	if err := s.tenants.GrantContract(r.Context(), t.ID, req.ContractID); err != nil {
		loggerFromContext(r.Context()).Error("granting contract", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("granting contract failed"))
		return
	}
	s.notifyTenantInvalidation(r.Context(), t.ID)
	s.writeGrants(w, r, t.ID)
}

func (s *Server) handleRevokeContract(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	contractID := chi.URLParam(r, "contract_id")
	if err := s.tenants.RevokeContract(r.Context(), t.ID, contractID); err != nil {
		loggerFromContext(r.Context()).Error("revoking contract", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("revoking contract failed"))
		return
	}
	s.notifyTenantInvalidation(r.Context(), t.ID)
	s.writeGrants(w, r, t.ID)
}

func (s *Server) handleListTenantGrants(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	s.writeGrants(w, r, t.ID)
}

func (s *Server) writeGrants(w http.ResponseWriter, r *http.Request, tenantID int64) {
	grants, err := s.tenants.ListGrants(r.Context(), tenantID)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing grants", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing grants failed"))
		return
	}
	// The whole list is returned on one page, so the total is just the
	// page size; no separate count query is needed.
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(grants)))
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, map[string]any{"contract_ids": grants})
}

type keyRequest struct {
	Name string `json:"name"`
}

// handleCreateTenantKey mints a key. The plaintext appears in this response
// and nowhere else, ever — only its digest is stored.
func (s *Server) handleCreateTenantKey(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	var req keyRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	plaintext, prefix, digest, err := GenerateAPIKey()
	if err != nil {
		loggerFromContext(r.Context()).Error("generating api key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("generating api key failed"))
		return
	}
	key, err := s.tenants.CreateTenantAPIKey(r.Context(), t.ID, req.Name, prefix, digest)
	if err != nil {
		loggerFromContext(r.Context()).Error("creating api key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("creating api key failed"))
		return
	}
	key.Secret = plaintext
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusCreated, key)
}

func (s *Server) handleListTenantKeys(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	keys, err := s.tenants.ListTenantAPIKeys(r.Context(), t.ID)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing api keys", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing api keys failed"))
		return
	}
	// The whole list is returned on one page, so the total is just the
	// page size; no separate count query is needed.
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(keys)))
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) handleRevokeTenantKey(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "key_id")
	if !ok {
		return
	}
	err := s.tenants.RevokeTenantAPIKey(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("api key %d not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("revoking api key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("revoking api key failed"))
		return
	}
	s.notifyAPIKeyInvalidation(r.Context(), id)
	writeCacheHeaders(w, cacheNoStore, 0, "")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleTenantUsage(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenantFromPath(w, r)
	if !ok {
		return
	}
	s.writeUsage(w, r, t, true)
}

// whoAmIResponse is the tenant's own view of its account: identity, what it
// may read, and the limits it is operating under.
type whoAmIResponse struct {
	Tenant   store.Tenant `json:"tenant"`
	Wildcard bool         `json:"wildcard"`
	// Grants is omitted for a wildcard tenant, for which the concept does
	// not apply — it reads everything, granted or not.
	Grants []string `json:"granted_contract_ids,omitempty"`
}

func (s *Server) handleWhoAmI(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	resp := whoAmIResponse{Tenant: p.Tenant, Wildcard: p.Scope.IsWildcard()}
	if !p.Scope.IsWildcard() {
		resp.Grants = p.Scope.Contracts()
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleOwnUsage(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	// Flush first so a tenant checking its own usage sees the requests it
	// just made rather than a figure up to one interval stale.
	s.usage.Flush(r.Context())
	s.writeUsage(w, r, p.Tenant, false)
}

// watchHeadroom is how much of a watch-list cap is in use. Limit is null when
// there is no cap: for a tenant, none of its own (it is still bounded by the
// instance-wide cap); for the instance, none configured.
type watchHeadroom struct {
	Held  int  `json:"held"`
	Limit *int `json:"limit"`
}

// usageResponse is the body of both usage endpoints.
//
// The instance-wide watch list is the union of every tenant's, so its size is
// aggregate data about other tenants. A tenant is therefore told only the one
// bit it can act on (InstanceWatchListFull: adding a contract nobody watches
// yet would be refused), while the operator view carries the numbers.
type usageResponse struct {
	Usage                    []store.TenantUsage `json:"usage"`
	WatchedContracts         watchHeadroom       `json:"watched_contracts"`
	InstanceWatchListFull    *bool               `json:"instance_watch_list_full,omitempty"`
	InstanceWatchedContracts *watchHeadroom      `json:"instance_watched_contracts,omitempty"`
}

// writeUsage writes the tenant's usage counters and its watch-list headroom.
// operator selects the admin view, which adds the instance-wide numbers.
func (s *Server) writeUsage(w http.ResponseWriter, r *http.Request, t store.Tenant, operator bool) {
	days := 30
	if raw := r.URL.Query().Get("days"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > maxUsageDays {
			writeError(w, http.StatusBadRequest,
				fmt.Errorf("days must be an integer in [1,%d]", maxUsageDays))
			return
		}
		days = n
	}
	resp, err := s.buildUsage(r.Context(), t, days, operator)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing usage", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing usage failed"))
		return
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, resp)
}

// buildUsage assembles the usage report: the day counters, the tenant's
// watch-list headroom and, depending on the audience, the instance-wide one.
func (s *Server) buildUsage(ctx context.Context, t store.Tenant, days int, operator bool) (usageResponse, error) {
	usage, err := s.tenants.ListUsage(ctx, t.ID, days)
	if err != nil {
		return usageResponse{}, fmt.Errorf("listing usage counters: %w", err)
	}
	held, err := s.tenants.CountTenantWatchedContracts(ctx, t.ID)
	if err != nil {
		return usageResponse{}, fmt.Errorf("counting the tenant watch list: %w", err)
	}
	resp := usageResponse{
		Usage:            usage,
		WatchedContracts: watchHeadroom{Held: held, Limit: t.MaxWatchedContracts},
	}
	if s.maxWatched == 0 && !operator {
		return resp, nil
	}

	instanceHeld, err := s.tenants.CountInstanceWatchedContracts(ctx)
	if err != nil {
		return usageResponse{}, fmt.Errorf("counting the instance watch list: %w", err)
	}
	var limit *int
	if s.maxWatched > 0 {
		capacity := s.maxWatched
		limit = &capacity
		if !operator {
			full := instanceHeld >= s.maxWatched
			resp.InstanceWatchListFull = &full
		}
	}
	if operator {
		resp.InstanceWatchedContracts = &watchHeadroom{Held: instanceHeld, Limit: limit}
	}
	return resp, nil
}

type watchRequest struct {
	ContractID string `json:"contract_id"`
	Network    string `json:"network,omitempty"`
}

func (s *Server) handleListOwnWatched(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	network := s.effectiveNetwork(r.URL.Query().Get("network"))
	s.writeOwnWatched(w, r, p.Tenant.ID, network)
}

// handleAddOwnWatched lets a tenant ask for a contract to be indexed.
//
// Watching is deliberately not the same thing as being granted read access:
// asking the ingester to fetch a contract is a resource request, whereas
// reading its events is an authorization question. A tenant that watches a
// contract it has not been granted causes rows to be ingested that it still
// cannot read. Auto-granting here would let any tenant give itself access to
// any contract on the network, which is the entire boundary undone by a
// convenience.
func (s *Server) handleAddOwnWatched(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	var req watchRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	if !config.ValidContractID(req.ContractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract_id %q", req.ContractID))
		return
	}
	network := s.effectiveNetwork(req.Network)
	err := s.tenants.AddTenantWatchedContract(r.Context(), p.Tenant, network, req.ContractID, s.maxWatched)
	if errors.Is(err, store.ErrQuotaExceeded) {
		writeError(w, http.StatusTooManyRequests, err)
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("adding watched contract", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("adding watched contract failed"))
		return
	}
	s.writeOwnWatched(w, r, p.Tenant.ID, network)
}

// handleRemoveOwnWatched drops this tenant's claim. Ingestion keeps
// following the contract if any other tenant still watches it — see
// watchedContractsUnion for why that needs no refcount.
func (s *Server) handleRemoveOwnWatched(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	contractID := chi.URLParam(r, "contract_id")
	// The id is checked before the store is touched so a typo in the path
	// reads as the client error it is, not as a 200 that looks like a
	// successful removal; the message matches the POST handler so one bad
	// id means one thing across both watch endpoints.
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract_id %q", contractID))
		return
	}
	// The delete is unconditional: a well-formed id the tenant does not
	// hold removes nothing and still answers 200 with the refreshed list,
	// so retries and concurrent removals stay idempotent instead of racing
	// into a 404.
	network := s.effectiveNetwork(r.URL.Query().Get("network"))
	if err := s.tenants.RemoveTenantWatchedContract(r.Context(), p.Tenant.ID, network, contractID); err != nil {
		loggerFromContext(r.Context()).Error("removing watched contract", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("removing watched contract failed"))
		return
	}
	s.writeOwnWatched(w, r, p.Tenant.ID, network)
}

func (s *Server) writeOwnWatched(w http.ResponseWriter, r *http.Request, tenantID int64, network string) {
	watched, err := s.tenants.ListTenantWatchedContracts(r.Context(), tenantID, network)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing watched contracts", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing watched contracts failed"))
		return
	}
	// The whole list is returned on one page, so the total is just the
	// page size; no separate count query is needed.
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(watched)))
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, map[string]any{"contract_ids": watched})
}

// tenantFromPath loads the tenant named by the {id} path parameter. Admin
// routes only — self-service handlers take the ID from the principal so a
// tenant cannot address another by changing the URL.
func (s *Server) tenantFromPath(w http.ResponseWriter, r *http.Request) (store.Tenant, bool) {
	id, ok := pathInt64(w, r, "id")
	if !ok {
		return store.Tenant{}, false
	}
	t, err := s.tenants.GetTenant(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("tenant %d not found", id))
		return store.Tenant{}, false
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("loading tenant", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading tenant failed"))
		return store.Tenant{}, false
	}
	return t, true
}

func pathInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := chi.URLParam(r, name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid %s %q", name, raw))
		return 0, false
	}
	return id, true
}
