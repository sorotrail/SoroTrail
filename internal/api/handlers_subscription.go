package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorotrail/internal/store"
)

// --- Subscription CRUD handlers ---

// subscriptionOwner is the ownership filter for this request.
//
// Single-tenant mode and admin tenants see every subscription; anyone else
// sees only their own. Like the read scope, this is resolved once and handed
// to the store rather than checked after the fact, so a tenant cannot read,
// modify or delete a callback belonging to someone else.
func (s *Server) subscriptionOwner(r *http.Request) store.SubscriptionOwner {
	p, ok := PrincipalFrom(r.Context())
	if !ok {
		// No principal should be impossible behind the router, but if it
		// happens the safe answer is an owner that matches nothing.
		return store.SubscriptionOwner{}
	}
	if p.Untenanted || p.Tenant.Admin {
		return store.AllSubscriptions()
	}
	return store.OwnedBy(p.Tenant.ID)
}

// authorizeSubscriptionFilters enforces that a tenant may only subscribe to
// events it is entitled to read.
//
// This is the check that keeps webhook delivery inside the tenant boundary.
// A subscription is a read path whose sink is a URL the subscriber chooses,
// so an unconstrained one is an exfiltration primitive: subscribe to a
// contract you cannot read and have its events posted to your own server.
// Requiring a non-wildcard tenant to name a granted contract means the
// subscription can only ever match events its owner could have fetched from
// /events anyway — which is what lets the delivery worker run unowned.
func authorizeSubscriptionFilters(r *http.Request, f store.SubscriptionFilter) error {
	p, ok := PrincipalFrom(r.Context())
	if !ok {
		return errors.New("unauthenticated")
	}
	if p.Untenanted || p.Scope.IsWildcard() {
		return nil
	}
	if f.ContractID == "" {
		return errors.New(
			"filters.contract_id is required: a subscription must name one of the " +
				"contracts granted to this tenant")
	}
	if !p.Scope.Allows(f.ContractID) {
		return errForbiddenContract{contractID: f.ContractID}
	}
	return nil
}

// createSubscriptionRequest is the JSON body for POST /subscriptions.
type createSubscriptionRequest struct {
	URL     string                   `json:"url"`
	Filters store.SubscriptionFilter `json:"filters"`
	Secret  string                   `json:"secret"`
	Enabled *bool                    `json:"enabled,omitempty"` // defaults to true
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, errors.New("url is required"))
		return
	}
	if req.Secret == "" {
		writeError(w, http.StatusBadRequest, errors.New("secret is required"))
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := authorizeSubscriptionFilters(r, req.Filters); err != nil {
		writeFilterError(w, err)
		return
	}

	sub := store.Subscription{
		URL:     req.URL,
		Filters: req.Filters,
		Secret:  req.Secret,
		Enabled: enabled,
	}
	// Ownership comes from the credential, never from the request body.
	if p, ok := PrincipalFrom(r.Context()); ok && !p.Untenanted {
		tenantID := p.Tenant.ID
		sub.TenantID = &tenantID
	}
	created, err := s.store.CreateSubscription(r.Context(), sub)
	if err != nil {
		loggerFromContext(r.Context()).Error("creating subscription", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("creating subscription failed"))
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := parseSubscriptionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sub, err := s.store.GetSubscription(r.Context(), id, s.subscriptionOwner(r))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("subscription %d not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("getting subscription", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("getting subscription failed"))
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := s.store.ListSubscriptions(r.Context(), s.subscriptionOwner(r))
	if err != nil {
		loggerFromContext(r.Context()).Error("listing subscriptions", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing subscriptions failed"))
		return
	}
	writeJSON(w, http.StatusOK, subs)
}

type updateSubscriptionRequest struct {
	URL     *string                   `json:"url,omitempty"`
	Filters *store.SubscriptionFilter `json:"filters,omitempty"`
	Secret  *string                   `json:"secret,omitempty"`
	Enabled *bool                     `json:"enabled,omitempty"`
}

func (s *Server) handleUpdateSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := parseSubscriptionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	owner := s.subscriptionOwner(r)
	existing, err := s.store.GetSubscription(r.Context(), id, owner)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("subscription %d not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("getting subscription for update", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("getting subscription failed"))
		return
	}

	var req updateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}

	if req.URL != nil {
		existing.URL = *req.URL
	}
	if req.Filters != nil {
		existing.Filters = *req.Filters
	}
	if req.Secret != nil {
		existing.Secret = *req.Secret
	}
	if req.Enabled != nil {
		existing.Enabled = *req.Enabled
	}
	if existing.URL == "" {
		writeError(w, http.StatusBadRequest, errors.New("url must not be empty"))
		return
	}
	// Re-checked after the merge: an update that widens the filters must
	// clear the same bar the original creation did.
	if err := authorizeSubscriptionFilters(r, existing.Filters); err != nil {
		writeFilterError(w, err)
		return
	}

	updated, err := s.store.UpdateSubscription(r.Context(), existing, owner)
	if err != nil {
		loggerFromContext(r.Context()).Error("updating subscription", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("updating subscription failed"))
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := parseSubscriptionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.DeleteSubscription(r.Context(), id, s.subscriptionOwner(r)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("subscription %d not found", id))
			return
		}
		loggerFromContext(r.Context()).Error("deleting subscription", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting subscription failed"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Delivery attempts ---

func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	id, err := parseSubscriptionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Verify the subscription exists and belongs to the caller.
	owner := s.subscriptionOwner(r)
	if _, err := s.store.GetSubscription(r.Context(), id, owner); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("subscription %d not found", id))
		return
	}

	limit := store.DefaultQueryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		l, err := strconv.Atoi(raw)
		if err != nil || l < 1 || l > maxLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit))
			return
		}
		limit = l
	}

	attempts, err := s.store.ListDeliveryAttempts(r.Context(), id, limit, owner)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing delivery attempts", "subscription_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing delivery attempts failed"))
		return
	}
	writeJSON(w, http.StatusOK, attempts)
}

func parseSubscriptionID(r *http.Request) (int64, error) {
	raw := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("subscription id must be a positive integer, got %q", raw)
	}
	return id, nil
}
