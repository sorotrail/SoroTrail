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

	sub := store.Subscription{
		URL:     req.URL,
		Filters: req.Filters,
		Secret:  req.Secret,
		Enabled: enabled,
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
	sub, err := s.store.GetSubscription(r.Context(), id)
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
	subs, err := s.store.ListSubscriptions(r.Context())
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

	existing, err := s.store.GetSubscription(r.Context(), id)
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

	updated, err := s.store.UpdateSubscription(r.Context(), existing)
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
	if err := s.store.DeleteSubscription(r.Context(), id); err != nil {
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

	// Verify the subscription exists.
	if _, err := s.store.GetSubscription(r.Context(), id); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("subscription %d not found", id))
		return
	}

	limit := store.DefaultQueryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		l, err := strconv.Atoi(raw)
		if err != nil || l < 1 || l > store.MaxQueryLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit))
			return
		}
		limit = l
	}

	attempts, err := s.store.ListDeliveryAttempts(r.Context(), id, limit)
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
