package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/spec"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Contract spec override endpoints (user-supplied contract spec, task #158).
//
// Some contracts do not expose a fetchable spec (no contractspecv0 section,
// unreachable wasm, etc.). These endpoints let an operator upload a spec JSON
// per contract_id; the spec.Enricher prefers the override over the
// RPC-fetched spec whenever one exists.
//
// All routes are gated by apiKeyAuth (same as watched-contracts): a spec
// override silently changes how events decode for that contract, so writes
// are never open.
//
// Every write is also recorded in an append-only history (see migration
// 0029), so "did someone change the override?" has an answer: the history
// records the spec, the API key prefix that made the change, and when.

type specOverrideResponse struct {
	ContractID string          `json:"contract_id"`
	Spec       json.RawMessage `json:"spec"`
	UpdatedAt  string          `json:"updated_at,omitempty"`
}

type specOverrideDeleteResponse struct {
	ContractID string `json:"contract_id"`
	Deleted    bool   `json:"deleted"`
}

// specOverrideHistoryResponse is the body of GET /contracts/{id}/spec/history.
type specOverrideHistoryResponse struct {
	Versions []store.ContractSpecOverrideVersion `json:"versions"`
	// Count is the number of versions on this page.
	Count int `json:"count"`
	// Cursor is the opaque cursor for the next (older) page. Empty when
	// this is the last page.
	Cursor string `json:"cursor,omitempty"`
}

// handlePutContractSpecOverride accepts {"spec": {...}} (or a bare spec
// object), validates the shape via spec.ParseOverrideSpec, and stores it.
func (s *Server) handlePutContractSpecOverride(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}

	// readJSONBody drains r.Body once, so both the wrapped and the bare
	// shape are decoded from the same bytes: r.Body is a stream, so a
	// second read would see an empty request. The oversized-body case is
	// reported as 413; everything else that cannot be decoded is 400.
	raw, err := readJSONBody(r, s.effectiveRequestBodyLimit(r))
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, err)
		return
	}
	var body struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err := decodeJSONBytes(raw, &body); err != nil {
		// Retry as a bare spec object: {"events": [...]} without the wrapper.
		var bare json.RawMessage
		if bareErr := decodeJSONBytes(raw, &bare); bareErr != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}
		body.Spec = bare
	}
	if len(body.Spec) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("request body must contain a \"spec\" object"))
		return
	}

	// Validate the spec shape BEFORE storing — invalid specs are rejected.
	parsed, err := spec.ParseOverrideSpec(body.Spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_ = parsed

	if err := s.store.SetContractSpecOverrideWithHistory(r.Context(), contractID, body.Spec, s.specOverrideKeyPrefix()); err != nil {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, err)
			return
		}
		s.log.Error("saving contract spec override", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("saving contract spec override failed"))
		return
	}

	// The cached override would otherwise keep decoding events the old way
	// for the life of the process: an override never expires on its own.
	s.invalidateSpecContract(contractID)
	s.publishSpecInvalidation(r.Context(), contractID)

	// A write whose result depends on operator state must never be cached.
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideResponse{
		ContractID: contractID,
		Spec:       body.Spec,
	})
}

// handleGetContractSpecOverride returns the stored override, or 404 when none.
func (s *Server) handleGetContractSpecOverride(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}

	data, err := s.store.GetContractSpecOverride(r.Context(), contractID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("no spec override for contract %q", contractID))
			return
		}
		s.log.Error("loading contract spec override", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading contract spec override failed"))
		return
	}

	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideResponse{
		ContractID: contractID,
		Spec:       json.RawMessage(data),
	})
}

// handleDeleteContractSpecOverride removes the override. Idempotent.
func (s *Server) handleDeleteContractSpecOverride(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}

	err := s.store.DeleteContractSpecOverrideWithHistory(r.Context(), contractID, s.specOverrideKeyPrefix())
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, err)
			return
		}
		s.log.Error("deleting contract spec override", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting contract spec override failed"))
		return
	}

	s.invalidateSpecContract(contractID)
	s.publishSpecInvalidation(r.Context(), contractID)

	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideDeleteResponse{ContractID: contractID, Deleted: true})
}

// handleListContractSpecOverrideHistory lists a contract's override history,
// newest first, with cursor pagination.
//
// The history is append-only: every PUT and DELETE appends an entry, and so
// does a revert. Entries carry the spec that was in force (absent for a
// tombstone, meaning the override was deleted), the API key prefix that made
// the change, and when.
func (s *Server) handleListContractSpecOverrideHistory(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}

	limit := store.DefaultQueryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit))
			return
		}
		limit = n
	}
	cursor := r.URL.Query().Get("cursor")

	versions, next, err := s.store.ListContractSpecOverrideHistory(r.Context(), contractID, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, err)
			return
		}
		if errors.Is(err, store.ErrInvalidCursor) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		s.log.Error("listing contract spec override history", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing contract spec override history failed"))
		return
	}
	if versions == nil {
		versions = []store.ContractSpecOverrideVersion{}
	}

	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideHistoryResponse{
		Versions: versions,
		Count:    len(versions),
		Cursor:   next,
	})
}

// specOverrideRevertRequest is the JSON body for POST /contracts/{id}/spec/revert.
type specOverrideRevertRequest struct {
	// Version is the id of the history entry to restore.
	Version int64 `json:"version"`
}

// handleRevertContractSpecOverride restores a previous version of the
// override, identified by the version id in the request body.
//
// The revert is itself a change, so it is recorded as a new history entry:
// the history stays append-only and keeps the whole sequence, including the
// way back.
func (s *Server) handleRevertContractSpecOverride(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}

	var req specOverrideRevertRequest
	if s.decodeJSONBody(w, r, &req) {
		return
	}
	if req.Version <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("version must be a positive integer"))
		return
	}
	versionID := req.Version

	// The version is located by walking the history, newest first. The
	// walk is bounded by the page size, so a very old version is reached
	// by paging — a caller that wants it asks for the page it is on.
	versions, _, err := s.store.ListContractSpecOverrideHistory(r.Context(), contractID, maxLimit, "")
	if err != nil {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, err)
			return
		}
		s.log.Error("listing contract spec override history for revert", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing contract spec override history failed"))
		return
	}
	var target *store.ContractSpecOverrideVersion
	for i := range versions {
		if versions[i].ID == versionID {
			target = &versions[i]
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound,
			fmt.Errorf("no spec override version %d for contract %q", versionID, contractID))
		return
	}

	// A tombstone version means the override was deleted at that point in
	// the history, so reverting to it means deleting the override again.
	if len(target.Spec) == 0 {
		err = s.store.DeleteContractSpecOverrideWithHistory(r.Context(), contractID, s.specOverrideKeyPrefix())
	} else {
		err = s.store.SetContractSpecOverrideWithHistory(r.Context(), contractID, target.Spec, s.specOverrideKeyPrefix())
	}
	if err != nil {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusNotImplemented, err)
			return
		}
		s.log.Error("reverting contract spec override", "contract_id", contractID, "version", versionID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("reverting contract spec override failed"))
		return
	}

	s.invalidateSpecContract(contractID)
	s.publishSpecInvalidation(r.Context(), contractID)

	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideResponse{
		ContractID: contractID,
		Spec:       target.Spec,
	})
}

// specOverrideKeyPrefix returns the public prefix of the API key that
// authorised a spec-override change. It is recorded in the override history
// so an operator can tell which key made a change; the secret half of the
// key is never stored or logged.
func (s *Server) specOverrideKeyPrefix() string {
	prefix, _, ok := apikey.Parse(s.apiKey)
	if !ok {
		return "unknown"
	}
	return prefix
}

// invalidateSpecContract evicts the contract's cached spec and override after
// a write. The enricher is optional — a server without one has no cache to
// invalidate — so that path is skipped in that case. The cache wired through
// SetSpecCache is invalidated the same way, so a deployment that supplies the
// cache without an enricher still drops the stale entry.
func (s *Server) invalidateSpecContract(contractID string) {
	if s.enricher != nil {
		if inv, ok := s.enricher.(SpecOverrideInvalidator); ok {
			inv.InvalidateContract(contractID)
		}
	}
	// SetSpecCache wires the cache as a SpecCacheStatsSource; invalidate
	// through it when it also implements the cache's invalidation methods.
	if sc := getSpecCache(); sc != nil {
		type cacheInvalidator interface {
			InvalidateContract(string)
			InvalidateOverride(string)
		}
		if inv, ok := sc.(cacheInvalidator); ok {
			inv.InvalidateContract(contractID)
			inv.InvalidateOverride(contractID)
		}
	}
}

// SpecOverrideInvalidator evicts a contract's cached spec and override.
// The spec.Enricher implements it; the API server discovers it by type
// assertion so the enricher stays an optional dependency.
type SpecOverrideInvalidator interface {
	InvalidateContract(contractID string)
}

// publishSpecInvalidation tells the other replicas that the contract's
// cached spec must be evicted.
//
// The spec cache is per-process, so without this the replicas that did not
// serve the write keep decoding the contract the old way until their own
// cache expires — the same request would then decode differently depending
// on which replica served it.
func (s *Server) publishSpecInvalidation(ctx context.Context, contractID string) {
	if s.specInvalidationPublisher == nil {
		return
	}
	if err := s.specInvalidationPublisher.PublishSpecInvalidation(ctx, contractID); err != nil {
		// The local cache is already correct; the other replicas fall back
		// to their own expiry. It is logged rather than swallowed so the
		// fan-out is observable when it breaks.
		s.log.Warn("publishing the spec invalidation failed",
			"contract_id", contractID,
			"error", err,
		)
	}
}
