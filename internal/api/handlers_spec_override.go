package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

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

type specOverrideResponse struct {
	ContractID string          `json:"contract_id"`
	Spec       json.RawMessage `json:"spec"`
	UpdatedAt  string          `json:"updated_at,omitempty"`
}

type specOverrideDeleteResponse struct {
	ContractID string `json:"contract_id"`
	Deleted    bool   `json:"deleted"`
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

	var body struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		// Retry as a bare spec object: {"events": [...]} without the wrapper.
		var bare json.RawMessage
		if bareErr := decodeJSONBody(r, &bare); bareErr != nil {
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

	if err := s.store.SetContractSpecOverride(r.Context(), contractID, body.Spec); err != nil {
		s.log.Error("saving contract spec override", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("saving contract spec override failed"))
		return
	}

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

	err := s.store.DeleteContractSpecOverride(r.Context(), contractID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.log.Error("deleting contract spec override", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting contract spec override failed"))
		return
	}

	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, specOverrideDeleteResponse{ContractID: contractID, Deleted: true})
}
