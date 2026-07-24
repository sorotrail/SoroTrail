package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/config"
)

// contractMetaResponse is the JSON shape for a single contract's metadata.
// All fields except contract_id are omitempty so null/missing metadata is
// cleanly absent from the response rather than showing as "name": null.
type contractMetaResponse struct {
	ContractID string  `json:"contract_id"`
	Name       *string `json:"name,omitempty"`
	Symbol     *string `json:"symbol,omitempty"`
	Decimals   *int    `json:"decimals,omitempty"`
}

// contractsResponse is the JSON shape for GET /contracts.
type contractsResponse struct {
	Contracts []contractMetaResponse `json:"contracts"`
}

// contractStatsResponse is the JSON shape for GET /contracts/{id}/stats.
type contractStatsResponse struct {
	ContractID string  `json:"contract_id"`
	Name       *string `json:"name,omitempty"`
	Symbol     *string `json:"symbol,omitempty"`
	Decimals   *int    `json:"decimals,omitempty"`
	EventCount int64   `json:"event_count"`
}

// handleListContracts returns all contracts seen by the indexer with their
// cached metadata (null when unknown).
func (s *Server) handleListContracts(w http.ResponseWriter, r *http.Request) {
	contractIDs, err := s.store.ListContractIDs(r.Context())
	if err != nil {
		s.log.Error("listing contract IDs", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing contracts failed"))
		return
	}

	contracts := make([]contractMetaResponse, 0, len(contractIDs))
	for _, cid := range contractIDs {
		cr := contractMetaResponse{ContractID: cid}
		if meta, err := s.store.GetContractMeta(r.Context(), cid); err == nil && meta.HasMetadata() {
			cr.Name = meta.Name
			cr.Symbol = meta.Symbol
			cr.Decimals = meta.Decimals
		}
		contracts = append(contracts, cr)
	}

	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, contractsResponse{Contracts: contracts})
}

// handleContractStats returns per-contract statistics with cached metadata.
func (s *Server) handleContractStats(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}

	// Verify the contract exists (has at least one event).
	count, err := s.store.CountContractEvents(r.Context(), contractID)
	if err != nil {
		s.log.Error("counting events for contract", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading contract stats failed"))
		return
	}
	if count == 0 {
		writeError(w, http.StatusNotFound, fmt.Errorf("contract %q not found", contractID))
		return
	}

	resp := contractStatsResponse{
		ContractID: contractID,
		EventCount: count,
	}

	// Attach metadata if available.
	if meta, err := s.store.GetContractMeta(r.Context(), contractID); err == nil && meta.HasMetadata() {
		resp.Name = meta.Name
		resp.Symbol = meta.Symbol
		resp.Decimals = meta.Decimals
	}

	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, resp)
}
