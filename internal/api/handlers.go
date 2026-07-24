package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

type errorResponse struct {
	Error string `json:"error"`
}

type eventsResponse struct {
	Events []store.Event `json:"events"`
	// Cursor is non-empty when another page exists; pass it back as ?cursor=.
	Cursor string `json:"cursor,omitempty"`
}

type healthResponse struct {
	Status string            `json:"status"` // ok | degraded
	Checks map[string]string `json:"checks"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: map[string]string{"database": "ok", "rpc": "ok"}}
	status := http.StatusOK

	if err := s.store.Ping(ctx); err != nil {
		resp.Status, resp.Checks["database"] = "degraded", err.Error()
		status = http.StatusServiceUnavailable
	}
	if health, err := s.rpc.GetHealth(ctx); err != nil {
		resp.Status, resp.Checks["rpc"] = "degraded", err.Error()
		status = http.StatusServiceUnavailable
	} else if health.Status != "healthy" {
		resp.Status, resp.Checks["rpc"] = "degraded", fmt.Sprintf("rpc reports %q", health.Status)
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.serveEvents(w, r, filter)
}

func (s *Server) handleContractEvents(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}
	filter.ContractID = contractID
	s.serveEvents(w, r, filter)
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter) {
	events, cursor, err := s.store.QueryEvents(r.Context(), filter)
	if err != nil {
		s.log.Error("querying events", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{Events: events, Cursor: cursor})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	event, err := s.store.GetEvent(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		s.log.Error("loading event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}
	writeJSON(w, http.StatusOK, event)
}

// Stats summarizes what the indexer has stored plus, when the auditor is
// running, the post-processing counters it has accumulated.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.log.Error("loading stats", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading stats failed"))
		return
	}
	if a := getAuditor(); a != nil {
		m := a.Metrics()
		stats.Auditor = store.AuditStats{
			PassesRun:             m.PassesRun,
			LedgersChecked:        m.LedgersChecked,
			FindingsOpened:        m.FindingsOpened,
			FindingsRepaired:      m.FindingsRepaired,
			FindingsUnverifiable:  m.FindingsUnverifiable,
			FindingsUnrecoverable: m.FindingsUnrecoverable,
			RPCRequests:           m.RPCRequests,
		}
	}
	writeJSON(w, http.StatusOK, stats)
}

// filterFromQuery parses the shared event-filter query params:
// contract_id, type, topic, from_ledger, to_ledger, from_time, to_time, cursor, limit.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()
	f := store.EventFilter{
		ContractID: q.Get("contract_id"),
		Cursor:     q.Get("cursor"),
	}

	if f.ContractID != "" && !config.ValidContractID(f.ContractID) {
		return f, fmt.Errorf("invalid contract_id %q", f.ContractID)
	}

	switch t := q.Get("type"); t {
	case "", "contract", "system", "diagnostic":
		f.Type = t
	default:
		return f, fmt.Errorf("invalid type %q (want contract|system|diagnostic)", t)
	}

	// topic accepts any JSON value; a bare word like `transfer` is treated
	// as the JSON string "transfer". Matching is exact against the stored
	// topic entries, e.g. topic={"symbol":"transfer"} for XDR-decoded rows.
	if topic := q.Get("topic"); topic != "" {
		if json.Valid([]byte(topic)) {
			f.Topic = json.RawMessage(topic)
		} else {
			quoted, err := json.Marshal(topic)
			if err != nil {
				return f, fmt.Errorf("invalid topic: %w", err)
			}
			f.Topic = quoted
		}
	}

	// topic_contains accepts any valid JSON value and uses @> containment
	// directly (no automatic array-wrapping). Unlike topic, bare words are
	// not allowed — the input must be parseable JSON.
	if raw := q.Get("topic_contains"); raw != "" {
		if !json.Valid([]byte(raw)) {
			return f, fmt.Errorf("topic_contains must be valid JSON")
		}
		f.TopicContains = json.RawMessage(raw)
	}

	// order controls sort direction for paginated results.
	order := q.Get("order")
	switch order {
	case "", "asc", "desc":
		f.Order = order
	default:
		return f, fmt.Errorf("invalid order %q (want asc or desc)", order)
	}

	var err error
	if f.FromLedger, err = parseLedgerParam(q.Get("from_ledger"), "from_ledger"); err != nil {
		return f, err
	}
	if f.ToLedger, err = parseLedgerParam(q.Get("to_ledger"), "to_ledger"); err != nil {
		return f, err
	}
	if f.FromLedger > 0 && f.ToLedger > 0 && f.FromLedger > f.ToLedger {
		return f, fmt.Errorf("from_ledger %d is after to_ledger %d", f.FromLedger, f.ToLedger)
	}

	if f.FromTime, err = parseTimeParam(q.Get("from_time"), "from_time"); err != nil {
		return f, err
	}
	if f.ToTime, err = parseTimeParam(q.Get("to_time"), "to_time"); err != nil {
		return f, err
	}
	if !f.FromTime.IsZero() && !f.ToTime.IsZero() && f.FromTime.After(f.ToTime) {
		return f, fmt.Errorf("from_time %s is after to_time %s",
			f.FromTime.Format(time.RFC3339), f.ToTime.Format(time.RFC3339))
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > store.MaxQueryLimit {
			return f, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit)
		}
		f.Limit = limit
	}
	return f, nil
}

func parseLedgerParam(raw, name string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

// parseTimeParam parses an RFC 3339 timestamp query parameter.
// Sub-second precision and missing timezone offset are rejected.
func parseTimeParam(raw, name string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 timestamp (e.g. 2026-07-21T00:00:00Z)", name)
	}
	if t.Nanosecond() != 0 {
		return time.Time{}, fmt.Errorf("%s sub-second precision is not supported", name)
	}
	return t, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
