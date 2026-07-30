package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

var cachePrivate atomic.Bool

func SetCachePrivate(v bool) { cachePrivate.Store(v) }

const immutableMaxAge = 365 * 24 * time.Hour

type cacheability int

const (
	cacheImmutable cacheability = iota
	cacheNoCache
	cacheNoStore
)

type errorResponse struct {
	Error string `json:"error"`
}

type eventsResponse struct {
	Events []store.Event `json:"events"`
	Cursor string        `json:"cursor,omitempty"`
}

type enrichedEventsResponse struct {
	Events []store.EnrichedEvent `json:"events"`
	Cursor string                `json:"cursor,omitempty"`
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: map[string]string{"database": "ok"}}
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
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, resp)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	filter, err := s.filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.serveEvents(w, r, filter)
}

func (s *Server) handleContractEvents(w http.ResponseWriter, r *http.Request) {
	filter, err := s.filterFromQuery(r)
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
	policy, etag, err := s.listCachePolicy(r.Context(), filter)
	if err != nil {
		s.log.Warn("deciding list cache policy", "error", err)
	} else if etag != "" && ifNoneMatch(r, etag) {
		writeNotModified(w, etag, policy)
		return
	}

	events, cursor, qerr := s.store.QueryEvents(r.Context(), filter)
	if qerr != nil {
		s.log.Error("querying events", "error", qerr)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}

	decoded := r.URL.Query().Get("decoded") == "true"
	if decoded && s.enricher != nil {
		enriched := s.enricher.EnrichEvents(r.Context(), events)
		writeJSON(w, http.StatusOK, enrichedEventsResponse{Events: enriched, Cursor: cursor})
		return
	}
	writeCacheHeaders(w, policy, immutableMaxAge, etag)
	writeJSON(w, http.StatusOK, eventsResponse{Events: events, Cursor: cursor})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	etag := `"` + id + `"`

	if ifNoneMatch(r, etag) {
		exists, err := s.store.EventExists(r.Context(), id)
		if err != nil {
			s.log.Error("checking event existence", "id", id, "error", err)
			writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
			return
		}
		if !exists {
			writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
			return
		}
		writeNotModified(w, etag, cacheImmutable)
		return
	}

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

	decoded := r.URL.Query().Get("decoded") == "true"
	if decoded && s.enricher != nil {
		enriched := s.enricher.EnrichEvents(r.Context(), []store.Event{event})
		if len(enriched) > 0 {
			writeJSON(w, http.StatusOK, enriched[0])
			return
		}
	}
	writeCacheHeaders(w, cacheImmutable, immutableMaxAge, etag)
	writeJSON(w, http.StatusOK, event)
}

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
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) listCachePolicy(ctx context.Context, filter store.EventFilter) (cacheability, string, error) {
	if filter.ToLedger <= 0 {
		return cacheNoCache, "", nil
	}
	frontier, err := s.lastIngestedLedger(ctx)
	if err != nil {
		return cacheNoCache, "", err
	}
	if filter.ToLedger >= frontier {
		return cacheNoCache, "", nil
	}
	return cacheImmutable, listETag(filter), nil
}

func (s *Server) lastIngestedLedger(ctx context.Context) (int64, error) {
	// Use the first network for cache policy. In multi-network scenario
	// the frontier is network-specific, but the cache policy is conservative
	// (falls back to no-cache on uncertainty).
	network := "default"
	if len(s.NetworkNames) > 0 {
		network = s.NetworkNames[0]
	}
	state, err := s.store.GetIngestionState(ctx, network)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return state.LastIngestedLedger, nil
}

func listETag(f store.EventFilter) string {
	key := struct {
		ContractID string          `json:"c"`
		Type       string          `json:"t"`
		Topic      json.RawMessage `json:"p,omitempty"`
		FromLedger int64           `json:"fl"`
		ToLedger   int64           `json:"tl"`
		FromTime   string          `json:"ft,omitempty"`
		ToTime     string          `json:"tt,omitempty"`
		Cursor     string          `json:"cu,omitempty"`
		Limit      int             `json:"l"`
		Order      string          `json:"o,omitempty"`
		Network    string          `json:"n,omitempty"`
	}{
		ContractID: f.ContractID,
		Type:       f.Type,
		Topic:      f.Topic,
		FromLedger: f.FromLedger,
		ToLedger:   f.ToLedger,
		FromTime:   timeOrEmpty(f.FromTime),
		ToTime:     timeOrEmpty(f.ToTime),
		Cursor:     f.Cursor,
		Limit:      resolvedLimit(f.Limit),
		Order:      resolvedOrder(f.Order),
		Network:    f.Network,
	}
	b, _ := json.Marshal(key)
	sum := sha256.Sum256(b)
	return fmt.Sprintf(`"%x"`, sum)
}

func resolvedLimit(n int) int {
	if n <= 0 {
		return store.DefaultQueryLimit
	}
	return n
}

func resolvedOrder(o string) string {
	if o == "" {
		return "asc"
	}
	return o
}

func timeOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func ifNoneMatch(r *http.Request, etag string) bool {
	raw := r.Header.Get("If-None-Match")
	if raw == "" || etag == "" {
		return false
	}
	if strings.TrimSpace(raw) == "*" {
		return true
	}
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if strings.TrimPrefix(t, "W/") == etag {
			return true
		}
	}
	return false
}

func writeCacheHeaders(w http.ResponseWriter, kind cacheability, maxAge time.Duration, etag string) {
	vary := w.Header().Get("Vary")
	if !strings.Contains(vary, "Accept-Encoding") {
		if vary == "" {
			vary = "Accept-Encoding"
		} else {
			vary = vary + ", Accept-Encoding"
		}
		w.Header().Set("Vary", vary)
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	switch kind {
	case cacheNoStore:
		w.Header().Set("Cache-Control", "no-store")
	case cacheNoCache:
		w.Header().Set("Cache-Control", "no-cache")
	case cacheImmutable:
		scope := "public"
		if cachePrivate.Load() {
			scope = "private"
		}
		w.Header().Set("Cache-Control",
			fmt.Sprintf("%s, max-age=%d, immutable", scope, int(maxAge.Seconds())))
	}
}

func writeNotModified(w http.ResponseWriter, etag string, kind cacheability) {
	w.Header().Set("Content-Type", "application/json")
	writeCacheHeaders(w, kind, immutableMaxAge, etag)
	w.WriteHeader(http.StatusNotModified)
}

// filterFromQuery parses the shared event-filter query params including network.
func (s *Server) filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()
	f := store.EventFilter{
		ContractID: q.Get("contract_id"),
		Cursor:     q.Get("cursor"),
		Network:    q.Get("network"),
	}

	// Validate network parameter.
	if f.Network == "" && s.HasMultipleNetworks {
		return f, fmt.Errorf("network is required when multiple networks are configured")
	}
	if f.Network != "" && len(s.NetworkNames) > 0 {
		valid := false
		for _, name := range s.NetworkNames {
			if f.Network == name {
				valid = true
				break
			}
		}
		if !valid {
			return f, fmt.Errorf("invalid network %q (valid: %v)", f.Network, s.NetworkNames)
		}
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

	parseTopic := func(name, raw string) (json.RawMessage, error) {
		if raw == "" {
			return nil, nil
		}
		if json.Valid([]byte(raw)) {
			return json.RawMessage(raw), nil
		}
		quoted, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", name, err)
		}
		return quoted, nil
	}

	order := q.Get("order")
	switch order {
	case "", "asc", "desc":
		f.Order = order
	default:
		return f, fmt.Errorf("invalid order %q (want asc or desc)", order)
	}

	var err error
	if topic := q.Get("topic"); topic != "" {
		parsed, err := parseTopic("topic", topic)
		if err != nil {
			return f, err
		}
		f.Topic = parsed
	}

	if f.Topic0, err = parseTopic("topic0", q.Get("topic0")); err != nil {
		return f, err
	}
	if f.Topic1, err = parseTopic("topic1", q.Get("topic1")); err != nil {
		return f, err
	}
	if f.Topic2, err = parseTopic("topic2", q.Get("topic2")); err != nil {
		return f, err
	}
	if f.Topic3, err = parseTopic("topic3", q.Get("topic3")); err != nil {
		return f, err
	}

	if len(f.Topic) > 0 && (len(f.Topic0) > 0 || len(f.Topic1) > 0 || len(f.Topic2) > 0 || len(f.Topic3) > 0) {
		return f, fmt.Errorf("topic and topic0..topic3 filters cannot be combined")
	}

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

func (s *Server) handleEventStreamWS(w http.ResponseWriter, r *http.Request) {
	if s.bcast == nil {
		http.Error(w, "streaming not configured", http.StatusNotImplemented)
		return
	}

	filter, err := s.filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.log.Error("websocket accept", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	sub := s.bcast.Subscribe(filter)
	defer sub.Close()

	ctx := r.Context()
	ctx = c.CloseRead(ctx)

	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				pCtx, cancel := context.WithTimeout(pingCtx, 5*time.Second)
				err := c.Ping(pCtx)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				s.log.Error("marshal event for ws", "error", err)
				continue
			}
			err = c.Write(ctx, websocket.MessageText, data)
			if err != nil {
				return
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
