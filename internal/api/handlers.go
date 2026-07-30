package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/buildinfo"
	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// decodeJSONBody parses a single small JSON body (≤4 KiB), rejecting
// unknown fields so a typo like {"contractID": "..."} doesn't fall
// through with an empty contract_id and a confusing 400 from a later
// check.
func decodeJSONBody(r *http.Request, dst any) error {
	if r.Body == nil {
		return errors.New("request body is empty")
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

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

type eventsWithXDRResponse struct {
	Events []eventWithXDR `json:"events"`
	Cursor string         `json:"cursor,omitempty"`
}

type enrichedEventsWithXDRResponse struct {
	Events []enrichedEventWithXDR `json:"events"`
	Cursor string                 `json:"cursor,omitempty"`
}

type eventWithXDR struct {
	store.Event
	TopicsXDR []string `json:"topics_xdr"`
	ValueXDR  *string  `json:"value_xdr"`
}

type enrichedEventWithXDR struct {
	eventWithXDR
	DecodedEvent *store.DecodedEventResponse `json:"decoded_event,omitempty"`
	Decoded      bool                        `json:"decoded"`
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

type versionResponse struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

// eventFieldNames is the set of JSON keys on store.Event that the ?fields=
// allowlist accepts.
var eventFieldNames = map[string]bool{
	"id":                 true,
	"contract_id":        true,
	"ledger":             true,
	"type":               true,
	"tx_hash":            true,
	"tx_index":           true,
	"op_index":           true,
	"in_successful_call": true,
	"topics":             true,
	"value":              true,
	"created_at":         true,
}

// parseFields splits a comma-separated ?fields= value and returns the
// allowlist set. Unknown field names are rejected with a 400-style error.
func parseFields(raw string) (map[string]bool, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	set := make(map[string]bool, len(parts))
	for _, p := range parts {
		f := strings.TrimSpace(p)
		if f == "" {
			continue
		}
		if !eventFieldNames[f] {
			return nil, fmt.Errorf("unknown field %q (valid: id, contract_id, ledger, type, tx_hash, tx_index, op_index, in_successful_call, topics, value, created_at)", f)
		}
		set[f] = true
	}
	if len(set) == 0 {
		return nil, nil
	}
	return set, nil
}

// projectEvent returns the event unchanged when fields is nil, or a
// map[string]any containing only the requested keys.
func projectEvent(ev store.Event, fields map[string]bool) any {
	if fields == nil {
		return ev
	}
	return eventToMap(ev, fields)
}

// projectEvents applies projectEvent to a slice.
func projectEvents(evs []store.Event, fields map[string]bool) any {
	if fields == nil {
		return evs
	}
	out := make([]map[string]any, len(evs))
	for i, ev := range evs {
		out[i] = eventToMap(ev, fields)
	}
	return out
}

func eventToMap(ev store.Event, fields map[string]bool) map[string]any {
	m := make(map[string]any, len(fields))
	if fields["id"] {
		m["id"] = ev.ID
	}
	if fields["contract_id"] {
		m["contract_id"] = ev.ContractID
	}
	if fields["ledger"] {
		m["ledger"] = ev.Ledger
	}
	if fields["type"] {
		m["type"] = ev.Type
	}
	if fields["tx_hash"] {
		m["tx_hash"] = ev.TxHash
	}
	if fields["tx_index"] {
		m["tx_index"] = ev.TxIndex
	}
	if fields["op_index"] {
		m["op_index"] = ev.OpIndex
	}
	if fields["in_successful_call"] {
		m["in_successful_call"] = ev.InSuccessfulCall
	}
	if fields["topics"] {
		m["topics"] = ev.Topics
	}
	if fields["value"] {
		m["value"] = ev.Value
	}
	if fields["created_at"] {
		m["created_at"] = ev.CreatedAt
	}
	return m
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

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, versionResponse{
		Version:   buildinfo.Version,
		Commit:    buildinfo.Commit,
		BuildDate: buildinfo.BuildDate,
	})
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.serveEvents(w, r, filter, fields)
}

func (s *Server) handleContractEvents(w http.ResponseWriter, r *http.Request) {
	filter, fields, err := parseFilterAndFields(r)
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
	s.serveEvents(w, r, filter, fields)
}

// serveEvents is the shared body for /events and /contracts/{id}/events.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter, fields map[string]bool) {
	policy, etag, err := s.listCachePolicy(r.Context(), filter)
	if err != nil {
		loggerFromContext(r.Context()).Warn("deciding list cache policy", "error", err)
	} else if etag != "" && ifNoneMatch(r, etag) {
		writeNotModified(w, etag, policy)
		return
	}

	events, cursor, qerr := s.store.QueryEvents(r.Context(), filter)
	if qerr != nil {
		loggerFromContext(r.Context()).Error("querying events", "error", qerr)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}
	includeXDR := r.URL.Query().Get("include_xdr") == "true"
	decoded := r.URL.Query().Get("decoded") == "true"
	writeCacheHeaders(w, policy, immutableMaxAge, etag)
	if decoded && s.enricher != nil {
		enriched := s.enricher.EnrichEvents(r.Context(), events)
		if includeXDR {
			writeJSON(w, http.StatusOK, enrichedEventsWithXDRResponse{
				Events: enrichEventsWithXDR(enriched),
				Cursor: cursor,
			})
			return
		}
		writeJSON(w, http.StatusOK, enrichedEventsResponse{Events: enriched, Cursor: cursor})
		return
	}
	if includeXDR {
		writeJSON(w, http.StatusOK, eventsWithXDRResponse{
			Events: eventsWithXDR(events),
			Cursor: cursor,
		})
		return
	}
	if fields == nil {
		writeJSON(w, http.StatusOK, eventsResponse{Events: events, Cursor: cursor})
	} else {
		m := map[string]any{"events": projectEvents(events, fields)}
		if cursor != "" {
			m["cursor"] = cursor
		}
		writeJSON(w, http.StatusOK, m)
	}
}

// parseFilterAndFields parses the shared filter params plus the optional ?fields= allowlist.
func parseFilterAndFields(r *http.Request) (store.EventFilter, map[string]bool, error) {
	filter, err := filterFromQuery(r)
	if err != nil {
		return filter, nil, err
	}
	fields, err := parseFields(r.URL.Query().Get("fields"))
	if err != nil {
		return filter, nil, err
	}
	return filter, fields, nil
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	fields, err := parseFields(r.URL.Query().Get("fields"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	etag := `"` + id + `"`

	if ifNoneMatch(r, etag) {
		exists, err := s.store.EventExists(r.Context(), id)
		if err != nil {
			loggerFromContext(r.Context()).Error("checking event existence", "id", id, "error", err)
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
		loggerFromContext(r.Context()).Error("loading event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}
	decoded := r.URL.Query().Get("decoded") == "true"
	includeXDR := r.URL.Query().Get("include_xdr") == "true"
	if decoded && s.enricher != nil {
		enriched := s.enricher.EnrichEvents(r.Context(), []store.Event{event})
		if len(enriched) > 0 {
			if includeXDR {
				writeJSON(w, http.StatusOK, enrichEventWithXDR(enriched[0]))
				return
			}
			writeJSON(w, http.StatusOK, enriched[0])
			return
		}
	}
	writeCacheHeaders(w, cacheImmutable, immutableMaxAge, etag)
	if includeXDR {
		writeJSON(w, http.StatusOK, eventToXDRResponse(event))
		return
	}
	writeJSON(w, http.StatusOK, projectEvent(event, fields))
}

func eventToXDRResponse(e store.Event) eventWithXDR {
	var value *string
	if e.RawValueXDR != "" {
		value = &e.RawValueXDR
	}
	return eventWithXDR{
		Event:     e,
		TopicsXDR: e.RawTopicXDR,
		ValueXDR:  value,
	}
}

func eventsWithXDR(events []store.Event) []eventWithXDR {
	out := make([]eventWithXDR, len(events))
	for i, event := range events {
		out[i] = eventToXDRResponse(event)
	}
	return out
}

func enrichEventWithXDR(e store.EnrichedEvent) enrichedEventWithXDR {
	return enrichedEventWithXDR{
		eventWithXDR: eventToXDRResponse(e.Event),
		DecodedEvent: e.DecodedEvent,
		Decoded:      e.Decoded,
	}
}

func enrichEventsWithXDR(events []store.EnrichedEvent) []enrichedEventWithXDR {
	out := make([]enrichedEventWithXDR, len(events))
	for i, event := range events {
		out[i] = enrichEventWithXDR(event)
	}
	return out
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	network, err := s.resolveNetwork(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	stats, err := s.store.Stats(r.Context(), network)
	if err != nil {
		loggerFromContext(r.Context()).Error("loading stats", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading stats failed"))
		return
	}
	s.addStatsFreshness(r.Context(), &stats)
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

// Watched contracts types.

type addWatchedRequest struct {
	ContractID string `json:"contract_id"`
}

type addWatchedResponse struct {
	ContractID        string `json:"contract_id"`
	AddedAt           string `json:"added_at"`
	HistoryFromLedger int64  `json:"history_from_ledger"`
	ModeTransition    string `json:"mode_transition,omitempty"`
}

type removeWatchedResponse struct {
	ContractID       string `json:"contract_id"`
	RemovedAt        string `json:"removed_at"`
	HistoryPreserved bool   `json:"history_preserved"`
	ModeTransition   string `json:"mode_transition,omitempty"`
}

type watchedListResponse struct {
	Contracts []store.WatchedContract `json:"contracts"`
	Count     int                     `json:"count"`
}

func (s *Server) handleListWatchedChains(w http.ResponseWriter, r *http.Request) {
	contracts, err := s.store.ListWatchedContracts(r.Context())
	if err != nil {
		s.log.Error("listing watched contracts", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading watched contracts failed"))
		return
	}
	writeJSON(w, http.StatusOK, watchedListResponse{Contracts: contracts, Count: len(contracts)})
}

func (s *Server) handleAddWatchedChain(w http.ResponseWriter, r *http.Request) {
	var req addWatchedRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !config.ValidContractID(req.ContractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract_id %q (want 56-char C... strkey)", req.ContractID))
		return
	}

	current, err := s.store.ListWatchedContracts(r.Context())
	if err != nil {
		s.log.Error("listing watched contracts for add", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading watched contracts failed"))
		return
	}

	modeTransition := ""
	if len(current) == 0 {
		if r.URL.Query().Get("confirm") != "true" {
			writeError(w, http.StatusBadRequest, errors.New(
				"adding the first watched contract would switch ingestion from "+
					"'all contract events' to a specific list — pass ?confirm=true to acknowledge"))
			return
		}
		modeTransition = "all_to_specific"
	}

	state, err := s.store.GetIngestionState(r.Context(), s.defaultNetwork)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.log.Error("loading ingestion state for add", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading ingestion state failed"))
		return
	}

	if err := s.store.AddWatchedContract(r.Context(), req.ContractID); err != nil {
		s.log.Error("adding watched contract", "contract_id", req.ContractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("adding watched contract failed"))
		return
	}

	writeJSON(w, http.StatusOK, addWatchedResponse{
		ContractID:        req.ContractID,
		AddedAt:           time.Now().UTC().Format(time.RFC3339),
		HistoryFromLedger: state.LastIngestedLedger,
		ModeTransition:    modeTransition,
	})
}

func (s *Server) handleRemoveWatchedChain(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !config.ValidContractID(id) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", id))
		return
	}

	current, err := s.store.ListWatchedContracts(r.Context())
	if err != nil {
		s.log.Error("listing watched contracts for remove", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading watched contracts failed"))
		return
	}

	modeTransition := ""
	if len(current) == 1 && current[0].ContractID == id {
		if r.URL.Query().Get("confirm") != "true" {
			writeError(w, http.StatusBadRequest, errors.New(
				"removing the last watched contract would switch ingestion from "+
					"'a specific list' back to 'all contract events' — pass ?confirm=true to acknowledge"))
			return
		}
		modeTransition = "specific_to_all"
	}

	if err := s.store.RemoveWatchedContract(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("contract %q is not in the watch list", id))
			return
		}
		s.log.Error("removing watched contract", "contract_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("removing watched contract failed"))
		return
	}

	writeJSON(w, http.StatusOK, removeWatchedResponse{
		ContractID:       id,
		RemovedAt:        time.Now().UTC().Format(time.RFC3339),
		HistoryPreserved: true,
		ModeTransition:   modeTransition,
	})
}

func (s *Server) addStatsFreshness(ctx context.Context, stats *store.Stats) {
	if s.rpc == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	health, err := s.rpc.GetHealth(ctx)
	if err != nil {
		loggerFromContext(ctx).Warn("loading RPC health for stats", "error", err)
		return
	}
	head := int64(health.LatestLedger)
	lag := ingestLagLedgers(head, stats.LastIngestedLedger)
	stats.ChainHeadLedger = &head
	stats.IngestLagLedgers = &lag
}

func ingestLagLedgers(chainHead, lastIngested int64) int64 {
	if lastIngested <= 0 {
		return 0
	}
	return chainHead - lastIngested
}

func (s *Server) listCachePolicy(ctx context.Context, filter store.EventFilter) (cacheability, string, error) {
	if filter.ToLedger <= 0 {
		return cacheNoCache, "", nil
	}
	frontier, err := s.lastIngestedLedger(ctx, filter.Network)
	if err != nil {
		return cacheNoCache, "", err
	}
	if filter.ToLedger >= frontier {
		return cacheNoCache, "", nil
	}
	return cacheImmutable, listETag(filter), nil
}

// lastIngestedLedger reads the frontier from the persisted ingestion state.
func (s *Server) lastIngestedLedger(ctx context.Context, network string) (int64, error) {
	state, err := s.store.GetIngestionState(ctx, network)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return state.LastIngestedLedger, nil
}

// resolveNetwork returns the network to use for the current request.
func (s *Server) resolveNetwork(r *http.Request) (string, error) {
	q := r.URL.Query().Get("network")
	if q == "" {
		if s.defaultNetwork != "" {
			return s.defaultNetwork, nil
		}
		if len(s.networkNames) == 0 {
			return "", nil
		}
		return "", fmt.Errorf("network query parameter is required when multiple networks are configured; available: %s", strings.Join(s.networkNames, ", "))
	}
	if len(s.networkNames) == 0 {
		return "", fmt.Errorf("unknown network %q; no networks configured", q)
	}
	for _, n := range s.networkNames {
		if n == q {
			return q, nil
		}
	}
	return "", fmt.Errorf("unknown network %q; available: %s", q, strings.Join(s.networkNames, ", "))
}

func listETag(f store.EventFilter) string {
	key := struct {
		ContractID    string          `json:"c"`
		Type          string          `json:"t"`
		Topic         json.RawMessage `json:"p,omitempty"`
		Topic0        json.RawMessage `json:"p0,omitempty"`
		Topic1        json.RawMessage `json:"p1,omitempty"`
		Topic2        json.RawMessage `json:"p2,omitempty"`
		Topic3        json.RawMessage `json:"p3,omitempty"`
		TopicContains json.RawMessage `json:"pc,omitempty"`
		FromLedger    int64           `json:"fl"`
		ToLedger      int64           `json:"tl"`
		FromTime      string          `json:"ft,omitempty"`
		ToTime        string          `json:"tt,omitempty"`
		Cursor        string          `json:"cu,omitempty"`
		Limit         int             `json:"l"`
		Order         string          `json:"o,omitempty"`
		Network       string          `json:"n,omitempty"`
	}{
		Network:       f.Network,
		ContractID:    f.ContractID,
		Type:          f.Type,
		Topic:         f.Topic,
		Topic0:        f.Topic0,
		Topic1:        f.Topic1,
		Topic2:        f.Topic2,
		Topic3:        f.Topic3,
		TopicContains: f.TopicContains,
		FromLedger:    f.FromLedger,
		ToLedger:      f.ToLedger,
		FromTime:      timeOrEmpty(f.FromTime),
		ToTime:        timeOrEmpty(f.ToTime),
		Cursor:        f.Cursor,
		Limit:         resolvedLimit(f.Limit),
		Order:         resolvedOrder(f.Order),
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

// filterFromQuery parses the shared event-filter query params:
// network, contract_id, type, topic, from_ledger, to_ledger, from_time, to_time, cursor, limit.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()

	f := store.EventFilter{
		ContractID: q.Get("contract_id"),
		Cursor:     q.Get("cursor"),
		Network:    q.Get("network"),
	}

	if f.ContractID != "" && !config.ValidContractID(f.ContractID) {
		return f, fmt.Errorf("invalid contract_id %q", f.ContractID)
	}

	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		return f, fmt.Errorf("invalid cursor %q", f.Cursor)
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

	// topic_contains accepts any valid JSON value and uses @> containment directly.
	if raw := q.Get("topic_contains"); raw != "" {
		if !json.Valid([]byte(raw)) {
			return f, fmt.Errorf("topic_contains must be valid JSON")
		}
		f.TopicContains = json.RawMessage(raw)
	}

	order := q.Get("order")
	switch order {
	case "", "asc", "desc":
		f.Order = order
	default:
		return f, fmt.Errorf("invalid order %q (want asc or desc)", order)
	}

	var err2 error
	if topic := q.Get("topic"); topic != "" {
		parsed, parseErr := parseTopic("topic", topic)
		if parseErr != nil {
			return f, parseErr
		}
		f.Topic = parsed
	}

	if f.Topic0, err2 = parseTopic("topic0", q.Get("topic0")); err2 != nil {
		return f, err2
	}
	if f.Topic1, err2 = parseTopic("topic1", q.Get("topic1")); err2 != nil {
		return f, err2
	}
	if f.Topic2, err2 = parseTopic("topic2", q.Get("topic2")); err2 != nil {
		return f, err2
	}
	if f.Topic3, err2 = parseTopic("topic3", q.Get("topic3")); err2 != nil {
		return f, err2
	}

	if len(f.Topic) > 0 && (len(f.Topic0) > 0 || len(f.Topic1) > 0 || len(f.Topic2) > 0 || len(f.Topic3) > 0) {
		return f, fmt.Errorf("topic and topic0..topic3 filters cannot be combined")
	}

	var ledgerErr error
	if f.FromLedger, ledgerErr = parseLedgerParam(q.Get("from_ledger"), "from_ledger"); ledgerErr != nil {
		return f, ledgerErr
	}
	if f.ToLedger, ledgerErr = parseLedgerParam(q.Get("to_ledger"), "to_ledger"); ledgerErr != nil {
		return f, ledgerErr
	}
	if f.FromLedger > 0 && f.ToLedger > 0 && f.FromLedger > f.ToLedger {
		return f, fmt.Errorf("from_ledger %d is after to_ledger %d", f.FromLedger, f.ToLedger)
	}

	var timeErr error
	if f.FromTime, timeErr = parseTimeParam(q.Get("from_time"), "from_time"); timeErr != nil {
		return f, timeErr
	}
	if f.ToTime, timeErr = parseTimeParam(q.Get("to_time"), "to_time"); timeErr != nil {
		return f, timeErr
	}
	if !f.FromTime.IsZero() && !f.ToTime.IsZero() && f.FromTime.After(f.ToTime) {
		return f, fmt.Errorf("from_time %s is after to_time %s",
			f.FromTime.Format(time.RFC3339), f.ToTime.Format(time.RFC3339))
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > store.MaxQueryLimit {
			return f, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit)
		}
		f.Limit = limit
	} else {
		f.Limit = store.DefaultQueryLimit
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

// Holders endpoint types.

type holderResponse struct {
	Address    string `json:"address"`
	Balance    string `json:"balance"`
	LastLedger int64  `json:"last_ledger"`
}

type holdersResponse struct {
	ContractID     string           `json:"contract_id"`
	EarliestLedger int64            `json:"earliest_ledger"`
	Holders        []holderResponse `json:"holders"`
	Cursor         string           `json:"cursor,omitempty"`
}

func (s *Server) handleContractHolders(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}

	network, err := s.resolveNetwork(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	minBalance := r.URL.Query().Get("min_balance")
	cursor := r.URL.Query().Get("cursor")
	limit := store.DefaultQueryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > store.MaxQueryLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit))
			return
		}
		limit = parsed
	}

	// Determine the earliest ledger for coverage indication.
	earliestLedger, err := s.store.GetEarliestLedger(r.Context(), network, contractID)
	if err != nil {
		// non-fatal; surface as 0 to indicate unknown coverage
		loggerFromContext(r.Context()).Warn("getting earliest ledger", "contract_id", contractID, "error", err)
	}

	balances, next, err := s.store.GetTokenBalances(r.Context(), contractID, network, minBalance, cursor, limit)
	if err != nil {
		loggerFromContext(r.Context()).Error("querying token holders", "contract_id", contractID, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("querying token holders failed"))
		return
	}

	holders := make([]holderResponse, len(balances))
	for i, tb := range balances {
		holders[i] = holderResponse{
			Address:    tb.Address,
			Balance:    tb.Balance,
			LastLedger: tb.LastLedger,
		}
	}

	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, holdersResponse{
		ContractID:     contractID,
		EarliestLedger: earliestLedger,
		Holders:        holders,
		Cursor:         next,
	})
}

func (s *Server) handleEventStreamWS(w http.ResponseWriter, r *http.Request) {
	if s.bcast == nil {
		http.Error(w, "streaming not configured", http.StatusNotImplemented)
		return
	}

	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		loggerFromContext(r.Context()).Error("websocket accept", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	log := loggerFromContext(r.Context())

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
				log.Error("marshal event for ws", "error", err)
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
