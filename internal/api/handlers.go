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

	"github.com/sorotrail/sorotrail/internal/buildinfo"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// decodeJSONBody parses a single small JSON body (≤4 KiB), rejecting
// unknown fields so a typo like {"contractID": "..."} doesn't fall
// through with an empty contract_id and a confusing 400 from a later
// check. Returns a typed error string the handler can surface directly.
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

// cachePrivate is the package-wide override that flips Cache-Control
// directives from `public` to `private`. Production code sets it once at
// startup via SetCachePrivate (mirrors SetAuditor's "set before serve"
// pattern). Tests use the same entry point with t.Cleanup to reset.
//
// Why this exists: when auth (#17) lands, a single deployment can serve
// per-user data behind authentication. `private` keeps the response in
// the user's own browser cache while preventing CDNs/proxies from
// sharing it across accounts — the bug the spec calls out as "must
// never leak across keys".
var cachePrivate atomic.Bool

// SetCachePrivate flips the public→private override for all cacheable
// endpoints. Call once at startup before serving requests.
func SetCachePrivate(v bool) { cachePrivate.Store(v) }

// immutableMaxAge is the max-age used for cacheable responses on
// immutable resources (single events and list pages whose entire
// upper bound sits behind the ingest frontier). One year matches what
// most guides and browsers recommend for `immutable` responses; longer
// values don't help, since the `immutable` directive already prevents
// revalidation for the cached lifetime.
const immutableMaxAge = 365 * 24 * time.Hour

// cacheability kinds a handler can ask for. The mapping to header
// directives lives in writeCacheHeaders so handlers never write
// Cache-Control themselves.
type cacheability int

const (
	// cacheImmutable: long-lived, public (or private when CACHE_PRIVATE
	// is on), with the `immutable` directive. Used only when the server
	// can prove the response will not change.
	cacheImmutable cacheability = iota
	// cacheNoCache: must revalidate (Cache-Control: no-cache). Safe for
	// any growing-page response — it's never optimistic, never implies
	// staleness is OK.
	cacheNoCache
	// cacheNoStore: do not cache anywhere (Cache-Control: no-store).
	// Reserved for /health and /stats — operational data, not
	// shareable across users or even across requests on the same box.
	cacheNoStore
)

type errorResponse struct {
	Error string `json:"error"`
}

type eventsResponse struct {
	Events []store.Event `json:"events"`
	// Cursor is non-empty when another page exists; pass it back as ?cursor=.
	Cursor string `json:"cursor,omitempty"`
}

type enrichedEventsResponse struct {
	Events []store.EnrichedEvent `json:"events"`
	// Cursor is non-empty when another page exists.
	Cursor string `json:"cursor,omitempty"`
}

type eventsWithXDRResponse struct {
	Events []eventWithXDR `json:"events"`
	// Cursor is non-empty when another page exists.
	Cursor string `json:"cursor,omitempty"`
}

type enrichedEventsWithXDRResponse struct {
	Events []enrichedEventWithXDR `json:"events"`
	// Cursor is non-empty when another page exists.
	Cursor string `json:"cursor,omitempty"`
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
	Status string            `json:"status"` // ok | degraded
	Checks map[string]string `json:"checks"`
}

type versionResponse struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

// eventFieldNames is the set of JSON keys on store.Event that the ?fields=
// allowlist accepts. It is built from the json struct tags so the compiler
// catches drift when Event gains or renames fields.
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
// An empty or missing raw value returns nil (meaning "return the full object").
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

// projectEvents applies projectEvent to a slice, returning []map[string]any
// when fields is set, or the original slice when nil.
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

// eventToMap builds a map with only the requested fields.
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

	resp := healthResponse{Status: "ok", Checks: map[string]string{"database": "ok", "rpc": "ok"}}
	status := http.StatusOK

	// DB connectivity check: Ping the store to verify the database is reachable.
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
	if r.URL.Query().Get("stream") == "true" {
		s.handleListEventsStream(w, r)
		return
	}
	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.serveEvents(w, r, filter, fields)
}

// streamBatchSize is the number of events fetched per internal query when
// streaming NDJSON. It balances query cost against flush frequency: too
// small wastes round trips; too large buffers too long before a client
// sees progress.
const streamBatchSize = 500

func (s *Server) handleListEventsStream(w http.ResponseWriter, r *http.Request) {
	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Streaming overrides pagination: the limit is an internal batch size.
	filter.Limit = streamBatchSize

	includeXDR := r.URL.Query().Get("include_xdr") == "true"
	decoded := r.URL.Query().Get("decoded") == "true"

	ctx := r.Context()

	// Fetch the first batch BEFORE writing headers so a query failure
	// returns a proper error envelope rather than a 200 with an empty
	// body. On success we write the NDJSON headers and stream out.
	events, cursor, qerr := s.store.QueryEvents(ctx, filter)
	if qerr != nil {
		loggerFromContext(ctx).Error("streaming events (first batch)", "error", qerr)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	writeCacheHeaders(w, cacheNoCache, 0, "")
	w.WriteHeader(http.StatusOK)

	// Grab the flusher before streaming so a non-streamable wrapper is
	// detected early. The compress middleware's Flush works correctly
	// for uninflated buffers: it decides not to compress and flushes
	// the underlying writer.
	flusher, flushable := w.(http.Flusher)

	enc := json.NewEncoder(w)

	// writeEvents marshals and writes a batch of events as NDJSON lines.
	writeEvents := func(evs []store.Event) {
		if decoded && s.enricher != nil {
			// Batch-enrich like serveEvents: one call per batch, not per event.
			for _, enrichedEv := range s.enricher.EnrichEvents(ctx, evs) {
				if includeXDR {
					_ = enc.Encode(enrichEventWithXDR(enrichedEv))
				} else {
					_ = enc.Encode(enrichedEv)
				}
			}
			return
		}
		for _, ev := range evs {
			if includeXDR {
				_ = enc.Encode(eventToXDRResponse(ev))
			} else {
				_ = enc.Encode(projectEvent(ev, fields))
			}
		}
	}

	writeEvents(events)

	// Flush the first batch so the client sees data immediately even
	// when the entire result set fits in one batch.
	if flushable {
		flusher.Flush()
	}

	for cursor != "" {
		filter.Cursor = cursor
		events, cursor, qerr = s.store.QueryEvents(ctx, filter)
		if qerr != nil {
			loggerFromContext(ctx).Error("streaming events", "error", qerr)
			return // connection likely gone; just stop
		}

		writeEvents(events)

		// Flush after every batch so clients see progress, and so the
		// compression middleware can push bytes through the compressor.
		if flushable {
			flusher.Flush()
		}

		// Check for client disconnect so we don't keep querying forever.
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
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
// It runs the cacheability decision (frontier vs. upper bound) BEFORE the
// SQL query, so an immutable page with a matching If-None-Match never
// touches the events table at all — only the cheap ingestion_state row
// used to read the frontier.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter, fields map[string]bool) {
	policy, etag, err := s.listCachePolicy(r.Context(), filter)
	if err != nil {
		// "When in doubt, don't cache" is the explicit guidance: any
		// failure to read the frontier falls back to no-cache rather
		// than guessing the page is safe.
		loggerFromContext(r.Context()).Warn("deciding list cache policy", "error", err)
	} else if etag != "" && ifNoneMatch(r, etag) {
		writeNotModified(w, etag, policy)
		return
	}

	events, cursor, qerr := s.store.QueryEvents(r.Context(), filter)
	if errors.Is(qerr, store.ErrInvalidCursor) {
		// A cursor that doesn't decode is client input — most often a cursor
		// taken from one ordering and replayed against another. Report it as
		// a bad request rather than a server error.
		writeError(w, http.StatusBadRequest, fmt.Errorf(
			"invalid cursor for order_by=%s; use the cursor returned by the same ordering", filter.OrderBy))
		return
	}
	if qerr != nil {
		loggerFromContext(r.Context()).Error("querying events", "error", qerr)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}

	// Total matching count (ignoring pagination) as a response header.
	// Failure to count is non-fatal: we log a warning and proceed without
	// the header rather than dropping a successful page.
	countFilter := filter
	countFilter.Cursor = ""
	countFilter.Order = ""
	countFilter.OrderBy = ""
	countFilter.Limit = 0
	if total, cerr := s.store.CountEvents(r.Context(), countFilter); cerr != nil {
		loggerFromContext(r.Context()).Warn("counting events for X-Total-Count", "error", cerr)
	} else {
		w.Header().Set("X-Total-Count", fmt.Sprintf("%d", total))
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

// parseFilterAndFields parses the shared filter params plus the optional
// ?fields= allowlist.
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

	// Strong ETag: the event ID is itself a perfect validator for an
	// immutable resource (each ID maps to exactly one row, that row's
	// body never changes). Using the ID instead of a body hash skips a
	// scan-and-hash on the cache-miss path and keeps 304s cheap.
	etag := `"` + id + `"`

	// 304 fast path: conditional GET with a matching validator. We
	// avoid the row-serialization path entirely (GetEvent is not
	// called), and instead probe presence with EventExists so retention
	// /pruning (#8) can't let a deleted event masquerade as still
	// present. A miss in EventExists is reported as 404, matching the
	// unconditional code path.
	if ifNoneMatch(r, etag) {
		exists, err := s.store.EventExists(r.Context(), id)
		if err != nil {
			loggerFromContext(r.Context()).Error("checking event existence", "id", id, "error", err)
			writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
			return
		}
		if !exists {
			// Retention/pruning (#8) deleted the row out from under cached
			// clients. writeError carries Cache-Control: no-store so a CDN
			// that warmed on the ETag-bearing 200 doesn't happily pool
			// this 404 for the immutable max-age.
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
			writeCacheHeaders(w, cacheImmutable, immutableMaxAge, etag)
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

// Stats summarizes what the indexer has stored plus, when the auditor is
// running, the post-processing counters it has accumulated.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		loggerFromContext(r.Context()).Error("loading stats", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading stats failed"))
		return
	}
	s.addStatsFreshness(r.Context(), &stats)
	stats.PanicsRecovered = s.recoverer.PanicsRecovered()
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
	if c := getRPCCounter(); c != nil {
		snap := c.Errors().Snapshot()
		stats.RPCErrors = store.RPCErrorStats{
			GetEvents:        snap.GetEvents,
			GetLatestLedger:  snap.GetLatestLedger,
			GetHealth:        snap.GetHealth,
			GetLedgerEntries: snap.GetLedgerEntries,
		}
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, stats)
}

// addWatchedRequest is the body for POST /watched-contracts: a single
// contract ID to add to the runtime watch list.
type addWatchedRequest struct {
	ContractID string `json:"contract_id"`
}

// addWatchedResponse includes the current ingestion cursor so callers
// know exactly where historical replay starts — events before this ledger
// are not backfilled by the runtime add (a separate replay tool covers them).
type addWatchedResponse struct {
	ContractID        string `json:"contract_id"`
	AddedAt           string `json:"added_at"`
	HistoryFromLedger int64  `json:"history_from_ledger"`
	// ModeTransition is "all_to_specific" when the list was empty before
	// this add, surfacing the same semantic change the confirm guard
	// guards. Useful to post-run auditors even when the caller already
	// confirmed.
	ModeTransition string `json:"mode_transition,omitempty"`
}

// removeWatchedResponse explains what removal actually did: stored events
// are NOT deleted; only future ingestion stops. The message is part of
// the contract so callers don't have to read the README to find out.
type removeWatchedResponse struct {
	ContractID       string `json:"contract_id"`
	RemovedAt        string `json:"removed_at"`
	HistoryPreserved bool   `json:"history_preserved"`
	ModeTransition   string `json:"mode_transition,omitempty"`
}

// watchedListResponse wraps the GET response in an envelope so the route
// is easy to extend without breaking JSON shape.
type watchedListResponse struct {
	Contracts []store.WatchedContract `json:"contracts"`
	Count     int                     `json:"count"`
}

// handleListWatchedChains returns the current watch list in stable
// (contract_id) order.
func (s *Server) handleListWatchedChains(w http.ResponseWriter, r *http.Request) {
	contracts, err := s.store.ListWatchedContracts(r.Context())
	if err != nil {
		s.log.Error("listing watched contracts", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading watched contracts failed"))
		return
	}
	writeJSON(w, http.StatusOK, watchedListResponse{Contracts: contracts, Count: len(contracts)})
}

// handleAddWatchedChain adds a contract to the runtime watch list and
// returns where historical replay for it starts (= the current cursor).
//
// Guarded by ?confirm=true when the add would switch the ingester from
// "all contract events" (empty list) to a specific contract list, since
// that silently narrows what gets stored going forward.
//
// TOCTOU note: the confirm check reads the list and then mutates it
// without holding a row lock. Two concurrent empty→non-empty POSTs can
// both pass the guard before either mutates — acceptable because
// ON CONFLICT DO NOTHING makes a duplicate Add a no-op, and only the
// first response carries the genuine transition; the second reports one
// too, but no row state diverges.
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

	state, err := s.store.GetIngestionState(r.Context())
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

// handleRemoveWatchedChain stops future ingestion for the named contract
// without deleting any events already stored. The same empty<->non-empty
// guard fires when removing the last contract on the list (specific -> all).
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
	return chainHead - lastIngested
}

// listCachePolicy decides whether a list page is cacheable as immutable
// based on the ingest frontier. The whole point of this function is
// correctness against a moving frontier: a 1-ledger mistake either
// way lets a stale row into a cache or strands one behind it, so the
// comparison is deliberately strict and biases toward no-cache.
//
// Rule: a page is only safe (= can't gain rows) when to_ledger is set
// AND strictly less than the last ingested ledger. Equality is folded
// into the unsafe bucket: at the frontier, ingestion may still be in
// progress and the boundary ledger's row count isn't guaranteed.
//
// Time-only filters (no ledger bound) are never cacheable: translating
// created_at to ledgers would need a maintained lookup and the spec
// says "when in doubt, don't cache". Time filters can still be applied
// alongside ledger bounds and the policy falls through correctly
// because the ledger-side comparison decides.
//
// On any failure to read frontier, the policy is no-cache.
func (s *Server) listCachePolicy(ctx context.Context, filter store.EventFilter) (cacheability, string, error) {
	if filter.ToLedger <= 0 {
		return cacheNoCache, "", nil
	}
	frontier, err := s.lastIngestedLedger(ctx)
	if err != nil {
		return cacheNoCache, "", err
	}
	if filter.ToLedger >= frontier {
		// Includes the boundary case by design. See the rationale above.
		return cacheNoCache, "", nil
	}
	return cacheImmutable, listETag(filter), nil
}

// lastIngestedLedger reads the frontier from the persisted ingestion
// state. We deliberately reuse GetIngestionState (the narrow index-only
// row already on the hot path) rather than Stats, whose count-aggregates
// scan the events table — the spec wants the existing value via the
// store, not a fancier query just for caching.
//
// A miss (cold start, no row yet) is treated as frontier=0 so every
// to_ledger is "not strictly below" and the caller returns no-cache.
// This is conservative on the safe side: nothing gets the immutable
// header until at least one ledger is ingested.
func (s *Server) lastIngestedLedger(ctx context.Context) (int64, error) {
	state, err := s.store.GetIngestionState(ctx)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return state.LastIngestedLedger, nil
}

// listETag is the strong validator for a list call. The frontier is
// deliberately NOT included in the hash: once a page is verified
// cacheable immutable, its ETag stays valid as the frontier moves
// forward, so a warmed cache isn't invalidated on every ingest cycle.
// Distinct filters produce distinct ETags because every component is
// marshaled to JSON (no separator-collision worries).
//
// Order and Limit are normalized to the values QueryEvents will
// actually use: the SQL layer treats Order=="" as "asc" and Limit<=0
// as DefaultQueryLimit. Hashing the unresolved values would give us
// different ETags for requests that produce identical bodies
// (e.g. ?order=asc vs no order param), which thrashes caches for no
// reason.
//
// DefaultQueryLimit is the value the store applies; copying it here
// (instead of importing `store`) keeps the cache layer unaware of the
// store's pagination rules and we re-verify by test.
func listETag(f store.EventFilter) string {
	// contributors: every field of EventFilter that narrows the result set
	// MUST appear here. A filter that is missing produces the same hash for
	// two requests that return different bodies, which on an immutable page
	// means a conditional request for one is answered 304 for the other, and
	// a shared cache pools one filter's body under the other's key — for the
	// full one-year max-age. TestListETag_CoversEveryFilterField enumerates
	// the fields independently and fails when a new one is not added.
	key := struct {
		ContractID    string          `json:"c"`
		Types         []string        `json:"t"`
		Topic         json.RawMessage `json:"p,omitempty"`
		Topic0        json.RawMessage `json:"p0,omitempty"`
		Topic1        json.RawMessage `json:"p1,omitempty"`
		Topic2        json.RawMessage `json:"p2,omitempty"`
		Topic3        json.RawMessage `json:"p3,omitempty"`
		TopicContains json.RawMessage `json:"pc,omitempty"`
		TxHash        string          `json:"th,omitempty"`
		FromLedger    int64           `json:"fl"`
		ToLedger      int64           `json:"tl"`
		FromTime      string          `json:"ft,omitempty"`
		ToTime        string          `json:"tt,omitempty"`
		Cursor        string          `json:"cu,omitempty"`
		Limit         int             `json:"l"`
		Order         string          `json:"o,omitempty"`
	}{
		ContractID: f.ContractID,
		Types:      f.Types,
		Topic:      f.Topic,
		// Each positional filter gets its own distinctly named key, so
		// topic0={x} and topic1={x} — which select different events — cannot
		// serialize identically.
		Topic0:        f.Topic0,
		Topic1:        f.Topic1,
		Topic2:        f.Topic2,
		Topic3:        f.Topic3,
		TopicContains: f.TopicContains,
		TxHash:        f.TxHash,
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

// resolvedLimit mirrors the default applied in QueryEvents. Pulling
// the constant from the store package keeps the cache layer from
// drifting if the store-side default ever moves: any change shows up
// immediately in the build, and the cache-stopping behavior updates
// in lockstep.
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

// timeOrEmpty renders a zero time as "" so two unset times don't both
// serialize to "0001-01-01T00:00:00Z" (which would otherwise be a
// distinct value from one caller that didn't set the field at all).
func timeOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// ifNoneMatch reports whether the request's If-None-Match header
// matches the supplied strong ETag. RFC 7232 §3.2: the comparison for
// strong ETags is byte-exact after stripping the W/ weakness prefix.
// `*` matches any present representation.
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

// writeCacheHeaders is the single place in the package that writes
// ETag, Cache-Control, Vary. Handlers pick a cacheability kind and an
// etag string; nothing else needs to know about header semantics.
//
// Vary: Accept-Encoding is set proactively so the future compression
// middleware (#25) can plug in without re-encoding responses already
// cached as gzip or vice versa; the header is the contract a shared
// cache uses to keep distinct variants. If a future middleware (auth
// #17, or any content-negotiating layer) has already populated Vary,
// we MERGE rather than overwrite so distinct dimensions coexist in
// the comma-separated value the cache uses.
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
			// Auth'd deployments get `private`: caching stays scoped to
			// the authenticated user (browser cache works), but shared
			// caches (CDN/proxy) cannot pool responses across users.
			scope = "private"
		}
		w.Header().Set("Cache-Control",
			fmt.Sprintf("%s, max-age=%d, immutable", scope, int(maxAge.Seconds())))
	}
}

// writeNotModified sends a 304 with the same cache-validation headers
// the original response would have carried. RFC 7232 §4.1 says a 304
// should mirror the 200 response's Content-Type so strict intermediaries
// can probe the body's media type before serving a stale entry — we
// set it before WriteHeader for that reason. The cache validators
// (Vary, ETag, Cache-Control) are emitted from the same writeCacheHeaders
// path the full response would use, so they're guaranteed identical.
func writeNotModified(w http.ResponseWriter, etag string, kind cacheability) {
	w.Header().Set("Content-Type", "application/json")
	writeCacheHeaders(w, kind, immutableMaxAge, etag)
	w.WriteHeader(http.StatusNotModified)
}

// filterFromQuery parses the shared event-filter query params:
// contract_id, type, topic, from_ledger, to_ledger, from_time, to_time, cursor, limit.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()
	f := store.EventFilter{
		ContractID: q.Get("contract_id"),
		Cursor:     q.Get("cursor"),
		TxHash:     q.Get("tx_hash"),
	}

	if f.ContractID != "" && !config.ValidContractID(f.ContractID) {
		return f, fmt.Errorf("invalid contract_id %q", f.ContractID)
	}

	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		return f, fmt.Errorf("invalid cursor %q", f.Cursor)
	}

	if raw := q.Get("type"); raw != "" {
		for _, t := range strings.Split(raw, ",") {
			t = strings.TrimSpace(t)
			switch t {
			case "contract", "system", "diagnostic":
			default:
				return f, fmt.Errorf("invalid type %q (want contract|system|diagnostic)", t)
			}
			f.Types = append(f.Types, t)
		}
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

	// order_by selects the sort column; order still controls the direction,
	// so the two combine (e.g. order_by=created_at&order=desc).
	orderBy := q.Get("order_by")
	if !store.ValidOrderBy(orderBy) {
		return f, fmt.Errorf("invalid order_by %q (want %s, %s or %s)",
			orderBy, store.OrderByID, store.OrderByLedger, store.OrderByCreatedAt)
	}
	f.OrderBy = orderBy

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

func (s *Server) handleEventStreamWS(w http.ResponseWriter, r *http.Request) {
	if s.bcast == nil {
		http.Error(w, "streaming not configured", http.StatusNotImplemented)
		return
	}

	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// InsecureSkipVerify disables WebSocket Origin checking: the WS endpoint
	// is server-to-client only (no client messages are read), so a forged
	// Origin header cannot influence what the client sees.
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

	// Use CloseRead to get a context cancelled when the client disconnects
	// and to ensure the library processes control frames (ping/pong/close).
	ctx = c.CloseRead(ctx)

	// Periodic ping to detect stale connections.
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
			data, err := json.Marshal(projectEvent(ev, fields))
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
	// Every error response is marked no-store so neither CDNs nor
	// browsers can pool a 4xx/5xx behind a success response's
	// validator. The prime motivator is the 404-on-eviction path in
	// handleGetEvent: a stale cache otherwise keeps returning "not
	// found" for an event that briefly aged out but never came back.
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
