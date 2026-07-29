package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/broadcast"
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

// tenantScoped mirrors Server.multiTenant for the package-level cache
// helpers, which are plain functions and have no server to ask.
// WithMultiTenancy sets it at startup, before any request is served.
//
// Caching is where multi-tenancy leaks if nobody is looking. Two tenants
// issuing byte-identical requests produce different bodies, so the response
// is not a function of the URL any more. Three things follow, and all three
// are needed — any one alone is insufficient:
//
//   - Cache-Control must be `private`, so a CDN or proxy never pools a
//     tenant-scoped body for the next caller.
//   - Vary must name the credential headers, so a cache that does store the
//     response keys it per credential.
//   - The ETag must incorporate the scope, so a conditional request
//     carrying another tenant's validator cannot be answered 304.
//
// The first two constrain intermediaries; the third constrains this server,
// which is the one that would otherwise hand out a 304 for a page the
// caller has never been entitled to see.
var tenantScoped atomic.Bool

// SetTenantScopedCaching marks responses as tenant-specific for caching
// purposes. Called by WithMultiTenancy; exported for tests.
func SetTenantScopedCaching(v bool) { tenantScoped.Store(v) }

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

// handleDeleteEvents is the admin-only bulk delete endpoint. It deletes all
// events whose ledger is strictly less than the ?before_ledger= query parameter.
// The endpoint is protected by apiKeyAuth middleware (same as watched-contracts).
//
// Query params:
//
//	before_ledger (required) — delete all events with ledger < this value
//
// Response:
//
//	200 { "deleted": <count> } on success
//	400 when before_ledger is missing or not a positive integer
func (s *Server) handleDeleteEvents(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("before_ledger")
	if raw == "" {
		writeError(w, http.StatusBadRequest, errors.New("before_ledger query parameter is required"))
		return
	}
	beforeLedger, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || beforeLedger <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("before_ledger must be a positive integer, got %q", raw))
		return
	}

	deleted, err := s.store.DeleteEventsBeforeLedger(r.Context(), beforeLedger)
	if err != nil {
		loggerFromContext(r.Context()).Error("bulk delete events", "before_ledger", beforeLedger, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting events failed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
}

// handleLivez is the liveness probe. It returns 200 as long as the process
// is running. No dependencies are checked — a transient DB or RPC outage
// must not cause orchestration to kill and restart the pod (readiness is
// a separate signal).
func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok", Checks: map[string]string{"process": "ok"}})
}

// handleReadyz is the readiness probe. It performs a bounded DB ping,
// RPC head check, and verifies the database schema is at the expected
// migration version. Returns 200 when all dependencies are reachable,
// 503 with a JSON body explaining which dependency is down.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: map[string]string{
		"database":       "ok",
		"rpc":            "ok",
		"schema_version": "ok",
	}}
	status := http.StatusOK

	if err := s.store.Ping(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["database"] = err.Error()
		status = http.StatusServiceUnavailable
	}

	// Check that the schema_migrations table reports a clean state with the
	// expected version. A dirty migration or missing schema is a deployment
	// error that should prevent the pod from receiving traffic.
	if version, dirty, err := s.store.MigrationVersion(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["schema_version"] = fmt.Sprintf("check failed: %v", err)
		status = http.StatusServiceUnavailable
	} else if version == 0 {
		resp.Status = "degraded"
		resp.Checks["schema_version"] = "no migrations applied"
		status = http.StatusServiceUnavailable
	} else if dirty {
		resp.Status = "degraded"
		resp.Checks["schema_version"] = fmt.Sprintf("dirty (version %d)", version)
		status = http.StatusServiceUnavailable
	} else {
		resp.Checks["schema_version"] = fmt.Sprintf("ok (version %d)", version)
	}

	if health, err := s.rpc.GetHealth(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["rpc"] = err.Error()
		status = http.StatusServiceUnavailable
	} else if health.Status != "healthy" {
		resp.Status = "degraded"
		resp.Checks["rpc"] = fmt.Sprintf("rpc reports %q", health.Status)
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
		writeFilterError(w, err)
		return
	}
	s.serveEvents(w, r, filter, fields)
}

// countResponse is the JSON body for GET /events/count.
type countResponse struct {
	Count int64 `json:"count"`
}

// handleCountEvents returns the number of events matching the same filters
// as GET /events. Pagination params (cursor, limit, order, order_by) are
// accepted in the URL but ignored for the count — only the filter fields
// that narrow the result set are applied.
func (s *Server) handleCountEvents(w http.ResponseWriter, r *http.Request) {
	filter, _, err := parseFilterAndFields(r)
	if err != nil {
		// writeFilterError, not a flat 400: a request naming a contract the
		// tenant lacks is a 403 here for the same reason it is on /events.
		writeFilterError(w, err)
		return
	}
	// Strip pagination — count is over the full matching set.
	filter.Cursor = ""
	filter.Order = ""
	filter.OrderBy = ""
	filter.Limit = 0

	total, err := s.store.CountEvents(r.Context(), filter)
	if err != nil {
		loggerFromContext(r.Context()).Error("counting events", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("counting events failed"))
		return
	}
	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, countResponse{Count: total})
}

// streamBatchSize is the number of events fetched per internal query when
// streaming NDJSON. It balances query cost against flush frequency: too
// small wastes round trips; too large buffers too long before a client
// sees progress.
const streamBatchSize = 500

// recentDefaultLimit is the number of events returned when ?recent is
// specified without a numeric value (i.e. ?recent=true). Chosen to be
// useful at a glance without overwhelming a caller that just wants the
// latest activity.
const recentDefaultLimit = 20

func (s *Server) handleListEventsStream(w http.ResponseWriter, r *http.Request) {
	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		// Same mapping as the non-streaming path: naming an ungranted
		// contract is a 403, not a malformed request.
		writeFilterError(w, err)
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
		writeFilterError(w, err)
		return
	}
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}
	if !filter.Scope.Allows(contractID) {
		writeForbiddenContract(w, contractID)
		return
	}
	filter.ContractID = contractID
	s.serveEvents(w, r, filter, fields)
}

// errForbiddenContract is returned by filterFromQuery when the request names
// a contract outside the caller's grants. It is a distinct type rather than
// a sentinel string so writeFilterError can separate "you asked wrongly"
// (400) from "you asked for someone else's data" (403).
type errForbiddenContract struct{ contractID string }

func (e errForbiddenContract) Error() string {
	return fmt.Sprintf("contract %s is not granted to this tenant", e.contractID)
}

// writeFilterError maps a filter-construction failure to its status. Every
// caller of filterFromQuery routes errors through here, so the 403 case
// cannot be reported as a 400 by one endpoint and correctly by another.
func writeFilterError(w http.ResponseWriter, err error) {
	var forbidden errForbiddenContract
	if errors.As(err, &forbidden) {
		writeForbiddenContract(w, forbidden.contractID)
		return
	}
	writeError(w, http.StatusBadRequest, err)
}

// writeForbiddenContract reports a contract the caller named explicitly but
// may not read.
//
// 403 rather than 404 is deliberate here, and is the opposite of the choice
// made for event IDs. The caller already possesses the contract ID — they
// typed it into the path — and contract IDs are public on-chain identifiers,
// so confirming "this exists but is not yours" discloses nothing they could
// not learn from a block explorer. Answering 404 instead would leave
// operators debugging a missing grant as though it were missing data.
func writeForbiddenContract(w http.ResponseWriter, contractID string) {
	writeError(w, http.StatusForbidden,
		errForbiddenContract{contractID: contractID})
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
	s.recordEventsServed(r.Context(), len(events))

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
		// The enriched branch writes no cache headers, so the Vary the
		// scoped path relies on is set explicitly here too. Without it a
		// shared cache could key ?decoded=true responses on URL alone.
		writeVary(w)
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

// rawEventResponse is the JSON body for GET /events/{id}/raw.
type rawEventResponse struct {
	TopicsXDR []string `json:"topics_xdr"`
	ValueXDR  string   `json:"value_xdr,omitempty"`
}

// handleGetEventRaw returns the stored raw topic/value XDR for an event.
// Returns 404 if the event is not found or no raw XDR was stored (e.g. the
// RPC returned already-decoded JSON for this row).
func (s *Server) handleGetEventRaw(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	// Raw XDR is the same row as GET /events/{id}, just a different
	// projection of it, so it takes the same scope on both the fetch and
	// the 304 probe below. Without this the raw view would be a way to
	// read an event body the scoped endpoint refuses.
	scope := scopeFrom(r.Context())

	event, err := s.store.GetEvent(r.Context(), id, scope)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("loading event for raw XDR", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}

	// Return 404 when no raw XDR was stored for this event.
	if len(event.RawTopicXDR) == 0 && event.RawValueXDR == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q has no raw XDR stored", id))
		return
	}

	// Strong ETag: the event ID is a perfect validator for an immutable
	// resource. The same reuse logic as handleGetEvent.
	etag := `"` + id + `"`

	if ifNoneMatch(r, etag) {
		exists, err := s.store.EventExists(r.Context(), id, scope)
		if err != nil {
			loggerFromContext(r.Context()).Error("checking event existence for raw XDR", "id", id, "error", err)
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

	writeCacheHeaders(w, cacheImmutable, immutableMaxAge, etag)
	writeJSON(w, http.StatusOK, rawEventResponse{
		TopicsXDR: event.RawTopicXDR,
		ValueXDR:  event.RawValueXDR,
	})
}

// handleGetEventTransaction returns all sibling events from the same
// transaction as the event with the given {id}. The referenced event
// itself is excluded from the response. Returns 404 when the event is
// not found; returns an empty list when the transaction has no other
// events.
func (s *Server) handleGetEventTransaction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Validate ?fields= before touching the store.
	fields, err := parseFields(r.URL.Query().Get("fields"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	event, err := s.store.GetEvent(r.Context(), id, scopeFrom(r.Context()))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("loading event for transaction siblings", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}

	// If the event has no transaction hash (should not normally happen),
	// return an empty list rather than erroring — the event is valid but
	// has no siblings.
	if event.TxHash == "" {
		writeCacheHeaders(w, cacheImmutable, immutableMaxAge, `"`+id+`:tx"`)
		writeJSON(w, http.StatusOK, eventsResponse{Events: []store.Event{}})
		return
	}

	siblings, err := s.store.GetEventsByTxHash(r.Context(), event.TxHash, id)
	if err != nil {
		loggerFromContext(r.Context()).Error("loading transaction siblings", "tx_hash", event.TxHash, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading transaction events failed"))
		return
	}

	decoded := r.URL.Query().Get("decoded") == "true"
	includeXDR := r.URL.Query().Get("include_xdr") == "true"

	etag := `"` + id + `:tx"`
	writeCacheHeaders(w, cacheImmutable, immutableMaxAge, etag)

	if decoded && s.enricher != nil {
		enriched := s.enricher.EnrichEvents(r.Context(), siblings)
		if includeXDR {
			writeJSON(w, http.StatusOK, enrichedEventsWithXDRResponse{Events: enrichEventsWithXDR(enriched)})
			return
		}
		writeJSON(w, http.StatusOK, enrichedEventsResponse{Events: enriched})
		return
	}
	if includeXDR {
		writeJSON(w, http.StatusOK, eventsWithXDRResponse{Events: eventsWithXDR(siblings)})
		return
	}
	if fields == nil {
		writeJSON(w, http.StatusOK, eventsResponse{Events: siblings})
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"events": projectEvents(siblings, fields)})
	}
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	// Both the existence probe and the row fetch below run under this
	// scope, so an event belonging to an ungranted contract is reported as
	// absent on every path through this handler — including the 304 fast
	// path, which would otherwise be a free existence oracle.
	scope := scopeFrom(r.Context())

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
		exists, err := s.store.EventExists(r.Context(), id, scope)
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

	event, err := s.store.GetEvent(r.Context(), id, scope)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("loading event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}
	s.recordEventsServed(r.Context(), 1)
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

// contractListResponse is the JSON body for GET /contracts.
type contractListResponse struct {
	Contracts []store.ContractSummary `json:"contracts"`
	Count     int                     `json:"count"`
	Cursor    string                  `json:"cursor,omitempty"`
}

// handleListContracts returns one ContractSummary per indexed contract,
// paginated, default-sorted by event_count desc (the most active
// contracts first). The endpoint is intentionally READ-ONLY and
// unauthenticated: a contract listing has no surface area for
// cross-tenant data leakage (a contract_id is opaque), and gating it
// behind API_KEY would force every browser dashboard to log in.
//
// Cache-Control is no-cache: a brand-new contract can be ingested at
// any time, and a stale cache would hide it from a freshly-launched
// explorer.
func (s *Server) handleListContracts(w http.ResponseWriter, r *http.Request) {
	f := store.ContractsFilter{
		ContractIDPrefix: r.URL.Query().Get("contract_id"),
		SortKey:          r.URL.Query().Get("sort"),
		Order:            r.URL.Query().Get("order"),
		Cursor:           r.URL.Query().Get("cursor"),
	}
	if !store.ValidContractsSortKey(f.SortKey) {
		writeError(w, http.StatusBadRequest, fmt.Errorf(
			"invalid sort %q (want %s, %s, %s, or %s)",
			f.SortKey,
			store.SortByActivity, store.SortByFirstLedger,
			store.SortByLastLedger, store.SortByLastSeen))
		return
	}
	if f.Order != "" && f.Order != "asc" && f.Order != "desc" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid order %q (want asc or desc)", f.Order))
		return
	}
	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid cursor %q", f.Cursor))
		return
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > store.MaxQueryLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit))
			return
		}
		f.Limit = n
	} else {
		f.Limit = store.DefaultQueryLimit
	}
	items, cursor, err := s.store.ListContracts(r.Context(), f)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing contracts", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing contracts failed"))
		return
	}
	total, cerr := s.store.CountContracts(r.Context(), f)
	if cerr != nil {
		loggerFromContext(r.Context()).Warn("counting contracts for X-Total-Count", "error", cerr)
	} else if total > 0 {
		w.Header().Set("X-Total-Count", fmt.Sprintf("%d", total))
	}
	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, contractListResponse{
		Contracts: items,
		Count:     len(items),
		Cursor:    cursor,
	})
}

// deadLetterListResponse is the JSON body for GET /dead-letters.
type deadLetterListResponse struct {
	DeadLetters []store.DeadLetter `json:"dead_letters"`
	Count       int                `json:"count"`
	Cursor      string             `json:"cursor,omitempty"`
}

// handleListDeadLetters returns the poison-event queue newest-first.
// Like the watched-contracts surface, this is gated behind API_KEY —
// dead-letter rows contain raw RPC payloads, and disclosing them on a
// public endpoint would leak every event the indexer failed to decode.
func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	f := struct {
		ContractID string
		Limit      int
		Cursor     string
	}{
		ContractID: r.URL.Query().Get("contract_id"),
		Cursor:     r.URL.Query().Get("cursor"),
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > store.MaxQueryLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit))
			return
		}
		f.Limit = n
	}
	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid cursor %q", f.Cursor))
		return
	}
	items, cursor, err := s.store.ListDeadLetters(r.Context(), f.ContractID, f.Limit, f.Cursor)
	if err != nil {
		loggerFromContext(r.Context()).Error("listing dead letters", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing dead letters failed"))
		return
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, deadLetterListResponse{
		DeadLetters: items,
		Count:       len(items),
		Cursor:      cursor,
	})
}

// handleDeleteDeadLetter removes a single dead-letter row by id.
// Idempotent: calling again returns 404.
func (s *Server) handleDeleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("dead-letter id must be a positive integer, got %q", idStr))
		return
	}
	if err := s.store.DeleteDeadLetter(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("dead letter %d not found", id))
			return
		}
		loggerFromContext(r.Context()).Error("deleting dead letter", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("deleting dead letter failed"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context(), scopeFrom(r.Context()))
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
	if p := getPruner(); p != nil {
		m := p.Metrics()
		stats.Pruner = store.PrunerStats{
			RunsCompleted:   m.RunsCompleted,
			TotalRowsPurged: m.TotalRowsPurged,
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

// handleAddressEvents returns events involving the given address,
// chronologically ordered, cursor-paginated.
func (s *Server) handleAddressEvents(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if !isValidAddress(address) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid address %q (want G... or C... strkey)", address))
		return
	}

	filter, _, err := parseFilterAndFields(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Address events are always ordered by event_id.
	if filter.OrderBy != "" && filter.OrderBy != store.OrderByID {
		writeError(w, http.StatusBadRequest, fmt.Errorf("address events only support order_by=id (the default)"))
		return
	}

	events, cursor, qerr := s.store.QueryAddressEvents(r.Context(), address, filter)
	if qerr != nil {
		loggerFromContext(r.Context()).Error("querying address events", "address", address, "error", qerr)
		writeError(w, http.StatusInternalServerError, errors.New("querying address events failed"))
		return
	}

	// Total matching count header.
	if total, cerr := s.store.CountAddressEvents(r.Context(), address); cerr != nil {
		loggerFromContext(r.Context()).Warn("counting address events", "error", cerr)
	} else {
		w.Header().Set("X-Total-Count", fmt.Sprintf("%d", total))
	}

	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, addressEventsResponse{Events: events, Cursor: cursor})
}

// handleAddressSummary returns aggregate information about an address's
// event history.
func (s *Server) handleAddressSummary(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if !isValidAddress(address) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid address %q (want G... or C... strkey)", address))
		return
	}

	summary, err := s.store.GetAddressSummary(r.Context(), address)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("address %q not found", address))
		return
	}
	if err != nil {
		loggerFromContext(r.Context()).Error("loading address summary", "address", address, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading address summary failed"))
		return
	}

	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, summary)
}

// isValidAddress checks if the string looks like a Stellar strkey (G... or C..., 56 chars).
func isValidAddress(s string) bool {
	if len(s) != 56 {
		return false
	}
	prefix := s[0]
	if prefix != 'G' && prefix != 'C' {
		return false
	}
	for _, r := range s[1:] {
		if (r < 'A' || r > 'Z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}

// addressEventsResponse is the response shape for GET /addresses/{address}/events.
type addressEventsResponse struct {
	Events []store.Event `json:"events"`
	Cursor string        `json:"cursor,omitempty"`
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
		HasValue      *bool           `json:"hv,omitempty"`
		TxIndex       *int32          `json:"txi,omitempty"`
		OpIndex       *int32          `json:"opi,omitempty"`
		FromLedger    int64           `json:"fl"`
		ToLedger      int64           `json:"tl"`
		FromTime      string          `json:"ft,omitempty"`
		ToTime        string          `json:"tt,omitempty"`
		Cursor        string          `json:"cu,omitempty"`
		Limit         int             `json:"l"`
		Order         string          `json:"o,omitempty"`
		// Scope makes the validator tenant-specific. Two tenants issuing
		// the same request are asking for different representations of
		// this URL, and without this component the second one's
		// If-None-Match would match the first one's page and be answered
		// 304 — a cross-tenant disclosure produced entirely inside this
		// server, with no CDN involved.
		Scope string `json:"s"`
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
		HasValue:      f.HasValue,
		TxIndex:       f.TxIndex,
		OpIndex:       f.OpIndex,
		FromLedger:    f.FromLedger,
		ToLedger:      f.ToLedger,
		FromTime:      timeOrEmpty(f.FromTime),
		ToTime:        timeOrEmpty(f.ToTime),
		Cursor:        f.Cursor,
		Limit:         resolvedLimit(f.Limit),
		Order:         resolvedOrder(f.Order),
		Scope:         f.Scope.Fingerprint(),
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
	writeVary(w)
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
		if cachePrivate.Load() || tenantScoped.Load() {
			// Auth'd deployments get `private`: caching stays scoped to
			// the authenticated user (browser cache works), but shared
			// caches (CDN/proxy) cannot pool responses across users.
			//
			// Multi-tenant mode forces this regardless of CACHE_PRIVATE.
			// The setting is an operator preference; the tenant boundary
			// is not, and `public` on a tenant-scoped body is a
			// cross-tenant disclosure waiting for a CDN to happen.
			scope = "private"
		}
		w.Header().Set("Cache-Control",
			fmt.Sprintf("%s, max-age=%d, immutable", scope, int(maxAge.Seconds())))
	}
}

// varyDimensions are the request headers a response can depend on.
// Accept-Encoding is set proactively for the compression middleware (#25);
// the credential headers are added in multi-tenant mode because the body
// genuinely differs per credential.
func varyDimensions() []string {
	if tenantScoped.Load() {
		return []string{"Accept-Encoding", "Authorization", "X-API-Key"}
	}
	return []string{"Accept-Encoding"}
}

// writeVary merges the response's Vary dimensions into whatever a previous
// middleware may already have set, rather than overwriting — distinct
// dimensions have to coexist in the one comma-separated value a cache reads.
func writeVary(w http.ResponseWriter) {
	vary := w.Header().Get("Vary")
	for _, dim := range varyDimensions() {
		if strings.Contains(vary, dim) {
			continue
		}
		if vary == "" {
			vary = dim
		} else {
			vary += ", " + dim
		}
	}
	if vary != "" {
		w.Header().Set("Vary", vary)
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

// ptr returns a pointer to v. Used for the tri-state query params
// (nil = unset) where a bare &true is not valid Go.
func ptr[T any](v T) *T { return &v }

// namedParam carries a query value together with the spelling the caller
// actually used, so an error message names the parameter they typed.
type namedParam struct {
	name  string
	value string
}

// timeParamAlias resolves a parameter that accepts two spellings. Passing
// both is an error: they bound the same column, so honouring one silently
// would make the ignored value look effective.
func timeParamAlias(q url.Values, primary, alias string) (namedParam, error) {
	pv, av := q.Get(primary), q.Get(alias)
	if pv != "" && av != "" {
		return namedParam{}, fmt.Errorf("%s and %s are aliases; set only one", primary, alias)
	}
	if av != "" {
		return namedParam{name: alias, value: av}, nil
	}
	return namedParam{name: primary, value: pv}, nil
}

// filterFromQuery parses the shared event-filter query params:
// contract_id, type, topic, from_ledger, to_ledger, from_time (alias
// created_after), to_time (alias created_before), cursor, limit.
//
// It is also the one place a list-shaped read acquires its authorization.
// Every endpoint that returns events builds its filter here, so the tenant
// boundary is attached by construction rather than by each handler
// remembering to attach it. A handler that hand-rolled an EventFilter
// instead would get the zero Scope and return nothing — see store.Scope for
// why that is the failure mode we chose.
// The parsing rules and validation live in the shared queries package so
// the GraphQL resolvers in internal/api/graphql can reuse them — there is
// exactly one source of truth for which topic positions are valid, what
// counts as an "invalid order", etc.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()

	var fromLedger, toLedger int64
	var fromTime, toTime time.Time
	var err error

	if fromLedger, err = queries.ParseLedgerParam(q.Get("from_ledger")); err != nil {
		// Match the historical REST error message: prefix the param name
		// so users see exactly which value was bad.
		return store.EventFilter{}, fmt.Errorf("from_ledger %s", err.Error())
	}
	if toLedger, err = queries.ParseLedgerParam(q.Get("to_ledger")); err != nil {
		return store.EventFilter{}, fmt.Errorf("to_ledger %s", err.Error())
	}

	// created_after / created_before are aliases for from_time / to_time:
	// both bound events.created_at. The alias reads more naturally next to
	// the created_at field it filters, and callers already using the
	// original names are unaffected. Supplying both spellings of the same
	// bound is rejected rather than silently picking one.
	fromTimeRaw, err := timeParamAlias(q, "from_time", "created_after")
	if err != nil {
		return store.EventFilter{}, err
	}
	toTimeRaw, err := timeParamAlias(q, "to_time", "created_before")
	if err != nil {
		return store.EventFilter{}, err
	}
	if fromTime, err = queries.ParseTimeParam(fromTimeRaw.value); err != nil {
		return store.EventFilter{}, fmt.Errorf("%s %s", fromTimeRaw.name, err.Error())
	}
	if toTime, err = queries.ParseTimeParam(toTimeRaw.value); err != nil {
		return store.EventFilter{}, fmt.Errorf("%s %s", toTimeRaw.name, err.Error())
	}

	types, err := queries.ParseTypes(q.Get("type"))
	if err != nil {
		return store.EventFilter{}, err
	}

	topic, err := queries.ParseTopic(q.Get("topic"))
	if err != nil {
		return store.EventFilter{}, fmt.Errorf("topic: %w", err)
	}
	t0, err := queries.ParseTopic(q.Get("topic0"))
	if err != nil {
		return store.EventFilter{}, fmt.Errorf("topic0: %w", err)
	}
	t1, err := queries.ParseTopic(q.Get("topic1"))
	if err != nil {
		return store.EventFilter{}, fmt.Errorf("topic1: %w", err)
	}
	t2, err := queries.ParseTopic(q.Get("topic2"))
	if err != nil {
		return store.EventFilter{}, fmt.Errorf("topic2: %w", err)
	}
	t3, err := queries.ParseTopic(q.Get("topic3"))
	if err != nil {
		return store.EventFilter{}, fmt.Errorf("topic3: %w", err)
	}
	tc, err := queries.ParseTopicContains(q.Get("topic_contains"))
	if err != nil {
		return store.EventFilter{}, err
	}

	args := queries.EventFilterArgs{
		ContractID:    q.Get("contract_id"),
		Types:         types,
		Topic:         topic,
		T0:            t0,
		T1:            t1,
		T2:            t2,
		T3:            t3,
		TopicContains: tc,
		TxHash:        q.Get("tx_hash"),
		FromLedger:    fromLedger,
		ToLedger:      toLedger,
		FromTime:      fromTime,
		ToTime:        toTime,
		Order:         q.Get("order"),
		OrderBy:       q.Get("order_by"),
		Cursor:        q.Get("cursor"),
	}

	// ?limit=N: explicit validation here so an explicit `?limit=0` (or
	// any value outside [1,MaxQueryLimit]) is a 400. BuildingEventFilter
	// treats args.Limit==0 as "use default" — we only invoke the
	// default when the param is absent, not when the caller explicitly
	// asked for an invalid value.
	if raw := q.Get("limit"); raw != "" {
		limit, lerr := strconv.Atoi(raw)
		if lerr != nil || limit < 1 || limit > store.MaxQueryLimit {
			return store.EventFilter{}, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit)
		}
		args.Limit = limit
	}

	f, err := queries.BuildEventFilter(args)
	if err != nil {
		return f, err
	}

	// Scope is attached here, the single place REST list filters are built:
	// queries.BuildEventFilter is shared with the GraphQL resolvers and
	// deliberately knows nothing about HTTP authentication.
	f.Scope = scopeFrom(r.Context())

	// An explicitly named contract the tenant lacks is refused rather than
	// quietly filtered to nothing, so a missing grant is distinguishable
	// from a contract with no events. The store still ANDs the scope into
	// the query regardless, so this check being wrong or removed downgrades
	// the error message without opening a leak.
	if f.ContractID != "" && !f.Scope.Allows(f.ContractID) {
		return f, errForbiddenContract{contractID: f.ContractID}
	}

	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		return f, fmt.Errorf("invalid cursor %q", f.Cursor)
	}

	if rawTx := q.Get("tx_index"); rawTx != "" {
		txIdx, err := strconv.Atoi(rawTx)
		if err != nil || txIdx < 0 {
			return f, fmt.Errorf("invalid tx_index %q (want a non-negative integer)", rawTx)
		}
		f.TxIndex = ptr(int32(txIdx))
	}
	if rawOp := q.Get("op_index"); rawOp != "" {
		opIdx, err := strconv.Atoi(rawOp)
		if err != nil || opIdx < 0 {
			return f, fmt.Errorf("invalid op_index %q (want a non-negative integer)", rawOp)
		}
		f.OpIndex = ptr(int32(opIdx))
	}

	switch raw := q.Get("in_successful_call"); raw {
	case "":
		// nil — no constraint
	case "true":
		f.InSuccessfulCall = ptr(true)
	case "false":
		f.InSuccessfulCall = ptr(false)
	default:
		return f, fmt.Errorf("invalid in_successful_call %q (want true or false)", raw)
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxLimit {
			return f, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit)
		}
		f.Limit = limit
	} else {
		f.Limit = store.DefaultQueryLimit
	}

	// ?recent=N: shorthand for "newest N events" — sets order=desc and
	// limit=N (default 20). This isn't a general-purpose filter (it
	// conflicts with explicit order/limit/order_by), so we keep the
	// branch in the REST layer and apply it AFTER BuildEventFilter so
	// the shared builder doesn't need a "shorthand mode" affordance.
	if raw := q.Get("recent"); raw != "" {
		if q.Get("order") != "" || q.Get("order_by") != "" {
			return f, fmt.Errorf("recent cannot be combined with order or order_by")
		}
		if q.Get("limit") != "" {
			return f, fmt.Errorf("recent cannot be combined with limit")
		}
		n := recentDefaultLimit
		if raw != "true" {
			n, err = strconv.Atoi(raw)
			if err != nil || n < 1 || n > maxLimit {
				return f, fmt.Errorf("recent must be a positive integer in [1,%d]", maxLimit)
			}
		}
		f.Order = "desc"
		f.Limit = n
	}

	if raw := q.Get("has_value"); raw != "" {
		switch raw {
		case "true":
			t := true
			f.HasValue = &t
		case "false":
			v := false
			f.HasValue = &v
		default:
			return f, fmt.Errorf("has_value must be true or false, got %q", raw)
		}
	}

	return f, nil
}

// DefaultStreamScopeSync bounds how long a live stream can keep serving a
// contract after the tenant's grant was revoked (and how long it waits for a
// newly granted one to start flowing).
const DefaultStreamScopeSync = 30 * time.Second

// syncStreamScope re-resolves the tenant's grants periodically and pushes
// them into the live subscription.
//
// Without this, the answer to the issue's "a tenant granted a contract
// mid-stream" question would be "nothing happens until they reconnect" —
// and, worse, the mirror-image case would be that revoking a grant does not
// stop delivery to anyone already connected. A stream is exactly the place
// where a snapshot-at-open authorization decision decays, because the
// snapshot can outlive the entitlement by hours.
//
// Single-tenant deployments start no goroutine: their scope is a constant.
func (s *Server) syncStreamScope(ctx context.Context, sub *broadcast.Subscription) {
	if !s.multiTenant {
		return
	}
	p, ok := PrincipalFrom(ctx)
	if !ok || p.Untenanted {
		return
	}
	every := s.streamScopeSync
	if every <= 0 {
		every = DefaultStreamScopeSync
	}
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Re-read the tenant too, so a suspension (enabled=false)
				// or a switch away from wildcard takes effect on open
				// streams and not just on new requests.
				tenant, err := s.tenants.GetTenant(ctx, p.Tenant.ID)
				if errors.Is(err, store.ErrNotFound) {
					// Deleted mid-stream: revoke everything. The read loop
					// sees an empty scope and the connection goes quiet
					// rather than continuing to serve a tenant that no
					// longer exists.
					sub.SetScope(store.Scope{})
					return
				}
				if err != nil {
					// Leave the existing scope in place on a transient
					// database error: it was correct as of the last
					// successful resolve, and widening or narrowing on a
					// failed read would be guessing.
					s.log.Warn("refreshing stream scope", "tenant", p.Tenant.ID, "error", err)
					continue
				}
				if !tenant.Enabled {
					sub.SetScope(store.Scope{})
					return
				}
				scope, err := s.tenants.ScopeForTenant(ctx, tenant)
				if err != nil {
					s.log.Warn("refreshing stream scope", "tenant", p.Tenant.ID, "error", err)
					continue
				}
				sub.SetScope(scope)
			}
		}
	}()
}

func (s *Server) handleEventStreamWS(w http.ResponseWriter, r *http.Request) {
	if s.bcast == nil {
		http.Error(w, "streaming not configured", http.StatusNotImplemented)
		return
	}

	filter, fields, err := parseFilterAndFields(r)
	if err != nil {
		writeFilterError(w, err)
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

	// Attribute the connection's wall-clock duration to the tenant when it
	// ends, however it ends.
	streamStart := time.Now()
	defer func() { s.recordStreamTime(r.Context(), time.Since(streamStart)) }()

	// Keep the subscription's authorization current for its whole life.
	s.syncStreamScope(ctx, sub)

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

// prettyWriter is implemented by ResponseWriter wrappers that carry the
// ?pretty flag so writeJSON can optionally indent the output.
type prettyWriter interface {
	Pretty() bool
}

// prettyResponseWriter wraps an http.ResponseWriter to carry the ?pretty flag
// through the middleware chain. All ResponseWriter methods delegate to the
// embedded writer so compression, flushing, and hijacking still work.
type prettyResponseWriter struct {
	http.ResponseWriter
	pretty bool
}

func (w *prettyResponseWriter) Pretty() bool { return w.pretty }

// Flush forwards to the embedded ResponseWriter if it supports flushing.
// This is required because interface embedding does not promote optional
// interfaces like http.Flusher — without it, the NDJSON stream handler's
// w.(http.Flusher) type assertion would fail.
func (w *prettyResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the embedded ResponseWriter if it supports hijacking,
// matching the pattern used by compressWriter.
func (w *prettyResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// prettyMiddleware reads ?pretty=true from the query string and wraps the
// ResponseWriter with the flag so writeJSON can set indentation when
// requested. It must be the innermost middleware (closest to the handler)
// so the type assertion in writeJSON sees the wrapper.
func prettyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pw := &prettyResponseWriter{ResponseWriter: w, pretty: r.URL.Query().Get("pretty") == "true"}
		next.ServeHTTP(pw, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	if pw, ok := w.(prettyWriter); ok && pw.Pretty() {
		enc.SetIndent("", "  ")
	}
	_ = enc.Encode(v)
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
