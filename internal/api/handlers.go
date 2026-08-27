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
	"reflect"

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

type eventWithXDR struct {
	Event     store.Event `json:"-"`
	TopicsXDR []string    `json:"topics_xdr"`
	ValueXDR  *string     `json:"value_xdr,omitempty"`
}

type enrichedEventWithXDR struct {
	eventWithXDR
	DecodedEvent *store.DecodedEventResponse `json:"decoded_event,omitempty"`
	Decoded      bool                        `json:"decoded"`
}

type enrichedEventsWithXDRResponse struct {
	Events []enrichedEventWithXDR `json:"events"`
	Cursor string                 `json:"cursor,omitempty"`
}

type eventsWithXDRResponse struct {
	Events []eventWithXDR `json:"events"`
	Cursor string         `json:"cursor,omitempty"`
}

// envelopeResponse is the JSON body returned when ?envelope=true is set on
// any paginated list endpoint. It normalises the response shape across all
// list endpoints so clients that prefer a consistent outer wrapper don't
// have to inspect endpoint-specific field names.
//
// Fields:
//
//	data        – the page items (array, never null).
//	next_cursor – opaque pagination cursor; empty/absent when exhausted.
type envelopeResponse struct {
	Data       any    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// wrapEnvelope builds an envelopeResponse from a page of items and the
// next-page cursor. It is a convenience constructor so call sites stay
// single-line.
func wrapEnvelope(data any, cursor string) envelopeResponse {
	// data is documented as "array, never null". A handler with no rows to
	// return usually holds a nil slice, which marshals to null and forces
	// every client to handle both shapes; normalise it to an empty array of
	// the same element type here, at the one place every caller passes
	// through.
	if rv := reflect.ValueOf(data); rv.Kind() == reflect.Slice && rv.IsNil() {
		data = reflect.MakeSlice(rv.Type(), 0, 0).Interface()
	}
	return envelopeResponse{Data: data, NextCursor: cursor}
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

type versionResponse struct {
	Version string `json:"version"`

	Commit string `json:"commit"`

	BuildDate string `json:"build_date"`
}

// eventFieldNames is the set of JSON keys on store.Event that the ?fields=
// allowlist accepts.
var eventFieldNames = map[string]bool{

	"id": true,

	"contract_id": true,

	"ledger": true,

	"type": true,

	"tx_hash": true,

	"tx_index": true,

	"op_index": true,

	"in_successful_call": true,

	"topics": true,

	"value": true,

	"created_at": true,
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

// handleMetrics serves the Prometheus /metrics endpoint. The response is
// always cacheNoStore so scrapers never see a stale snapshot.
func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok", Checks: map[string]string{"process": "ok"}})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: map[string]string{
		"database":      "ok",
		"rpc":           "ok",
		"ingestion_lag": "ok",
	}}
	status := http.StatusOK

	if err := s.store.Ping(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["database"] = err.Error()
		status = http.StatusServiceUnavailable
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

	// Check ingestion lag: if we're more than 100 ledgers behind the chain
	// head, mark the instance as not ready (it should be pulled from the
	// load balancer until it catches up).
	if state, err := s.store.GetIngestionState(ctx); err == nil {
		// Only check lag if we have an ingestion state and an RPC client.
		if s.rpc != nil {
			if health, err := s.rpc.GetHealth(ctx); err == nil {
				lag := ingestLagLedgers(int64(health.LatestLedger), state.LastIngestedLedger)
				if lag > 100 && state.LastIngestedLedger > 0 {
					resp.Status = "degraded"
					resp.Checks["ingestion_lag"] = fmt.Sprintf(
						"%d ledgers behind (last ingested: %d, chain head: %d)",
						lag, state.LastIngestedLedger, health.LatestLedger)
					status = http.StatusServiceUnavailable
				}
			}
		}
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

// countResponse is the JSON body for GET /events/count.
type countResponse struct {
	Count int64 `json:"count"`
}

// bucketResponse is the JSON body for GET /events/aggregate.
type bucketResponse struct {
	Buckets []AggregateBucket `json:"buckets"`
}

// AggregateBucket is one bucket in an aggregation result.
type AggregateBucket = store.AggregateBucket

// handleAggregateEvents returns event counts grouped by ledger or
// by a time interval. The ?bucket parameter controls the grouping
// and accepts "ledger" or a Go duration string (e.g. "1h", "1d").
// All other event filter params (contract_id, type, from_ledger,
// to_ledger, from_time, to_time, topic, topic0..topic3,
// topic_contains, tx_hash) are accepted and applied to the
// aggregation. Pagination params (cursor, limit, order, order_by)
// are accepted in the URL but ignored.
func (s *Server) handleAggregateEvents(w http.ResponseWriter, r *http.Request) {
	filter, _, err := parseFilterAndFields(r)
	if err != nil {

		writeError(w, http.StatusBadRequest, err)

		return

	}
	filter.Cursor = ""
	filter.Order = ""
	filter.OrderBy = ""
	filter.Limit = 0

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeError(w, http.StatusBadRequest, errors.New("bucket parameter is required"))
		return
	}
	if bucket != "ledger" {
		if _, err := time.ParseDuration(bucket); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid bucket %q: must be \"ledger\" or a duration", bucket))
			return
		}
	}

	buckets, err := s.store.AggregateEvents(r.Context(), filter, bucket)
	if err != nil {
		loggerFromContext(r.Context()).Error("aggregating events", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("aggregating events failed"))
		return
	}
	writeCacheHeaders(w, cacheNoCache, 0, "")
	writeJSON(w, http.StatusOK, bucketResponse{Buckets: buckets})
}

// streamBatchSize is the number of events fetched per internal query when

// streaming NDJSON. It balances query cost against flush frequency: too

// small wastes round trips; too large buffers too long before a client

// sees progress.

const streamBatchSize = 500

// recentDefaultLimit is the page size applied by ?recent=N shorthand when
// the value is the bare "true" (no explicit count).
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

	// prevCursor tracks the last cursor fed back into the store so a
	// backend that keeps returning the same page (a stuck or buggy
	// driver, a misbehaving store) cannot spin this loop forever: no
	// cursor progress means the stream is done.
	prevCursor := filter.Cursor

	for cursor != "" {

		if cursor == prevCursor {
			break
		}
		prevCursor = cursor

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
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter, fields map[string]bool) {

	policy, etag, err := s.listCachePolicy(r.Context(), filter)

	if err != nil {
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

	// A missing page must serialize as [] rather than null so envelope
	// consumers can iterate unconditionally.
	if events == nil {
		events = []store.Event{}
	}

	setPaginationHeaders(w, r, cursor)

	// Tag every event with its SEP-41 normalized envelope (if any) before
	// rendering — the layer is additive and never destructive, so events
	// that do not match keep exactly the same shape they had before.
	for i := range events {
		events[i].WithSEP41()
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
	envelope := r.URL.Query().Get("envelope") == "true"
	writeCacheHeaders(w, policy, immutableMaxAge, etag)

	// RFC 5988 pagination links: when the store reports a next-page
	// cursor, hand clients ready-made URLs instead of making them
	// reassemble one. All original query params are preserved so pages
	// keep the caller's filter; a request that arrived mid-pagination
	// also advertises the way back to the first page via rel="prev".
	if cursor != "" {
		var links []string
		if r.URL.Query().Get("cursor") != "" {
			first := r.URL.Query()
			first.Del("cursor")
			prev := url.URL{Path: r.URL.Path, RawQuery: first.Encode()}
			links = append(links, fmt.Sprintf("<%s>; rel=\"prev\"", prev.String()))
		}
		q := r.URL.Query()
		q.Set("cursor", cursor)
		next := url.URL{Path: r.URL.Path, RawQuery: q.Encode()}
		links = append(links, fmt.Sprintf("<%s>; rel=\"next\"", next.String()))
		w.Header().Set("Link", strings.Join(links, ", "))
	}

	if decoded && s.enricher != nil {

		enriched := s.enricher.EnrichEvents(r.Context(), events)
		// The enriched branch writes no cache headers, so the Vary the
		// scoped path relies on is set explicitly here too. Without it a
		// shared cache could key ?decoded=true responses on URL alone.
		writeVary(w)
		if includeXDR {
			items := enrichEventsWithXDR(enriched)
			if envelope {
				writeJSON(w, http.StatusOK, wrapEnvelope(items, cursor))
				return
			}
			writeJSON(w, http.StatusOK, enrichedEventsWithXDRResponse{
				Events: items,
				Cursor: cursor,
			})

			return

		}
		if envelope {
			writeJSON(w, http.StatusOK, wrapEnvelope(enriched, cursor))
			return

		}

		writeJSON(w, http.StatusOK, enrichedEventsResponse{Events: enriched, Cursor: cursor})

		return

	}

	if includeXDR {
		items := eventsWithXDR(events)
		if envelope {
			writeJSON(w, http.StatusOK, wrapEnvelope(items, cursor))
			return
		}
		writeJSON(w, http.StatusOK, eventsWithXDRResponse{
			Events: items,
			Cursor: cursor,
		})

		return

	}

	if fields == nil {
		if envelope {
			writeJSON(w, http.StatusOK, wrapEnvelope(events, cursor))
			return
		}
		writeJSON(w, http.StatusOK, eventsResponse{Events: events, Cursor: cursor})

	} else {

		m := map[string]any{"events": projectEvents(events, fields)}

		if cursor != "" {

			m["cursor"] = cursor

		}
		if envelope {
			writeJSON(w, http.StatusOK, wrapEnvelope(projectEvents(events, fields), cursor))
			return
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
	if len(event.RawTopicXDR) == 0 && event.RawValueXDR == "" {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q has no stored raw XDR", id))
		return
	}

	etag := `"` + id + `:raw"`
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

// rawEventResponse is the JSON body for GET /events/{id}/raw.
type rawEventResponse struct {
	TopicsXDR []string `json:"topics_xdr"`
	ValueXDR  string   `json:"value_xdr,omitempty"`
}

// handleGetEventTransaction returns all sibling events from the same
// transaction as the event with the given {id}. The referenced event
// itself is excluded from the response. Returns 404 when the event is
// not found; returns an empty list when the transaction has no other
// events.
func (s *Server) handleGetEventTransaction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	// Both the existence probe and the row fetch below run under this
	// scope, so an event belonging to an ungranted contract is reported as
	// absent on every path through this handler — including the 304 fast
	// path, which would otherwise be a free existence oracle.
	scope := scopeFrom(r.Context())

	// Validate ?fields= before touching the store.
	fields, err := parseFields(r.URL.Query().Get("fields"))

	if err != nil {

		writeError(w, http.StatusBadRequest, err)

		return

	}

	event, err := s.store.GetEvent(r.Context(), id, scope)
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

	etag := `"` + id + `"`

	if ifNoneMatch(r, etag) {
		exists, err := s.store.EventExists(r.Context(), id, scope)
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

	// Additive SEP-41 normalization on the single-event path; non-matches
	// simply omit the field.
	event.WithSEP41()

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

		Event: e,

		TopicsXDR: e.RawTopicXDR,

		ValueXDR: value,
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

		Decoded: e.Decoded,
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

// maxContractsListLimit caps ?limit= on GET /contracts. The listing is a
// grouped scan over the events table (one row per contract), so its page
// size is bounded tighter than the event endpoints — 1–200 with a default
// of 50, matching the shared OpenAPI Limit parameter.
const maxContractsListLimit = 200

// handleListContracts returns one ContractSummary per indexed contract,
// paginated, default-ordered by contract_id ascending (a stable
// alphabetical walk; ?sort=count ranks by activity instead). The endpoint
// is intentionally READ-ONLY and unauthenticated: a contract listing has
// no surface area for cross-tenant data leakage (a contract_id is
// opaque), and gating it behind API_KEY would force every browser
// dashboard to log in.
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
			"invalid sort %q (want %s, %s, %s, %s, or %s)",
			f.SortKey,
			store.SortByContractID, store.SortByActivity,
			store.SortByFirstLedger, store.SortByLastLedger,
			store.SortByLastSeen))
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
		if err != nil || n < 1 || n > maxContractsListLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxContractsListLimit))
			return
		}
		f.Limit = n
	} else {
		f.Limit = store.DefaultQueryLimit
	}
	items, cursor, err := s.store.ListContracts(r.Context(), f)
	if err != nil {
		// A cursor that passes the charset check but not the decode is
		// bad client input (400), not a server fault (500).
		if errors.Is(err, store.ErrInvalidContractsCursor) {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid cursor %q", f.Cursor))
			return
		}
		loggerFromContext(r.Context()).Error("listing contracts", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing contracts failed"))
		return
	}
	total, cerr := s.store.CountContracts(r.Context(), f)
	if cerr != nil {
		loggerFromContext(r.Context()).Warn("counting contracts for X-Total-Count", "error", cerr)
	} else {
		// The header is set even for an empty result set: a client that
		// reads "0" learns the endpoint is healthy and there is simply
		// nothing to page, whereas an absent header is ambiguous.
		w.Header().Set("X-Total-Count", fmt.Sprintf("%d", total))
	}
	if items == nil {
		items = []store.ContractSummary{}
	}
	writeCacheHeaders(w, cacheNoCache, 0, "")
	if r.URL.Query().Get("envelope") == "true" {
		writeJSON(w, http.StatusOK, wrapEnvelope(items, cursor))
		return
	}
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
		if err != nil || n < 1 || n > maxLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit))
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

	// Total matching count (ignoring pagination) as a response header,
	// following the events pattern exactly: a failed count is logged and
	// the header omitted, never a failed request.
	if total, cerr := s.store.CountDeadLetters(r.Context(), f.ContractID); cerr != nil {
		loggerFromContext(r.Context()).Warn("counting dead letters for X-Total-Count", "error", cerr)
	} else {
		w.Header().Set("X-Total-Count", fmt.Sprintf("%d", total))
	}

	writeCacheHeaders(w, cacheNoStore, 0, "")
	if r.URL.Query().Get("envelope") == "true" {
		if items == nil {
			items = []store.DeadLetter{}
		}
		writeJSON(w, http.StatusOK, wrapEnvelope(items, cursor))
		return
	}
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

// handleStats serves the aggregate /stats response. The store aggregation and
// RPC freshness lookup are the expensive parts, so the assembled result is
// cached per tenant-scope for statsTTL; a request that lands within the window
// is served from cache without touching the database. After the TTL expires
// the next request recomputes, so values refresh automatically without a
// background timer.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	key := scopeFrom(r.Context()).Fingerprint()
	if stats, ok := s.getStatsCache().Get(key, time.Now()); ok {
		writeCacheHeaders(w, cacheNoStore, 0, "")
		writeJSON(w, http.StatusOK, stats)
		return
	}

	stats, err := s.assembleStats(r.Context())
	if err != nil {
		loggerFromContext(r.Context()).Error("loading stats", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading stats failed"))
		return
	}

	s.getStatsCache().Put(key, stats, time.Now())
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, stats)
}

// getStatsCache returns the server's per-scope cache, building it lazily on
// first use so a Server constructed without a configured TTL never allocates
// one until /stats is actually hit with caching enabled.
func (s *Server) getStatsCache() *StatsCache {
	if s.statsCache == nil {
		s.statsCache = newStatsCache(s.statsTTL)
	}
	return s.statsCache
}

// assembleStats computes the full /stats payload: the store aggregate, the
// RPC freshness fields, and the in-memory process counters (auditor, pruner,
// recoverer, RPC errors). Callers cache the result keyed by tenant scope.
func (s *Server) assembleStats(ctx context.Context) (store.Stats, error) {
	stats, err := s.store.Stats(ctx, scopeFrom(ctx))
	if err != nil {
		return store.Stats{}, err
	}

	s.addStatsFreshness(ctx, &stats)

	stats.PanicsRecovered = s.recoverer.PanicsRecovered()

	if a := getAuditor(); a != nil {

		m := a.Metrics()

		stats.Auditor = store.AuditStats{

			PassesRun: m.PassesRun,

			LedgersChecked: m.LedgersChecked,

			FindingsOpened: m.FindingsOpened,

			FindingsRepaired: m.FindingsRepaired,

			FindingsUnverifiable: m.FindingsUnverifiable,

			FindingsUnrecoverable: m.FindingsUnrecoverable,

			RPCRequests: m.RPCRequests,
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

			GetEvents: snap.GetEvents,

			GetLatestLedger: snap.GetLatestLedger,

			GetHealth: snap.GetHealth,

			GetLedgerEntries: snap.GetLedgerEntries,
		}

	}
	return stats, nil
}

// Watched contracts types.

type addWatchedRequest struct {
	ContractID string `json:"contract_id"`
}

type addWatchedResponse struct {
	ContractID string `json:"contract_id"`

	AddedAt string `json:"added_at"`

	HistoryFromLedger int64  `json:"history_from_ledger"`
	ModeTransition    string `json:"mode_transition,omitempty"`
}

type removeWatchedResponse struct {
	ContractID string `json:"contract_id"`

	RemovedAt string `json:"removed_at"`

	HistoryPreserved bool `json:"history_preserved"`

	ModeTransition string `json:"mode_transition,omitempty"`
}

type watchedListResponse struct {
	Contracts []store.WatchedContract `json:"contracts"`

	Count int `json:"count"`
}

func (s *Server) handleListWatchedChains(w http.ResponseWriter, r *http.Request) {

	contracts, err := s.store.ListWatchedContracts(r.Context())

	if err != nil {

		s.log.Error("listing watched contracts", "error", err)

		writeError(w, http.StatusInternalServerError, errors.New("loading watched contracts failed"))

		return

	}

	// The whole watch list is returned on one page, so the total is just
	// the page size; no separate count query is needed.
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(contracts)))

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

	// history_from_ledger tells the caller where ingestion for this
	// contract will begin: an existing per-contract cursor wins (re-adding
	// a contract resumes, never re-backfills); otherwise the cold-start
	// retention window - latest minus RETENTION_LEDGERS, clamped to what
	// the RPC still retains - is where the automatic backfill starts.
	historyFrom := state.LastIngestedLedger
	if cc, cerr := s.store.GetContractCursor(r.Context(), req.ContractID); cerr == nil {
		historyFrom = cc.LastIngestedLedger
	} else if errors.Is(cerr, store.ErrNotFound) {
		if health, herr := s.rpc.GetHealth(r.Context()); herr == nil {
			start := int64(health.LatestLedger) - int64(s.retentionWindow())
			if oldest := int64(health.OldestLedger); start < oldest {
				start = oldest
			}
			if start < 2 {
				start = 2
			}
			historyFrom = start
		}
	}

	writeJSON(w, http.StatusOK, addWatchedResponse{

		ContractID: req.ContractID,

		AddedAt: time.Now().UTC().Format(time.RFC3339),

		HistoryFromLedger: historyFrom,

		ModeTransition: modeTransition,
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

		ContractID: id,

		RemovedAt: time.Now().UTC().Format(time.RFC3339),

		HistoryPreserved: true,

		ModeTransition: modeTransition,
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
	envelope := r.URL.Query().Get("envelope") == "true"
	if envelope {
		writeJSON(w, http.StatusOK, wrapEnvelope(events, cursor))
		return
	}
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
	if chainHead <= 0 || lastIngested <= 0 {
		return 0
	}
	lag := chainHead - lastIngested
	if lag < 0 {
		return 0
	}
	return lag
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

// lastIngestedLedger reads the frontier from the persisted ingestion state.
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

func listETag(f store.EventFilter) string {
	key := struct {
		ContractID string `json:"c"`

		ContractIDPrefix string   `json:"cp,omitempty"`
		Types            []string `json:"t"`

		Topic json.RawMessage `json:"p,omitempty"`

		Topic0 json.RawMessage `json:"p0,omitempty"`

		Topic1 json.RawMessage `json:"p1,omitempty"`

		Topic2 json.RawMessage `json:"p2,omitempty"`

		Topic3 json.RawMessage `json:"p3,omitempty"`

		TopicContains json.RawMessage `json:"pc,omitempty"`

		TxHash string `json:"th,omitempty"`

		HasValue   *bool  `json:"hv,omitempty"`
		TxIndex    *int32 `json:"txi,omitempty"`
		OpIndex    *int32 `json:"opi,omitempty"`
		FromLedger int64  `json:"fl"`

		ToLedger int64 `json:"tl"`

		FromTime string `json:"ft,omitempty"`

		ToTime string `json:"tt,omitempty"`

		Cursor string `json:"cu,omitempty"`

		Limit int `json:"l"`

		Order string `json:"o,omitempty"`

		// Scope makes the validator tenant-specific. Two tenants issuing
		// the same request are asking for different representations of
		// this URL, and without this component the second one's
		// If-None-Match would match the first one's page and be answered
		// 304 — a cross-tenant disclosure produced entirely inside this
		// server, with no CDN involved.
		Scope string `json:"s"`
	}{
		ContractID:       f.ContractID,
		ContractIDPrefix: f.ContractIDPrefix,
		Types:            f.Types,
		Topic:            f.Topic,
		// Each positional filter gets its own distinctly named key, so

		// topic0={x} and topic1={x} — which select different events — cannot

		// serialize identically.

		Topic0: f.Topic0,

		Topic1: f.Topic1,

		Topic2: f.Topic2,

		Topic3: f.Topic3,

		TopicContains: f.TopicContains,

		TxHash: f.TxHash,

		HasValue:   f.HasValue,
		TxIndex:    f.TxIndex,
		OpIndex:    f.OpIndex,
		FromLedger: f.FromLedger,

		ToLedger: f.ToLedger,

		FromTime: timeOrEmpty(f.FromTime),

		ToTime: timeOrEmpty(f.ToTime),
		Cursor: f.Cursor,

		Limit: resolvedLimit(f.Limit),

		Order: resolvedOrder(f.Order),
		Scope: f.Scope.Fingerprint(),
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

// setPaginationHeaders emits RFC 5988 Link headers for the event list
// endpoints: rel="next" whenever the store returned a continuation cursor,
// and rel="prev" whenever the caller supplied one. It must run before the
// body is written, since writeJSON commits the status line.
func setPaginationHeaders(w http.ResponseWriter, r *http.Request, nextCursor string) {
	var links []string
	if nextCursor != "" {
		links = append(links, fmt.Sprintf(`<%s>; rel="next"`, paginationLink(r, nextCursor)))
	}
	if _, ok := r.URL.Query()["cursor"]; ok {
		links = append(links, fmt.Sprintf(`<%s>; rel="prev"`, paginationLink(r, "")))
	}
	if len(links) > 0 {
		w.Header().Set("Link", strings.Join(links, ", "))
	}
}

// paginationLink rebuilds the current request URL with cursor set to the
// given value (or removed, for the prev link), preserving every other query
// parameter so a client can follow the link without re-deriving its filters.
func paginationLink(r *http.Request, cursor string) string {
	q := r.URL.Query()
	if cursor == "" {
		q.Del("cursor")
	} else {
		q.Set("cursor", cursor)
	}

	scheme := "http"
	if r.URL.Scheme != "" {
		scheme = r.URL.Scheme
	} else if r.TLS != nil {
		scheme = "https"
	}

	host := r.Host
	if host == "" {
		host = r.URL.Host
	}

	u := &url.URL{Scheme: scheme, Host: host, Path: r.URL.Path, RawQuery: q.Encode()}
	if r.URL.RawPath != "" {
		u.RawPath = r.URL.RawPath
	}
	return u.String()
}

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

// filterFromQuery parses the shared event-filter query params:

// contract_id, type, topic, from_ledger, to_ledger, from_time, to_time, cursor, limit.
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

	if fromTime, err = queries.ParseTimeParam(q.Get("from_time")); err != nil {
		return store.EventFilter{}, fmt.Errorf("from_time %s", err.Error())
	}
	if toTime, err = queries.ParseTimeParam(q.Get("to_time")); err != nil {
		return store.EventFilter{}, fmt.Errorf("to_time %s", err.Error())
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

	// contract_ids is a comma-separated list of contract IDs. When present,
	// each element must be a valid contract strkey. The empty string (no
	// parameter) or a single value without commas behave identically to the
	// original ?contract_id= — no breaking change to existing callers.
	var contractIDs []string
	if raw := q.Get("contract_id"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			// The doc comment above promises each element is a valid
			// contract strkey; enforce it rather than passing a typo
			// through to a query that silently matches nothing.
			if !config.ValidContractID(part) {
				return store.EventFilter{}, fmt.Errorf("invalid contract_id %q", part)
			}
			contractIDs = append(contractIDs, part)
		}
	}
	// Backward compatibility: a single contract_id without commas still
	// sets ContractID so existing callers (handleContractEvents, GraphQL)
	// are unaffected. When multiple IDs are given, ContractID is left empty
	// and ContractIDs carries the full list.
	var singleID string
	if len(contractIDs) == 1 {
		singleID = contractIDs[0]
		contractIDs = nil
	}

	args := queries.EventFilterArgs{
		// singleID, not the raw param: a lone contract_id keeps the
		// historical single-ID behaviour, while a comma-separated list is
		// carried by ContractIDs below.
		ContractID:       singleID,
		ContractIDPrefix: q.Get("contract_id_prefix"),
		Types:            types,
		Topic:            topic,
		T0:               t0,
		T1:               t1,
		T2:               t2,
		T3:               t3,
		TopicContains:    tc,
		TxHash:           q.Get("tx_hash"),
		FromLedger:       fromLedger,
		ToLedger:         toLedger,
		FromTime:         fromTime,
		ToTime:           toTime,
		Order:            q.Get("order"),
		OrderBy:          q.Get("order_by"),
		Cursor:           q.Get("cursor"),
	}

	// ?limit=N: explicit validation here so an explicit `?limit=0` (or
	// any value outside [1,maxLimit]) is a 400. BuildingEventFilter
	// treats args.Limit==0 as "use default" — we only invoke the
	// default when the param is absent, not when the caller explicitly
	// asked for an invalid value.
	if raw := q.Get("limit"); raw != "" {
		limit, lerr := strconv.Atoi(raw)
		if lerr != nil || limit < 1 || limit > maxLimit {
			return store.EventFilter{}, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit)
		}
		args.Limit = limit
	}

	f, err := queries.BuildEventFilter(args)
	if err != nil {
		return f, err

	}
	// ContractIDs is set outside EventFilterArgs because the shared queries
	// package (used by GraphQL) has no multi-ID concept yet; the store
	// turns a non-empty list into `contract_id = ANY($N)`.
	f.ContractIDs = contractIDs

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

	// order/order_by/topic/topic0..topic3/topic_contains/from_ledger/
	// to_ledger/from_time/to_time are applied by queries.BuildEventFilter
	// above (via the args struct populated at the top of this function).

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

	filter, err := filterFromQuery(r)
	if err != nil {
		writeFilterError(w, err)
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
	writeCacheHeaders(w, cacheNoStore, 0, "")

	writeJSON(w, status, errorResponse{Error: err.Error()})

}
