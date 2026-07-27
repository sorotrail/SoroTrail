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
	"github.com/khaylebfortune/sorotrail/internal/version"
)

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

type healthResponse struct {
	Status string            `json:"status"` // ok | degraded
	Checks map[string]string `json:"checks"`
}

type versionResponse struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
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
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, resp)
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	resp := versionResponse{
		Version: version.GetVersion(),
		Commit:  version.GetCommit(),
		Date:    version.GetDate(),
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, resp)
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

// serveEvents is the shared body for /events and /contracts/{id}/events.
// It runs the cacheability decision (frontier vs. upper bound) BEFORE the
// SQL query, so an immutable page with a matching If-None-Match never
// touches the events table at all — only the cheap ingestion_state row
// used to read the frontier.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter) {
	policy, etag, err := s.listCachePolicy(r.Context(), filter)
	if err != nil {
		// "When in doubt, don't cache" is the explicit guidance: any
		// failure to read the frontier falls back to no-cache rather
		// than guessing the page is safe.
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
			s.log.Error("checking event existence", "id", id, "error", err)
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
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, stats)
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

	// order controls sort direction for paginated results.
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

	filter, err := filterFromQuery(r)
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
		s.log.Error("websocket accept", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

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
	// Every error response is marked no-store so neither CDNs nor
	// browsers can pool a 4xx/5xx behind a success response's
	// validator. The prime motivator is the 404-on-eviction path in
	// handleGetEvent: a stale cache otherwise keeps returning "not
	// found" for an event that briefly aged out but never came back.
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
