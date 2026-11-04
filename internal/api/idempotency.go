package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	// IdempotencyKeyHeader is the optional request header accepted on all
	// write endpoints (POST, PUT, PATCH, DELETE).
	IdempotencyKeyHeader = "Idempotency-Key"

	// IdempotencyReplayedHeader is set to "true" on responses served from a
	// previously recorded idempotency key rather than re-executing the handler.
	IdempotencyReplayedHeader = "Idempotency-Replayed"

	// MaxIdempotencyKeyLength caps the length of a client-supplied
	// Idempotency-Key header so keys fit comfortably inside a B-tree index.
	MaxIdempotencyKeyLength = 255

	// defaultIdempotencySweepInterval rate-limits opportunistic expired-key
	// cleanup on the write path so retention sweeps stay cheap.
	defaultIdempotencySweepInterval = time.Minute
)

type inFlightIdempotencySlot struct {
	done        chan struct{}
	requestHash string
	method      string
	path        string
	rec         store.IdempotencyRecord
	err         error
}

type idempotencySlotKey struct {
	tenantID string
	key      string
}

type idempotencyCoordinator struct {
	mu          sync.Mutex
	fallback    *store.MemoryIdempotencyStore
	override    store.IdempotencyStore
	ttl         time.Duration
	nowFn       func() time.Time
	inFlight    map[idempotencySlotKey]*inFlightIdempotencySlot
	lastSweepAt time.Time
}

func newIdempotencyCoordinator() *idempotencyCoordinator {
	return &idempotencyCoordinator{
		fallback: store.NewMemoryIdempotencyStore(),
		ttl:      store.DefaultIdempotencyTTL,
		nowFn:    time.Now,
		inFlight: make(map[idempotencySlotKey]*inFlightIdempotencySlot),
	}
}

func (s *Server) ensureIdempotency() *idempotencyCoordinator {
	s.idempotencyOnce.Do(func() {
		if s.idempotency == nil {
			s.idempotency = newIdempotencyCoordinator()
		}
	})
	return s.idempotency
}

// SetIdempotencyStore overrides the backing IdempotencyStore used by write
// endpoints. By default, Server uses s.store when it implements
// store.IdempotencyStore (Postgres and SQLite) and falls back to an in-memory
// store for unit-test stubs.
func (s *Server) SetIdempotencyStore(st store.IdempotencyStore) {
	c := s.ensureIdempotency()
	c.mu.Lock()
	c.override = st
	c.mu.Unlock()
}

// SetIdempotencyTTL configures the retention window for idempotency keys.
// Zero or negative values restore store.DefaultIdempotencyTTL (24h).
func (s *Server) SetIdempotencyTTL(ttl time.Duration) {
	c := s.ensureIdempotency()
	c.mu.Lock()
	if ttl <= 0 {
		c.ttl = store.DefaultIdempotencyTTL
	} else {
		c.ttl = ttl
	}
	c.mu.Unlock()
}

// SetIdempotencyClock overrides the clock used for idempotency key creation
// and expiration checks (used by deterministic expiry tests).
func (s *Server) SetIdempotencyClock(nowFn func() time.Time) {
	c := s.ensureIdempotency()
	c.mu.Lock()
	if nowFn == nil {
		c.nowFn = time.Now
	} else {
		c.nowFn = nowFn
	}
	c.mu.Unlock()
}

// SweepExpiredIdempotencyKeys deletes up to limit expired idempotency keys
// from the active store.
func (s *Server) SweepExpiredIdempotencyKeys(ctx context.Context, limit int) (int64, error) {
	c := s.ensureIdempotency()
	backend := s.resolveIdempotencyStore(c)
	c.mu.Lock()
	now := c.nowFn()
	c.mu.Unlock()
	return backend.DeleteExpiredIdempotencyKeys(ctx, now, limit)
}

func (s *Server) resolveIdempotencyStore(c *idempotencyCoordinator) store.IdempotencyStore {
	c.mu.Lock()
	override := c.override
	fallback := c.fallback
	c.mu.Unlock()
	if override != nil {
		return override
	}
	if st, ok := s.store.(store.IdempotencyStore); ok {
		return st
	}
	return fallback
}

// idempotencyTenantScope derives the tenant isolation key for an authenticated
// write request. In multi-tenant mode it uses the authenticated Tenant.ID; in
// single-tenant mode it scopes by the presented credential digest (when
// present) so distinct API keys cannot collide, falling back to "default".
func (s *Server) idempotencyTenantScope(r *http.Request) string {
	if p, ok := PrincipalFrom(r.Context()); ok {
		if !p.Untenanted && p.Tenant.ID > 0 {
			return fmt.Sprintf("tenant:%d", p.Tenant.ID)
		}
		if p.Tenant.Name != "" {
			return "tenant:" + p.Tenant.Name
		}
	}
	if cred := credentialFromRequest(r); cred != "" {
		return "key:" + store.HashIdempotencyRequest("CRED", "", []byte(cred))[:24]
	}
	return "default"
}

func (s *Server) idempotencyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawKey := strings.TrimSpace(r.Header.Get(IdempotencyKeyHeader))
		if rawKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		if len(rawKey) > MaxIdempotencyKeyLength {
			writeError(w, http.StatusBadRequest,
				fmt.Errorf("Idempotency-Key must be at most %d characters", MaxIdempotencyKeyLength))
			return
		}

		bodyBytes, err := readIdempotencyBody(r, s.httpRequestBodyLimit)
		if err != nil {
			if errors.Is(err, errBodyTooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, err)
				return
			}
			writeError(w, http.StatusBadRequest, err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		c := s.ensureIdempotency()
		backend := s.resolveIdempotencyStore(c)

		c.mu.Lock()
		now := c.nowFn().UTC()
		ttl := c.ttl
		shouldSweep := c.lastSweepAt.IsZero() || now.Sub(c.lastSweepAt) >= defaultIdempotencySweepInterval
		if shouldSweep {
			c.lastSweepAt = now
		}
		c.mu.Unlock()

		if shouldSweep {
			_, _ = backend.DeleteExpiredIdempotencyKeys(r.Context(), now, store.DefaultIdempotencySweepBatchSize)
		}

		tenantScope := s.idempotencyTenantScope(r)
		targetPath := r.URL.RequestURI()
		if targetPath == "" {
			targetPath = r.URL.Path
		}
		reqHash := store.HashIdempotencyRequest(r.Method, targetPath, bodyBytes)

		slotKey := idempotencySlotKey{tenantID: tenantScope, key: rawKey}

		// Coalesce concurrent in-process requests with the same (tenantScope, rawKey)
		// so only one goroutine hits the handler and store transaction while
		// siblings wait for its outcome.
		for {
			c.mu.Lock()
			inFlight, exists := c.inFlight[slotKey]
			if !exists {
				inFlight = &inFlightIdempotencySlot{
					done:        make(chan struct{}),
					requestHash: reqHash,
					method:      r.Method,
					path:        targetPath,
				}
				c.inFlight[slotKey] = inFlight
				c.mu.Unlock()
				s.executeWithIdempotencySlot(w, r, next, backend, c, slotKey, inFlight, bodyBytes, now, ttl)
				return
			}
			c.mu.Unlock()

			// Another goroutine on this instance is already executing this key.
			// Wait for it to finish (or for our request context to cancel).
			select {
			case <-inFlight.done:
			case <-r.Context().Done():
				writeError(w, http.StatusRequestTimeout, r.Context().Err())
				return
			}

			// If the in-flight request had a different payload/method/path,
			// reject immediately with 409 Conflict.
			if inFlight.requestHash != reqHash || inFlight.method != r.Method || inFlight.path != targetPath {
				writeError(w, http.StatusConflict, store.ErrIdempotencyKeyMismatch)
				return
			}
			if inFlight.err == nil && inFlight.rec.StatusCode > 0 {
				writeReplayedIdempotencyResponse(w, inFlight.rec)
				return
			}
			// If the previous attempt aborted with a 5xx/error and deleted its
			// reservation, loop once to claim and execute.
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
	})
}

func (s *Server) executeWithIdempotencySlot(
	w http.ResponseWriter,
	r *http.Request,
	next http.Handler,
	backend store.IdempotencyStore,
	c *idempotencyCoordinator,
	slotKey idempotencySlotKey,
	slot *inFlightIdempotencySlot,
	bodyBytes []byte,
	now time.Time,
	ttl time.Duration,
) {
	defer func() {
		c.mu.Lock()
		delete(c.inFlight, slotKey)
		close(slot.done)
		c.mu.Unlock()
	}()

	candidate := store.IdempotencyRecord{
		TenantID:    slotKey.tenantID,
		Key:         slotKey.key,
		RequestHash: slot.requestHash,
		Method:      slot.method,
		Path:        slot.path,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
	}

	outcome, existing, err := backend.ClaimIdempotencyKey(r.Context(), candidate, now)
	if errors.Is(err, store.ErrIdempotencyKeyMismatch) {
		slot.err = err
		writeError(w, http.StatusConflict, store.ErrIdempotencyKeyMismatch)
		return
	}
	if errors.Is(err, store.ErrIdempotencyInFlight) {
		// Another replica is currently executing the same key; poll briefly
		// until it completes or the request context expires.
		completed, waitErr := waitForRemoteInFlightKey(r.Context(), backend, slotKey.tenantID, slotKey.key, now)
		if waitErr != nil {
			slot.err = waitErr
			writeError(w, http.StatusConflict, errors.New("concurrent request with same Idempotency-Key is still in progress"))
			return
		}
		if completed.RequestHash != slot.requestHash || completed.Method != slot.method || completed.Path != slot.path {
			slot.err = store.ErrIdempotencyKeyMismatch
			writeError(w, http.StatusConflict, store.ErrIdempotencyKeyMismatch)
			return
		}
		slot.rec = completed
		writeReplayedIdempotencyResponse(w, completed)
		return
	}
	if err != nil {
		slot.err = err
		loggerFromContext(r.Context()).Error("claiming idempotency key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("idempotency check failed"))
		return
	}

	if outcome == store.IdempotencyReplayed {
		slot.rec = existing
		writeReplayedIdempotencyResponse(w, existing)
		return
	}

	// Outcome == IdempotencyStarted: capture the handler's response.
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	rec := newIdempotencyResponseRecorder(w)
	panicked := true
	defer func() {
		if panicked {
			_ = backend.DeleteIdempotencyKey(context.Background(), slotKey.tenantID, slotKey.key)
		}
	}()
	next.ServeHTTP(rec, r)
	panicked = false

	statusCode := rec.statusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	// Transient 5xx server failures release the reservation so the client's
	// retry can re-attempt the operation once the dependency recovers.
	// Deterministic 2xx and 4xx responses are persisted and replayed.
	if statusCode >= http.StatusInternalServerError {
		slot.err = errors.New("server error")
		_ = backend.DeleteIdempotencyKey(r.Context(), slotKey.tenantID, slotKey.key)
		rec.flushToUnderlying()
		return
	}

	completed := candidate
	completed.StatusCode = statusCode
	completed.ContentType = rec.Header().Get("Content-Type")
	completed.ResponseBody = rec.body.Bytes()

	if err := backend.CompleteIdempotencyKey(r.Context(), completed); err != nil {
		loggerFromContext(r.Context()).Error("completing idempotency key", "error", err)
	}
	slot.rec = completed
	rec.flushToUnderlying()
}

func waitForRemoteInFlightKey(
	ctx context.Context,
	backend store.IdempotencyStore,
	tenantID, key string,
	now time.Time,
) (store.IdempotencyRecord, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return store.IdempotencyRecord{}, ctx.Err()
		case <-deadline.C:
			return store.IdempotencyRecord{}, store.ErrIdempotencyInFlight
		case <-ticker.C:
			rec, err := backend.GetIdempotencyKey(ctx, tenantID, key, now)
			if err != nil {
				return store.IdempotencyRecord{}, err
			}
			if rec.StatusCode > 0 {
				return rec, nil
			}
		}
	}
}

func writeReplayedIdempotencyResponse(w http.ResponseWriter, rec store.IdempotencyRecord) {
	if rec.ContentType != "" {
		w.Header().Set("Content-Type", rec.ContentType)
	}
	w.Header().Set(IdempotencyReplayedHeader, "true")
	w.WriteHeader(rec.StatusCode)
	if len(rec.ResponseBody) > 0 {
		_, _ = w.Write(rec.ResponseBody)
	}
}

func readIdempotencyBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	var body []byte
	var err error
	if limit > 0 {
		body, err = io.ReadAll(io.LimitReader(r.Body, limit+1))
	} else {
		body, err = io.ReadAll(r.Body)
	}
	if err != nil {
		tooLarge := errors.Is(err, errBodyTooLarge) || strings.Contains(err.Error(), "request body too large")
		if tooLarge {
			return nil, fmt.Errorf("request body exceeds %d bytes: %w", limit, errBodyTooLarge)
		}
		return nil, fmt.Errorf("reading request body: %w", err)
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, fmt.Errorf("request body exceeds %d bytes: %w", limit, errBodyTooLarge)
	}
	return body, nil
}

type idempotencyResponseRecorder struct {
	underlying  http.ResponseWriter
	headers     http.Header
	body        bytes.Buffer
	statusCode  int
	wroteHeader bool
}

func newIdempotencyResponseRecorder(w http.ResponseWriter) *idempotencyResponseRecorder {
	return &idempotencyResponseRecorder{
		underlying: w,
		headers:    make(http.Header),
	}
}

func (r *idempotencyResponseRecorder) Header() http.Header {
	return r.headers
}

func (r *idempotencyResponseRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.statusCode = code
	r.wroteHeader = true
}

func (r *idempotencyResponseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(p)
}

func (r *idempotencyResponseRecorder) Pretty() bool {
	if pw, ok := r.underlying.(interface{ Pretty() bool }); ok {
		return pw.Pretty()
	}
	return false
}

func (r *idempotencyResponseRecorder) flushToUnderlying() {
	for k, vals := range r.headers {
		r.underlying.Header()[k] = append([]string(nil), vals...)
	}
	statusCode := r.statusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	r.underlying.WriteHeader(statusCode)
	if r.body.Len() > 0 {
		_, _ = r.underlying.Write(r.body.Bytes())
	}
}
