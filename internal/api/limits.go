package api

import (
	"compress/flate"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

const (
	// DefaultHTTPRequestBodyLimit is the default maximum request body size
	// (1 MiB), bounding both raw wire bytes and decompressed request bytes.
	DefaultHTTPRequestBodyLimit int64 = 1 << 20

	// DefaultSpecOverrideBodyLimit is the per-endpoint request body size limit
	// (2 MiB) for PUT /contracts/{id}/spec, which accepts full contract event
	// specification documents that legitimately exceed single-object writes.
	DefaultSpecOverrideBodyLimit int64 = 2 << 20

	// DefaultHTTPResponseBodyLimit is the default maximum size (16 MiB) for
	// non-streaming JSON responses. Streaming endpoints (/events.csv,
	// /contracts/{id}/export, /events?stream=true, /events/ws) flush
	// incrementally and are not subject to this cap.
	DefaultHTTPResponseBodyLimit int64 = 16 << 20

	// MaxResponseBytesHeader is an optional request header a client may send
	// to request a tighter upper bound (in bytes) on a non-streaming JSON
	// response body.
	MaxResponseBytesHeader = "X-Max-Response-Bytes"
)

// SetHTTPResponseBodyLimit sets the maximum non-streaming JSON response body
// size, in bytes. A value <= 0 disables the server-side response size cap.
func (s *Server) SetHTTPResponseBodyLimit(n int64) {
	s.httpResponseBodyLimit = n
}

// SetEndpointRequestBodyLimit overrides the request body size limit (in bytes)
// for a specific route pattern such as "PUT /contracts/{id}/spec" or
// "/contracts/{id}/spec". Passing n <= 0 removes the override for that route.
func (s *Server) SetEndpointRequestBodyLimit(route string, n int64) {
	key := normalizeEndpointPattern(route)
	if key == "" {
		return
	}
	if s.endpointRequestBodyLimits == nil {
		s.endpointRequestBodyLimits = make(map[string]int64)
	}
	if n <= 0 {
		delete(s.endpointRequestBodyLimits, key)
		return
	}
	s.endpointRequestBodyLimits[key] = n
}

func normalizeEndpointPattern(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return ""
	}
	if method, path, ok := strings.Cut(route, " "); ok {
		return strings.ToUpper(strings.TrimSpace(method)) + " " + strings.TrimSpace(path)
	}
	return route
}

// effectiveRequestBodyLimit returns the request body size limit in bytes that
// applies to r: a matching per-endpoint override when one is configured, or
// the server's global httpRequestBodyLimit otherwise.
func (s *Server) effectiveRequestBodyLimit(r *http.Request) int64 {
	if s == nil {
		return 0
	}
	if r != nil && len(s.endpointRequestBodyLimits) > 0 {
		pattern := s.routePattern(r)
		if limit, ok := s.endpointRequestBodyLimits[r.Method+" "+pattern]; ok {
			return limit
		}
		if limit, ok := s.endpointRequestBodyLimits[pattern]; ok {
			return limit
		}
		for key, limit := range s.endpointRequestBodyLimits {
			wantMethod, wantPath, hasMethod := strings.Cut(key, " ")
			if hasMethod {
				if !strings.EqualFold(wantMethod, r.Method) {
					continue
				}
			} else {
				wantPath = key
			}
			if matchRoutePattern(wantPath, r.URL.Path) {
				return limit
			}
		}
	}
	return s.httpRequestBodyLimit
}

// matchRoutePattern matches a chi-style route pattern (with {param} segments)
// against a concrete URL path so per-endpoint body limits work even in
// middleware that executes before route dispatch completes.
func matchRoutePattern(pattern, path string) bool {
	pSegs := strings.Split(strings.Trim(pattern, "/"), "/")
	uSegs := strings.Split(strings.Trim(path, "/"), "/")
	if len(pSegs) != len(uSegs) {
		return false
	}
	for i := range pSegs {
		ps := pSegs[i]
		if strings.HasPrefix(ps, "{") && strings.HasSuffix(ps, "}") {
			if uSegs[i] == "" {
				return false
			}
			continue
		}
		if ps != uSegs[i] {
			return false
		}
	}
	return true
}

// effectiveResponseBodyLimit returns the non-streaming JSON response cap in
// bytes for r, combining the server's configured httpResponseBodyLimit with
// any tighter positive bound requested via MaxResponseBytesHeader.
func (s *Server) effectiveResponseBodyLimit(r *http.Request) int64 {
	var limit int64
	if s != nil && s.httpResponseBodyLimit > 0 {
		limit = s.httpResponseBodyLimit
	}
	if r != nil {
		if raw := strings.TrimSpace(r.Header.Get(MaxResponseBytesHeader)); raw != "" {
			if clientLimit, err := strconv.ParseInt(raw, 10, 64); err == nil && clientLimit > 0 {
				if limit <= 0 || clientLimit < limit {
					limit = clientLimit
				}
			}
		}
	}
	return limit
}

type responseLimitWriter interface {
	MaxResponseBytes() int64
}

// boundedRequestBody wraps an http.Request.Body so that:
//  1. Wire reads are capped by http.MaxBytesReader when a response writer and
//     a positive limit are available.
//  2. Compressed request bodies (Content-Encoding: gzip or deflate) are
//     transparently decompressed and bounded by their DECOMPRESSED size, not
//     only their compressed wire size, preventing compression bombs.
//  3. Rejections are counted once in sorotrail_http_limit_rejections_total
//     with reason="request_body" or reason="request_decompressed".
type boundedRequestBody struct {
	raw          io.ReadCloser
	wire         io.ReadCloser
	encoding     string
	limit        int64
	decompressor io.ReadCloser
	initErr      error
	initialized  bool
	wireRead     int64
	decompRead   int64
	counted      bool
}

func wrapBoundedRequestBody(w http.ResponseWriter, r *http.Request, limit int64) *boundedRequestBody {
	if existing, ok := r.Body.(*boundedRequestBody); ok {
		if limit > 0 && existing.limit != limit {
			existing.limit = limit
			if w != nil {
				existing.wire = http.MaxBytesReader(w, existing.raw, limit)
			}
		}
		return existing
	}
	raw := r.Body
	wire := raw
	if limit > 0 && w != nil && raw != nil {
		wire = http.MaxBytesReader(w, raw, limit)
	}
	enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	br := &boundedRequestBody{
		raw:      raw,
		wire:     wire,
		encoding: enc,
		limit:    limit,
	}
	r.Body = br
	return br
}

func (b *boundedRequestBody) initDecompressor() error {
	if b.initialized {
		return b.initErr
	}
	b.initialized = true

	// Count raw wire bytes passing through b.wire so we can distinguish a wire
	// limit breach from a decompression expansion breach.
	wireCounter := &countingReadCloser{
		rc:    b.wire,
		count: &b.wireRead,
	}

	switch b.encoding {
	case "", "identity":
		b.decompressor = wireCounter
	case "gzip":
		gz, err := gzip.NewReader(wireCounter)
		if err != nil {
			if b.isWireLimitError(err) {
				b.initErr = b.recordWireLimitError()
			} else {
				b.initErr = fmt.Errorf("invalid gzip request body: %w", err)
			}
			return b.initErr
		}
		b.decompressor = gz
	case "deflate":
		b.decompressor = flate.NewReader(wireCounter)
	default:
		b.initErr = fmt.Errorf("unsupported Content-Encoding %q (want gzip, deflate, or identity)", b.encoding)
		return b.initErr
	}
	return nil
}

func (b *boundedRequestBody) isCompressed() bool {
	return b.encoding == "gzip" || b.encoding == "deflate"
}

func (b *boundedRequestBody) isWireLimitError(err error) bool {
	if err == nil {
		return false
	}
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr) ||
		errors.Is(err, errBodyTooLarge) ||
		strings.Contains(err.Error(), "request body too large")
}

type bodyLimitError struct {
	limit      int64
	compressed bool
}

func (e *bodyLimitError) Error() string {
	if e.compressed {
		return fmt.Sprintf("decompressed request body exceeds %d bytes: %s", e.limit, errBodyTooLarge.Error())
	}
	return fmt.Sprintf("request body exceeds %d bytes: %s", e.limit, errBodyTooLarge.Error())
}

func (e *bodyLimitError) Unwrap() []error {
	return []error{errBodyTooLarge, &http.MaxBytesError{Limit: e.limit}}
}

func (b *boundedRequestBody) recordWireLimitError() error {
	if !b.counted {
		b.counted = true
		metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestBody).Inc()
	}
	return &bodyLimitError{limit: b.limit, compressed: false}
}

func (b *boundedRequestBody) recordDecompressedLimitError() error {
	if !b.counted {
		b.counted = true
		metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestDecompressed).Inc()
	}
	return &bodyLimitError{limit: b.limit, compressed: true}
}

func (b *boundedRequestBody) Read(p []byte) (int, error) {
	if err := b.initDecompressor(); err != nil {
		return 0, err
	}
	if b.limit > 0 {
		remaining := (b.limit + 1) - b.decompRead
		if remaining <= 0 {
			if b.isCompressed() {
				return 0, b.recordDecompressedLimitError()
			}
			return 0, b.recordWireLimitError()
		}
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
	}

	n, err := b.decompressor.Read(p)
	b.decompRead += int64(n)

	if b.limit > 0 && b.decompRead > b.limit {
		if b.isCompressed() {
			return n, b.recordDecompressedLimitError()
		}
		return n, b.recordWireLimitError()
	}
	if err != nil && err != io.EOF {
		if b.isWireLimitError(err) {
			return n, b.recordWireLimitError()
		}
	}
	return n, err
}

func (b *boundedRequestBody) Close() error {
	var firstErr error
	if b.decompressor != nil && b.decompressor != b.wire {
		if err := b.decompressor.Close(); err != nil {
			firstErr = err
		}
	}
	if b.wire != nil {
		if err := b.wire.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type countingReadCloser struct {
	rc    io.ReadCloser
	count *int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	*c.count += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}
