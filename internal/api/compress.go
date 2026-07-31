package api

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// Response compression.
//
// Event listings are the large responses here — a 200-event page is mostly
// repetitive JSON keys and hex strings, which gzip handles very well. Small
// responses are a different story: below roughly one network packet,
// compressing costs CPU on both ends and can make the body *larger* once the
// gzip header and trailer are counted. So compression is negotiated per
// request and applied per response, only once the body proves big enough.
//
// The decision is deferred rather than made upfront because a handler's
// response size isn't known until it writes. The writer buffers up to the
// threshold, then commits one way or the other exactly once.

// CompressMinSize is the body size, in bytes, below which a response is sent
// uncompressed. 1400 keeps the common small response — an error envelope, a
// /health probe, a single event — inside one Ethernet MTU, where compressing
// buys nothing and costs latency on both ends.
const CompressMinSize = 1400

// compressibleTypes are the media types worth compressing. Anything absent
// is passed through: already-compressed formats (images, gzip payloads) only
// grow, and unknown types aren't worth guessing at.
var compressibleTypes = map[string]bool{
	"application/json":       true,
	"application/javascript": true,
	"application/xml":        true,
	"image/svg+xml":          true,
	"text/css":               true,
	"text/csv":               true,
	"text/html":              true,
	"text/plain":             true,
	"text/xml":               true,
}

// Compress returns middleware that gzip- or deflate-encodes responses whose
// bodies reach minSize bytes, when the client advertises support via
// Accept-Encoding. minSize <= 0 uses CompressMinSize.
//
// Clients that don't advertise an encoding — and requests the middleware
// declines to touch, notably WebSocket upgrades — are served exactly as they
// were before, byte for byte.
func Compress(minSize int) func(http.Handler) http.Handler {
	if minSize <= 0 {
		minSize = CompressMinSize
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			encoding := negotiateEncoding(r.Header.Get("Accept-Encoding"))
			// A WebSocket upgrade never carries a compressible body: the
			// response is a 101 and the connection is then hijacked. Leave
			// the ResponseWriter untouched so the upgrade sees the real one.
			if encoding == "" || isWebSocketUpgrade(r) {
				next.ServeHTTP(w, r)
				return
			}

			// Vary is what stops a shared cache from serving a gzip body to
			// a client that can't decode it. writeCacheHeaders sets this for
			// cacheable responses already; set it here too so responses that
			// bypass that path (errors, subscription CRUD) are also safe.
			addVaryAcceptEncoding(w.Header())

			cw := &compressWriter{ResponseWriter: w, encoding: encoding, minSize: minSize}
			defer cw.Close()
			next.ServeHTTP(cw, r)
		})
	}
}

// negotiateEncoding picks an encoding from Accept-Encoding, preferring gzip.
// It honors q=0 ("explicitly not acceptable") and returns "" when the client
// advertises nothing usable.
func negotiateEncoding(header string) string {
	if header == "" {
		return ""
	}
	acceptable := map[string]bool{}
	for _, part := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "gzip" && name != "deflate" {
			continue
		}
		// q=0 means the client is refusing this encoding, not ranking it.
		if q, ok := strings.CutPrefix(strings.TrimSpace(params), "q="); ok {
			if v, err := strconv.ParseFloat(strings.TrimSpace(q), 64); err == nil && v == 0 {
				continue
			}
		}
		acceptable[name] = true
	}
	switch {
	case acceptable["gzip"]:
		return "gzip" // better ratio and universally supported
	case acceptable["deflate"]:
		return "deflate"
	default:
		return ""
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func addVaryAcceptEncoding(h http.Header) {
	if v := h.Get("Vary"); v == "" {
		h.Set("Vary", "Accept-Encoding")
	} else if !strings.Contains(v, "Accept-Encoding") {
		h.Set("Vary", v+", Accept-Encoding")
	}
}

// compressWriter buffers the start of a response so it can decide, once,
// whether the body is worth compressing.
type compressWriter struct {
	http.ResponseWriter

	encoding string
	minSize  int

	status  int
	wrote   bool // WriteHeader already sent to the client
	decided bool
	buf     []byte
	enc     io.WriteCloser // set only when compressing
}

func (w *compressWriter) WriteHeader(status int) {
	// Hold the status until the encoding is known: Content-Encoding and
	// Content-Length must be settled before headers go out.
	w.status = status
	if w.decided {
		w.flushHeader()
	}
}

func (w *compressWriter) flushHeader() {
	if w.wrote {
		return
	}
	w.wrote = true
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(w.status)
}

func (w *compressWriter) Write(b []byte) (int, error) {
	if w.decided {
		return w.write(b)
	}
	w.buf = append(w.buf, b...)
	if len(w.buf) >= w.minSize {
		if err := w.decide(true); err != nil {
			return 0, err
		}
	}
	// Report the full length: the bytes are accepted, just not forwarded yet.
	return len(b), nil
}

// decide commits to compressing or not, flushes the buffered prefix, and
// makes every later Write go straight through.
func (w *compressWriter) decide(big bool) error {
	w.decided = true
	h := w.Header()

	if big && w.shouldCompress() {
		h.Set("Content-Encoding", w.encoding)
		// The stored length describes the identity body, which is no longer
		// what's on the wire; the response becomes chunked instead.
		h.Del("Content-Length")
		// A strong ETag must identify one representation. The gzip body is a
		// different representation of the same resource, so weaken it rather
		// than let a cache serve the compressed bytes for the identity
		// validator. ifNoneMatch strips the W/ prefix, so conditional
		// requests still match.
		weakenETag(h)

		switch w.encoding {
		case "gzip":
			w.enc = gzip.NewWriter(w.ResponseWriter)
		case "deflate":
			// NewWriter only errors on an invalid level; the constant is valid.
			fw, _ := flate.NewWriter(w.ResponseWriter, flate.DefaultCompression)
			w.enc = fw
		}
	}

	w.flushHeader()
	if len(w.buf) > 0 {
		buf := w.buf
		w.buf = nil
		if _, err := w.write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (w *compressWriter) write(b []byte) (int, error) {
	if w.enc != nil {
		return w.enc.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

// shouldCompress reports whether this particular response is worth encoding,
// given the status and headers the handler produced.
func (w *compressWriter) shouldCompress() bool {
	// A handler that encoded its own body owns the encoding.
	if w.Header().Get("Content-Encoding") != "" {
		return false
	}
	// 204 and 304 carry no body; compressing is meaningless and a 304 with
	// Content-Encoding confuses caches.
	switch w.status {
	case http.StatusNoContent, http.StatusNotModified:
		return false
	}
	mediaType, _, _ := strings.Cut(w.Header().Get("Content-Type"), ";")
	return compressibleTypes[strings.ToLower(strings.TrimSpace(mediaType))]
}

// Close finishes the response: an undersized body is emitted uncompressed,
// and a compressed one gets its trailer.
func (w *compressWriter) Close() {
	if !w.decided {
		// Never reached the threshold — send it as-is.
		_ = w.decide(false)
	}
	if w.enc != nil {
		_ = w.enc.Close()
	}
	w.flushHeader() // handlers that wrote no body at all
}

// Flush supports streaming handlers. Buffering is the enemy of a stream, so
// an unflushed response that hasn't reached the threshold gives up on
// compression rather than holding bytes back waiting for more.
func (w *compressWriter) Flush() {
	if !w.decided {
		_ = w.decide(false)
	}
	if f, ok := w.enc.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack keeps connection-taking handlers working through the wrapper.
// WebSocket upgrades bypass this middleware entirely, but a wrapper that
// silently dropped Hijacker would break any future one, so pass it through.
func (w *compressWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
}

// weakenETag converts a strong ETag to a weak one, leaving an already-weak
// or absent validator alone.
func weakenETag(h http.Header) {
	etag := h.Get("ETag")
	if etag == "" || strings.HasPrefix(etag, "W/") {
		return
	}
	h.Set("ETag", "W/"+etag)
}
