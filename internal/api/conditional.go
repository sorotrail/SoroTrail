package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
)

// conditionalMaxBody bounds how much of a response conditionalGET buffers to
// compute its validator. A larger body is streamed through unchanged, without
// an ETag: a validator that needs the whole body in memory is not worth an
// unbounded buffer.
const conditionalMaxBody = 8 << 20

// conditionalGET gives a read endpoint a strong entity tag and answers
// If-None-Match with 304 Not Modified.
//
// Polling is the dominant access pattern: a client following a contract
// re-requests the same listing and, when nothing was emitted, gets a
// byte-identical page. With a validator the repeat costs one header round
// trip instead of the whole body, and the client skips re-parsing it.
//
// The validator is a hash of the exact response body, so it is correct by
// construction: it changes when, and only when, the bytes a client would
// receive change. That includes tenant scope, filters, cursors, ordering and
// ?pretty, all of which already shape the body, so no separate scope
// component is needed to keep two tenants' validators apart. What it does not
// do is skip the query: the handler still runs. A cheaper validator needs a
// store-side change counter, which the store does not have.
//
// Only a 200 response without an ETag of its own is touched. Handlers that
// already compute a validator (immutable windows, single events) keep it, and
// errors, redirects and streams pass through. It must sit inside the
// compression middleware so the hash is of the identity body; Compress then
// weakens the tag for the gzip representation.
func conditionalGET(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		cw := &conditionalWriter{ResponseWriter: w}
		next.ServeHTTP(cw, r)
		cw.finish(r)
	})
}

type conditionalWriter struct {
	http.ResponseWriter

	status      int
	buf         bytes.Buffer
	passthrough bool
	committed   bool
}

// Pretty forwards the ?pretty flag so writeJSON still sees it through this
// wrapper.
func (w *conditionalWriter) Pretty() bool {
	pw, ok := w.ResponseWriter.(prettyWriter)
	return ok && pw.Pretty()
}

func (w *conditionalWriter) WriteHeader(code int) {
	if w.passthrough {
		if !w.committed {
			w.committed = true
			w.ResponseWriter.WriteHeader(code)
		}
		return
	}
	if w.status != 0 {
		return
	}
	w.status = code
	// Only a plain 200 that the handler has not already tagged is ours to
	// validate; everything else is forwarded as the handler wrote it.
	if code != http.StatusOK || w.Header().Get("ETag") != "" {
		w.startPassthrough()
	}
}

func (w *conditionalWriter) Write(b []byte) (int, error) {
	if w.passthrough {
		w.commit()
		return w.ResponseWriter.Write(b)
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
		if w.passthrough {
			w.commit()
			return w.ResponseWriter.Write(b)
		}
	}
	if w.buf.Len()+len(b) > conditionalMaxBody {
		w.startPassthrough()
		w.commit()
		return w.ResponseWriter.Write(b)
	}
	return w.buf.Write(b)
}

// Flush means the handler is streaming; it cannot be validated, so whatever
// is buffered goes out and the rest follows unbuffered.
func (w *conditionalWriter) Flush() {
	if !w.passthrough {
		w.startPassthrough()
	}
	w.commit()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// startPassthrough stops buffering. Header and buffered bytes are written
// lazily by commit so a handler that sets headers after WriteHeader (it
// should not) is not made worse.
func (w *conditionalWriter) startPassthrough() { w.passthrough = true }

// commit writes the status line and any bytes buffered so far, once.
func (w *conditionalWriter) commit() {
	if w.committed {
		return
	}
	w.committed = true
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(status)
	if w.buf.Len() > 0 {
		_, _ = w.ResponseWriter.Write(w.buf.Bytes())
		w.buf.Reset()
	}
}

// finish completes a buffered response: tag it and answer 304 when the
// client's validator still matches.
func (w *conditionalWriter) finish(r *http.Request) {
	if w.passthrough {
		w.commit()
		return
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	body := w.buf.Bytes()

	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:16]) + `"`
	h := w.Header()
	h.Set("ETag", etag)

	if ifNoneMatch(r, etag) {
		// A 304 carries the validator and the cache headers but no
		// representation metadata.
		h.Del("Content-Length")
		h.Del("Content-Type")
		h.Del("Content-Encoding")
		w.ResponseWriter.WriteHeader(http.StatusNotModified)
		return
	}
	w.ResponseWriter.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.ResponseWriter.Write(body)
	}
}
