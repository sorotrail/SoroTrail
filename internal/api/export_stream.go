package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Headers that report how an export ended. They are declared as HTTP trailers
// before the first row and filled in after the last, so a client that reads
// the trailers can tell a complete export from one that stopped early.
const (
	exportStatusTrailer = "X-Export-Status"
	exportRowsTrailer   = "X-Export-Rows"

	exportStatusComplete = "complete"
)

// exportOptions are the per-request choices of an export.
type exportOptions struct {
	// Terminator appends an in-band end marker after the last row: a final
	// NDJSON line {"export":{"complete":true,"rows":N}}, or a final CSV
	// record whose first cell is "#export-complete" and whose second is N.
	// It is for consumers that cannot see HTTP trailers.
	Terminator bool
}

// errExportIncomplete reports that an export was cut short after its response
// had been committed. The caller must abort the connection rather than end the
// response normally: a clean chunked ending would make a truncated file look
// complete.
var errExportIncomplete = errors.New("export stopped before the last row")

// abortIncompleteExport ends a response that cannot be completed. Panicking
// with http.ErrAbortHandler is the net/http contract for "close the
// connection without the terminating chunk", which the client sees as a
// transport error instead of a short but valid body. The Recoverer lets this
// one panic through untouched.
func abortIncompleteExport() { panic(http.ErrAbortHandler) }

// parseExportTerminator reads the opt-in terminator flag.
func parseExportTerminator(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false", "0":
		return false, nil
	case "true", "1":
		return true, nil
	default:
		return false, errors.New("invalid terminator (want true or false)")
	}
}

// streamExportOpts pages through filter via QueryEvents and writes each page
// to w in the requested format, flushing after every page so the client sees
// data as it is produced. Memory stays flat in the size of the result: one
// page of events is held at a time, and the loop is driven by the same
// cursor-paged query the listing uses, so the two cannot diverge on filters
// or tenant scope.
//
// How an export ends, from the consumer's side:
//
//   - Complete: the X-Export-Status trailer is "complete" and X-Export-Rows
//     is the row count; with the terminator option the last line or record
//     says the same in-band.
//   - Stopped early (a store error, a write error, the client's context or the
//     server timeout ending after rows were sent): the response has already
//     said 200, so the only honest signal left is a broken stream. streamExportOpts
//     returns errExportIncomplete and the handler aborts the connection.
//   - Failed before any byte was written: an ordinary 500 JSON error.
//
// Rows served are charged to the tenant's events-served counter.
func (s *Server) streamExportOpts(ctx context.Context, w http.ResponseWriter, filter store.EventFilter, format exportFormat, opts exportOptions) error {
	flusher, flushable := w.(http.Flusher)

	var (
		cw      *csv.Writer
		enc     *json.Encoder
		rows    int64
		started bool
	)

	// fail ends the export after an error. Before the first byte it can still
	// be an honest 500; after, the stream is broken on purpose.
	fail := func(msg string, err error) error {
		loggerFromContext(ctx).Error(msg, "error", err, "rows_written", rows)
		if !started {
			w.Header().Del("Content-Disposition")
			w.Header().Del("Trailer")
			writeError(w, http.StatusInternalServerError, errors.New("export failed"))
			return nil
		}
		return errExportIncomplete
	}

	if format == formatCSV {
		cw = csv.NewWriter(w)
		if err := cw.Write(exportCSVHeader); err != nil {
			return fail("writing csv header", err)
		}
		cw.Flush()
		started = true
		if flushable {
			flusher.Flush() // the header row lands before any other body
		}
	} else {
		enc = json.NewEncoder(w)
	}

	for {
		events, cursor, err := s.store.QueryEvents(ctx, filter)
		if errors.Is(err, store.ErrInvalidCursor) {
			return fail("export cursor", err)
		}
		if err != nil {
			return fail("export query", err)
		}
		for _, ev := range events {
			if cw != nil {
				if err := cw.Write(exportCSVRow(ev)); err != nil {
					return fail("export csv write", err)
				}
			} else {
				if err := enc.Encode(ev); err != nil {
					return fail("export ndjson write", err)
				}
			}
			started = true
			rows++
		}
		s.recordEventsServed(ctx, len(events))
		if cw != nil {
			cw.Flush()
			if err := cw.Error(); err != nil {
				return fail("export csv write", err)
			}
		}
		if flushable {
			flusher.Flush()
		}
		if cursor == "" {
			break
		}
		filter.Cursor = cursor
		// A client that disconnected, or the server's request timeout, ends
		// the walk. Rows were already sent, so this is an incomplete export,
		// not a finished one.
		if err := ctx.Err(); err != nil {
			return fail("export interrupted", err)
		}
	}

	if opts.Terminator {
		if cw != nil {
			_ = cw.Write([]string{"#export-complete", strconv.FormatInt(rows, 10)})
			cw.Flush()
		} else {
			_ = enc.Encode(map[string]any{"export": map[string]any{"complete": true, "rows": rows}})
		}
	}
	w.Header().Set(exportStatusTrailer, exportStatusComplete)
	w.Header().Set(exportRowsTrailer, strconv.FormatInt(rows, 10))
	return nil
}
