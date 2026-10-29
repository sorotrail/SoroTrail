package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// exportQueryBatchSize is the page size requested from the store while
// streaming an export. It sits between the default (50) and the maximum
// (200) so a large export issues a steady stream of moderately-sized
// queries rather than many small ones, while still respecting
// store.MaxQueryLimit.
const exportQueryBatchSize = 200

// exportFormat selects the wire format of an export response.
type exportFormat string

const (
	formatCSV    exportFormat = "csv"
	formatNDJSON exportFormat = "ndjson"
)

// parseExportFormat returns the recognized format or a 400-style error.
// Both export endpoints call this one parser, so "which formats exist"
// and "what an invalid value looks like" can never drift between them.
func parseExportFormat(raw string) (exportFormat, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "csv":
		return formatCSV, nil
	case "ndjson":
		return formatNDJSON, nil
	default:
		return "", fmt.Errorf("invalid format %q (want csv or ndjson)", raw)
	}
}

// exportCSVHeader is the column order every CSV export writes, in both
// endpoints. It mirrors the field names of the JSON API's Event
// representation (see store.Event) so a consumer switching from JSON to
// CSV, or between the two export endpoints, never has to relearn a shape.
var exportCSVHeader = []string{"id", "ledger", "type", "tx_hash", "topics", "value"}

// exportCSVRow renders one event as a CSV record in exportCSVHeader order.
// topics and value are written as JSON strings so spreadsheets and pandas
// can parse them as a single cell without splitting on commas inside the
// event payload.
//
// Every cell goes through csvSafe, which neutralises spreadsheet formulas.
func exportCSVRow(ev store.Event) []string {
	return []string{
		csvSafe(ev.ID),
		fmt.Sprintf("%d", ev.Ledger),
		csvSafe(ev.Type),
		csvSafe(ev.TxHash),
		csvSafe(string(ev.Topics)),
		csvSafe(string(ev.Value)),
	}
}

// csvSafe stops a cell from being executed as a formula when the file is
// opened in a spreadsheet. Excel, LibreOffice and Sheets treat a cell that
// starts with =, +, - or @ (and a leading tab or carriage return) as a
// formula, and these values come from contracts, so an event can carry one.
// A dangerous cell is prefixed with a single quote, the spreadsheet escape
// that makes it display as text.
//
// topics and value are JSON documents, so a leading - is a negative number,
// which a spreadsheet reads as a number and not a formula; those cells are
// left alone when they are valid JSON, which keeps the column parseable.
// Any other cell starting with - is escaped.
func csvSafe(cell string) string {
	if cell == "" {
		return cell
	}
	switch cell[0] {
	case 0x3d, 0x2b, 0x40, 0x09, 0x0d: // = + @ tab CR
		return "'" + cell
	case 0x2d: // -
		if json.Valid([]byte(cell)) {
			return cell
		}
		return "'" + cell
	}
	return cell
}

// setExportHeaders commits the Content-Type, Content-Disposition and
// cache headers shared by every export response. It must run before the
// first byte of the body so a store failure before streaming starts can
// still return a JSON error envelope instead of a half-written file.
func setExportHeaders(w http.ResponseWriter, format exportFormat, filenameStem string) {
	switch format {
	case formatCSV:
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "application/x-ndjson")
	}
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s.%s"`, filenameStem, format))
	// How the export ended is only known after the last row, so it travels as
	// an HTTP trailer; see streamExportOpts.
	w.Header().Set("Trailer", exportStatusTrailer+", "+exportRowsTrailer)
	// Streaming responses always flush before reaching the compression
	// threshold, but we still set it via writeCacheHeaders — a stale
	// browser cache keeping an old export would be a bug, so we mark
	// the response no-cache.
	writeCacheHeaders(w, cacheNoStore, 0, "")
}

// streamExport streams filter in the requested format with the default
// options. It is the one streaming path both export endpoints use; see
// streamExportOpts for the contract.
func (s *Server) streamExport(ctx context.Context, w http.ResponseWriter, filter store.EventFilter, format exportFormat) error {
	return s.streamExportOpts(ctx, w, filter, format, exportOptions{})
}

// handleContractExport streams a contract's events for a closed ledger
// range. Required query params: from_ledger, to_ledger. Optional: format
// (csv|ndjson, default csv).
//
// The store is queried in fixed-size pages so the handler never loads the
// full result set into memory: each page is flushed to the client as it
// arrives. The Content-Type and Transfer-Encoding headers are committed
// before the first write, so an early store failure returns a JSON error
// envelope (the request's preferred behavior since the headers haven't
// been flushed yet). Once streaming starts, late errors are logged but
// not surfaced — the connection's likely gone.
//
// Hard cap: Config.ExportMaxRange bounds how many ledgers one request
// may span. Operators who need bigger analytical exports raise the cap;
// default 17280 ≈ 24h means a stolen or buggy param can't accidentally
// drain the database.
func (s *Server) handleContractExport(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID))
		return
	}
	// Refused before any header is committed, matching
	// GET /contracts/{id}/events. This governs the status code only — the
	// store ANDs the scope into every page regardless, so removing this
	// check downgrades a 403 to an empty file rather than opening a leak.
	if !scopeFrom(r.Context()).Allows(contractID) {
		writeForbiddenContract(w, contractID)
		return
	}

	format, err := parseExportFormat(r.URL.Query().Get("format"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// parseLedgerParam is now in internal/api/queries. We reconstruct
	// the original error string (which used the param-name prefix) so
	// REST clients see the same message they have always seen.
	fromLedger, err := queries.ParseLedgerParam(r.URL.Query().Get("from_ledger"))
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("from_ledger %s", err.Error()))
		return
	}
	toLedger, err := queries.ParseLedgerParam(r.URL.Query().Get("to_ledger"))
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("to_ledger %s", err.Error()))
		return
	}
	if fromLedger <= 0 || toLedger <= 0 {
		writeError(w, http.StatusBadRequest,
			errors.New("from_ledger and to_ledger are required for export"))
		return
	}
	if fromLedger > toLedger {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("from_ledger %d is after to_ledger %d", fromLedger, toLedger))
		return
	}
	terminator, err := parseExportTerminator(r.URL.Query().Get("terminator"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	span := toLedger - fromLedger + 1
	if s.exportMaxRange > 0 && span > s.exportMaxRange {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("ledger range [%d,%d] spans %d ledgers, exceeding EXPORT_MAX_RANGE=%d", fromLedger, toLedger, span, s.exportMaxRange))
		return
	}

	ctx := r.Context()
	setExportHeaders(w, format, fmt.Sprintf("%s-ledgers-%d-%d", contractID, fromLedger, toLedger))

	filter := s.exportFilter(ctx, contractID, fromLedger, toLedger)
	if err := s.streamExportOpts(ctx, w, filter, format, exportOptions{Terminator: terminator}); err != nil {
		abortIncompleteExport()
	}
}

// exportFilter is the filter every export page request uses; the
// handler sets FromLedger/ToLedger once and lets QueryEvents walk the
// result range via cursor pagination.
//
// The scope comes from the request context and is carried on every page,
// not just the first: an export is a long-running cursor walk, and a
// filter that lost its scope midway would widen the walk rather than end
// it. In single-tenant mode the injected principal is the wildcard, so
// this is the pre-#48 behavior exactly.
func (s *Server) exportFilter(ctx context.Context, contractID string, fromLedger, toLedger int64) store.EventFilter {
	return store.EventFilter{
		ContractID: contractID,
		FromLedger: fromLedger,
		ToLedger:   toLedger,
		Order:      "asc",
		OrderBy:    store.OrderByLedger,
		Limit:      exportQueryBatchSize,
		Scope:      scopeFrom(ctx),
	}
}

// handleEventsCSV streams all matching events, using the same filter
// params as GET /events, in the format requested by ?format= (csv|ndjson,
// default csv — see parseExportFormat). Despite the historical ".csv"
// route name, this endpoint accepts the same format values as
// /contracts/{id}/export so a client doesn't have to rediscover per-route
// behavior when it switches which events it's exporting.
func (s *Server) handleEventsCSV(w http.ResponseWriter, r *http.Request) {
	format, err := parseExportFormat(r.URL.Query().Get("format"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	filter, err := filterFromQuery(r)
	if err != nil {
		writeFilterError(w, err)
		return
	}
	filter.Cursor = ""
	filter.Limit = exportQueryBatchSize
	terminator, err := parseExportTerminator(r.URL.Query().Get("terminator"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	setExportHeaders(w, format, "events")
	if err := s.streamExportOpts(r.Context(), w, filter, format, exportOptions{Terminator: terminator}); err != nil {
		abortIncompleteExport()
	}
}
