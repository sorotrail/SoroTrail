package api

import (
	"context"
	"encoding/csv"
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
	span := toLedger - fromLedger + 1
	if s.exportMaxRange > 0 && span > s.exportMaxRange {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("ledger range [%d,%d] spans %d ledgers, exceeding EXPORT_MAX_RANGE=%d", fromLedger, toLedger, span, s.exportMaxRange))
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s-ledgers-%d-%d.%s"`,
			contractID, fromLedger, toLedger, format))
	// Streaming responses always flush before reaching the compression
	// threshold, but we still set it via writeCacheHeaders — a stale
	// browser cache keeping an old export would be a bug, so we mark
	// the response no-cache.
	writeCacheHeaders(w, cacheNoStore, 0, "")

	switch format {
	case formatCSV:
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		s.streamExportCSV(ctx, w, contractID, fromLedger, toLedger)
	default:
		w.Header().Set("Content-Type", "application/x-ndjson")
		s.streamExportNDJSON(ctx, w, contractID, fromLedger, toLedger)
	}
}

// streamExportCSV writes one CSV record per event with columns
// id, ledger, type, tx_hash, topics (JSON string), value (JSON string).
// topics and value are written as JSON strings so spreadsheets and pandas
// can parse them as a single cell without splitting on commas inside the
// event payload.
func (s *Server) streamExportCSV(ctx context.Context, w http.ResponseWriter, contractID string, fromLedger, toLedger int64) {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"id", "ledger", "type", "tx_hash", "topics", "value"}); err != nil {
		loggerFromContext(ctx).Error("writing csv header", "error", err)
		// Headers were already sent; nothing else we can do but log and
		// drop the connection.
		return
	}
	flusher, flushable := w.(http.Flusher)
	if flushable {
		flusher.Flush() // header row lands before any other body
	}

	filter := s.exportFilter(ctx, contractID, fromLedger, toLedger)
	for {
		events, cursor, err := s.store.QueryEvents(ctx, filter)
		if errors.Is(err, store.ErrInvalidCursor) {
			loggerFromContext(ctx).Error("export cursor", "error", err)
			return
		}
		if err != nil {
			loggerFromContext(ctx).Error("export query", "error", err)
			return
		}
		for _, ev := range events {
			if err := cw.Write([]string{
				ev.ID,
				fmt.Sprintf("%d", ev.Ledger),
				ev.Type,
				ev.TxHash,
				string(ev.Topics),
				string(ev.Value),
			}); err != nil {
				loggerFromContext(ctx).Error("export csv write", "error", err)
				return
			}
		}
		cw.Flush()
		if flushable {
			flusher.Flush()
		}
		if cursor == "" {
			return
		}
		filter.Cursor = cursor
		// Client disconnect: bail without an error envelope (already
		// streaming, so the headers are gone and any error body would
		// just be noise).
		if ctx.Err() != nil {
			return
		}
	}
}

// streamExportNDJSON writes one JSON object per line, exactly the shape
// served by GET /events/{id} (no wrap, no cursor key — each line IS an
// event). jq-friendly and pandas-friendly (lines=True).
func (s *Server) streamExportNDJSON(ctx context.Context, w http.ResponseWriter, contractID string, fromLedger, toLedger int64) {
	enc := json.NewEncoder(w)
	flusher, flushable := w.(http.Flusher)
	filter := s.exportFilter(ctx, contractID, fromLedger, toLedger)
	for {
		events, cursor, err := s.store.QueryEvents(ctx, filter)
		if errors.Is(err, store.ErrInvalidCursor) {
			loggerFromContext(ctx).Error("export cursor", "error", err)
			return
		}
		if err != nil {
			loggerFromContext(ctx).Error("export query", "error", err)
			return
		}
		for _, ev := range events {
			if err := enc.Encode(ev); err != nil {
				loggerFromContext(ctx).Error("export ndjson write", "error", err)
				return
			}
		}
		if flushable {
			flusher.Flush()
		}
		if cursor == "" {
			return
		}
		filter.Cursor = cursor
		if ctx.Err() != nil {
			return
		}
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
