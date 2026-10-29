package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// This file is the catalogue of every error the API can return (#719):
// one entry per failure condition, naming its status, the canonical
// message a client sees, and the operations it can surface on.
//
// The catalogue is checked against the code rather than maintained
// alongside it by hand: extractErrorCallSites parses every non-test file
// in this package and reports each writeError/http.Error call with the
// status and message argument exactly as written, and
// TestErrorCatalogueMatchesTheSource requires the two to agree, both
// ways. A handler that starts returning a new error therefore fails the
// test until the condition is catalogued with its status, message and
// route — and a stale entry for a condition the handlers no longer
// produce fails it too.
//
// The same data renders docs/errors.md (TestErrorCatalogueDocumentation)
// and is checked against the shipped OpenAPI spec
// (TestErrorCatalogueMatchesTheSpec), so the code, the docs and the spec
// cannot drift apart without a test noticing.

// errorCondition is one catalogued failure.
type errorCondition struct {
	// ID is a stable slug, unique across the catalogue.
	ID string

	// File, Func, Status and Arg identify the call site in the source:
	// the enclosing function, the status argument and the message
	// argument, each whitespace-normalised so reformatting does not
	// invalidate the catalogue. Multiple call sites with identical
	// File/Func/Status/Arg are allowed; each occurrence must have its
	// own entry.
	File   string
	Func   string
	Status int
	Arg    string

	// Message is the canonical message (or message template) a client
	// sees in the "error" field. Format verbs (%q, %s, %d) stand for
	// the values the server interpolates.
	Message string

	// Condition is a full-sentence description of what happened.
	Condition string

	// Count is how many call sites share this exact File/Func/Status/Arg
	// (0 means 1). Handlers with several identical failures — a 404 on
	// every read path inside one function, for example — collapse into
	// one entry whose count the source check verifies.
	Count int

	// Routes lists the chi route patterns ("GET /events") this
	// condition surfaces on. Empty routes mean the condition is looked
	// up in funcRoutes by Func at check time; entries that carry their
	// own Routes override the table (used by the builtin entries, which
	// have no enclosing function).
	Routes []string

	// Builtin marks conditions that are not writeError call sites in
	// this package: the router's own defaults, the probes' health body,
	// and the websocket handshake. They are documented but excluded
	// from the source-code comparison.
	Builtin bool

	// SpecExempt marks conditions whose coverage in the OpenAPI spec is
	// asserted as a set rather than per operation (middleware-wide
	// rejections, whose reachability is "any request the middleware
	// wraps" rather than a fixed route list), or not asserted at all
	// when the spec deliberately omits them (router defaults, the
	// GraphQL envelope).
	SpecExempt bool

	// AnyNonProbe lets a SpecExempt entry satisfy the spec's reverse
	// check on every non-probe operation: these rejections can surface
	// anywhere the middleware runs, so the spec may document them where
	// they matter without enumerating the middleware's full reach.
	AnyNonProbe bool

	// Kind is "envelope" for the JSON {"error": "..."} body,
	// "plain" for responses written outside writeError (chi's own
	// router defaults, the websocket handshake), "health" for the
	// probes' health body, and "graphql" for the GraphQL envelope.
	Kind string

	// Notes records headers or behaviours a client depends on
	// (Retry-After, WWW-Authenticate, Cache-Control: no-store).
	Notes string
}

// errorCatalogue is the catalogue itself. Entries are grouped by file,
// mirroring the source; the docs renderer groups them by status.
//
// Routes default to funcRoutes[Func]; builtin entries carry their own.
var errorCatalogue = []errorCondition{
	// --- auth.go ---
	{File: "auth.go", Func: "Server.authenticate", Status: 403, Arg: `errors.New("tenant is disabled")`, Message: "tenant is disabled", Condition: "multi-tenant authentication holds a valid key for an account an administrator has suspended", SpecExempt: true, AnyNonProbe: true, Notes: "distinct from the 401 so the holder of a real key learns the account is suspended, not that the key is wrong"},
	{File: "auth.go", Func: "Server.authenticate", Status: 500, Arg: `errors.New("authentication failed")`, Count: 2, Message: "authentication failed", Condition: "the tenant store fails while looking up or scoping the presented key", SpecExempt: true, AnyNonProbe: true},
	{File: "auth.go", Func: "Server.handleCreateAPIKey", Status: 400, Arg: `errors.New("name must be at most 100 characters")`, Message: "name must be at most 100 characters", Condition: "POST /apikeys names a key longer than the store allows"},
	{File: "auth.go", Func: "Server.handleCreateAPIKey", Status: 400, Arg: `fmt.Errorf("invalid JSON: %w", err)`, Message: "invalid JSON: <json error>", Condition: "POST /apikeys cannot decode its JSON body"},
	{File: "auth.go", Func: "Server.handleCreateAPIKey", Status: 500, Arg: `errors.New("creating API key failed")`, Count: 2, Message: "creating API key failed", Condition: "the store fails while persisting a freshly minted key"},
	{File: "auth.go", Func: "Server.handleCreateAPIKey", Status: 500, Arg: `errors.New("generating API key failed")`, Message: "generating API key failed", Condition: "the server cannot generate the key material itself"},
	{File: "auth.go", Func: "Server.handleCreateAPIKey", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement API keys (ErrUnsupported, e.g. ClickHouse)"},
	{File: "auth.go", Func: "Server.handleListAPIKeys", Status: 500, Arg: `errors.New("listing API keys failed")`, Message: "listing API keys failed", Condition: "the store fails while listing API keys"},
	{File: "auth.go", Func: "Server.handleListAPIKeys", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement API keys (ErrUnsupported)"},
	{File: "auth.go", Func: "Server.handleRevokeAPIKey", Status: 400, Arg: `fmt.Errorf("API key id must be a positive integer, got %q", chi.URLParam(r, "id"))`, Message: "API key id must be a positive integer, got %q", Condition: "DELETE /apikeys/{id} is given a path id that is not a positive integer"},
	{File: "auth.go", Func: "Server.handleRevokeAPIKey", Status: 404, Arg: `fmt.Errorf("API key %d not found", id)`, Message: "API key %d not found", Condition: "the API key to revoke does not exist"},
	{File: "auth.go", Func: "Server.handleRevokeAPIKey", Status: 500, Arg: `errors.New("revoking API key failed")`, Message: "revoking API key failed", Condition: "the store fails while revoking an API key"},
	{File: "auth.go", Func: "Server.handleRevokeAPIKey", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement API keys (ErrUnsupported)"},
	{File: "auth.go", Func: "Server.requireAPIKey", Status: 500, Arg: `errors.New("checking API key failed")`, Message: "checking API key failed", Condition: "the store fails while resolving the presented key (a backend fault, not a bad credential)"},
	{File: "auth.go", Func: "Server.requireAdmin", Status: 403, Arg: `errors.New("admin privileges required")`, Message: "admin privileges required", Condition: "a non-admin tenant calls an /admin endpoint"},
	{File: "auth.go", Func: "Server.requireAdmin", Status: 404, Arg: `errors.New("admin API is available only when MULTI_TENANT=true")`, Message: "admin API is available only when MULTI_TENANT=true", Condition: "the admin API is called on a single-tenant instance, where the routes are deliberately inert"},
	{File: "auth.go", Func: "Server.requireTenant", Status: 404, Arg: `errors.New("tenant API is available only when MULTI_TENANT=true")`, Message: "tenant API is available only when MULTI_TENANT=true", Condition: "the tenant self-service API is called on a single-tenant instance, where the routes are deliberately inert"},
	{File: "auth.go", Func: "Server.writeUnauthorized", Status: 401, Arg: `errInvalidAPIKey`, Message: "missing or invalid API key", Condition: "requireAPIKey sees a missing, malformed, unknown, revoked or mismatched key — one sentence for all five so the response is not an existence oracle", Notes: "WWW-Authenticate: Bearer"},
	{File: "auth.go", Func: "writeUnauthorized", Status: 401, Arg: `errors.New(msg)`, Message: "missing API key; malformed API key; unknown or revoked API key", Condition: "multi-tenant authentication rejects the credential, with the reason that lets a legitimate caller fix it", SpecExempt: true, AnyNonProbe: true, Notes: "WWW-Authenticate: Bearer realm=\"sorotrail\" per RFC 7235 §4.1"},

	// --- export.go ---
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `err`, Count: 2, Message: "invalid format %q (want csv or ndjson); invalid terminator (want true or false)", Condition: "the export's format parameter names something other than csv or ndjson, or its terminator parameter is neither true nor false"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `errors.New("from_ledger and to_ledger are required for export")`, Message: "from_ledger and to_ledger are required for export", Condition: "the export is requested without explicit ledger bounds"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `fmt.Errorf("from_ledger %d is after to_ledger %d", fromLedger, toLedger)`, Message: "from_ledger %d is after to_ledger %d", Condition: "the export's ledger bounds are inverted"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `fmt.Errorf("from_ledger %s", err.Error())`, Message: "from_ledger <detail>", Condition: "the export's from_ledger bound is not a positive integer"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the export is requested for a path contract id that is not a strkey"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `fmt.Errorf("ledger range [%d,%d] spans %d ledgers, exceeding EXPORT_MAX_RANGE=%d", fromLedger, toLedger, span, s.exportMaxRange)`, Message: "ledger range [%d,%d] spans %d ledgers, exceeding EXPORT_MAX_RANGE=%d", Condition: "the export's ledger range is wider than the operator's cap"},
	{File: "export.go", Func: "Server.handleContractExport", Status: 400, Arg: `fmt.Errorf("to_ledger %s", err.Error())`, Message: "to_ledger <detail>", Condition: "the export's to_ledger bound is not a positive integer"},
	{File: "export.go", Func: "Server.handleEventsCSV", Status: 400, Arg: `err`, Count: 2, Message: "invalid format %q (want csv or ndjson); invalid terminator (want true or false)", Condition: "the CSV export's format parameter names something other than csv or ndjson, or its terminator parameter is neither true nor false"},
	{File: "export_stream.go", Func: "Server.streamExportOpts", Status: 500, Arg: `errors.New("export failed")`, Message: "export failed", Condition: "the store fails before the first byte of an NDJSON export has been written (after that the stream is aborted instead)"},
	// --- handlers.go ---
	{File: "handlers.go", Func: "Server.handleAddWatchedChain", Status: 400, Arg: `errors.New( "adding the first watched contract would switch ingestion from "+ "'all contract events' to a specific list — pass ?confirm=true to acknowledge")`, Message: "adding the first watched contract would switch ingestion from 'all contract events' to a specific list — pass ?confirm=true to acknowledge", Condition: "the first watch would silently narrow ingestion from all contracts to a list, and the caller did not pass ?confirm=true"},
	{File: "handlers.go", Func: "Server.handleAddWatchedChain", Status: 400, Arg: `fmt.Errorf("invalid contract_id %q (want 56-char C... strkey)", req.ContractID)`, Message: "invalid contract_id %q (want 56-char C... strkey)", Condition: "the body's contract_id is not a strkey"},
	{File: "handlers.go", Func: "Server.handleAddWatchedChain", Status: 500, Arg: `errors.New("adding watched contract failed")`, Message: "adding watched contract failed", Condition: "the store fails while adding the contract to the watch list"},
	{File: "handlers.go", Func: "Server.handleAddWatchedChain", Status: 500, Arg: `errors.New("loading ingestion state failed")`, Message: "loading ingestion state failed", Condition: "the store fails while reading where historical replay would start"},
	{File: "handlers.go", Func: "Server.handleAddWatchedChain", Status: 500, Arg: `errors.New("loading watched contracts failed")`, Message: "loading watched contracts failed", Condition: "the store fails while reading the current watch list to decide the mode transition"},
	{File: "handlers.go", Func: "Server.handleAddressEvents", Status: 400, Arg: `err`, Message: "event-filter validation message (see \"Event-filter validation messages\" below)", Condition: "the address-events filter fails shared validation"},
	{File: "handlers.go", Func: "Server.handleAddressEvents", Status: 400, Arg: `fmt.Errorf("address events only support order_by=id (the default)")`, Message: "address events only support order_by=id (the default)", Condition: "the address-events index offers id ordering only and the request asked for another sort"},
	{File: "handlers.go", Func: "Server.handleAddressEvents", Status: 400, Arg: `fmt.Errorf("invalid address %q (want G... or C... strkey)", address)`, Message: "invalid address %q (want G... or C... strkey)", Condition: "the {address} path segment is neither an account nor a contract strkey"},
	{File: "handlers.go", Func: "Server.handleAddressEvents", Status: 500, Arg: `errors.New("querying address events failed")`, Message: "querying address events failed", Condition: "the store fails while querying the address's events"},
	{File: "handlers.go", Func: "Server.handleAddressSummary", Status: 400, Arg: `fmt.Errorf("invalid address %q (want G... or C... strkey)", address)`, Message: "invalid address %q (want G... or C... strkey)", Condition: "the {address} path segment is neither an account nor a contract strkey"},
	{File: "handlers.go", Func: "Server.handleAddressSummary", Status: 404, Arg: `fmt.Errorf("address %q not found", address)`, Message: "address %q not found", Condition: "the address has no rows in the activity index"},
	{File: "handlers.go", Func: "Server.handleAddressSummary", Status: 500, Arg: `errors.New("loading address summary failed")`, Message: "loading address summary failed", Condition: "the store fails while aggregating the address's summary"},
	{File: "handlers.go", Func: "Server.handleAggregateEvents", Status: 400, Arg: `err`, Message: "event-filter validation message (see \"Event-filter validation messages\" below)", Condition: "the aggregation filter fails shared validation"},
	{File: "handlers.go", Func: "Server.handleAggregateEvents", Status: 400, Arg: `errors.New("bucket parameter is required")`, Message: "bucket parameter is required", Condition: "the aggregation is requested without a ?bucket"},
	{File: "handlers.go", Func: "Server.handleAggregateEvents", Status: 400, Arg: `fmt.Errorf("invalid bucket %q: must be \"ledger\" or a duration", bucket)`, Message: "invalid bucket %q: must be \"ledger\" or a duration", Condition: "the ?bucket names neither the ledger bucket nor a Go duration"},
	{File: "handlers.go", Func: "Server.handleAggregateEvents", Status: 500, Arg: `errors.New("aggregating events failed")`, Message: "aggregating events failed", Condition: "the store fails while aggregating events into buckets"},
	{File: "handlers.go", Func: "Server.handleContractEvents", Status: 400, Arg: `fmt.Errorf("invalid contract ID %q", contractID)`, Message: "invalid contract ID %q", Condition: "the {id} path segment of /contracts/{id}/events is not a strkey"},
	{File: "handlers.go", Func: "Server.handleCountEvents", Status: 500, Arg: `errors.New("counting events failed")`, Message: "counting events failed", Condition: "the store fails while counting events matching the filter"},
	{File: "handlers.go", Func: "Server.handleDeleteDeadLetter", Status: 400, Arg: `fmt.Errorf("dead-letter id must be a positive integer, got %q", idStr)`, Message: "dead-letter id must be a positive integer, got %q", Condition: "DELETE /dead-letters/{id} is given a path id that is not a positive integer"},
	{File: "handlers.go", Func: "Server.handleDeleteDeadLetter", Status: 404, Arg: `fmt.Errorf("dead letter %d not found", id)`, Message: "dead letter %d not found", Condition: "the dead letter to delete does not exist (anymore)"},
	{File: "handlers.go", Func: "Server.handleDeleteDeadLetter", Status: 500, Arg: `errors.New("deleting dead letter failed")`, Message: "deleting dead letter failed", Condition: "the store fails while deleting the dead letter"},
	{File: "handlers.go", Func: "Server.handleDeleteEvents", Status: 400, Arg: `errors.New("before_ledger query parameter is required")`, Message: "before_ledger query parameter is required", Condition: "the bulk delete is requested without the mandatory before_ledger bound"},
	{File: "handlers.go", Func: "Server.handleDeleteEvents", Status: 400, Arg: `fmt.Errorf("before_ledger must be a positive integer, got %q", raw)`, Message: "before_ledger must be a positive integer, got %q", Condition: "the bulk delete's before_ledger bound is not a positive integer"},
	{File: "handlers.go", Func: "Server.handleDeleteEvents", Status: 500, Arg: `errors.New("deleting events failed")`, Message: "deleting events failed", Condition: "the store fails while deleting the ledger range"},
	{File: "handlers.go", Func: "Server.handleEventStreamWS", Status: 501, Arg: `errors.New("streaming not configured")`, Message: "streaming not configured", Condition: "the live stream is requested on an instance with no broadcaster"},
	{File: "handlers.go", Func: "Server.handleGetEventRaw", Status: 404, Arg: `fmt.Errorf("event %q has no stored raw XDR", id)`, Message: "event %q has no stored raw XDR", Condition: "the event exists but only its decoded form was kept, so no raw XDR can be served"},
	{File: "handlers.go", Func: "Server.handleGetEventRaw", Status: 404, Arg: `fmt.Errorf("event %q not found", id)`, Count: 2, Message: "event %q not found", Condition: "the event is not stored, or is invisible to the caller's scope (including on the conditional-GET path, so a 304 cannot become an existence oracle)"},
	{File: "handlers.go", Func: "Server.handleGetEventRaw", Status: 500, Arg: `errors.New("loading event failed")`, Count: 2, Message: "loading event failed", Condition: "the store fails while probing existence or loading the raw event"},
	{File: "handlers.go", Func: "Server.handleGetEventTransaction", Status: 400, Arg: `err`, Message: "unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)", Condition: "the ?fields= allowlist names a field the event object does not have"},
	{File: "handlers.go", Func: "Server.handleGetEventTransaction", Status: 404, Arg: `fmt.Errorf("event %q not found", id)`, Message: "event %q not found", Condition: "the anchor event is not stored, or is invisible to the caller's scope"},
	{File: "handlers.go", Func: "Server.handleGetEventTransaction", Status: 500, Arg: `errors.New("loading event failed")`, Message: "loading event failed", Condition: "the store fails while loading the anchor event"},
	{File: "handlers.go", Func: "Server.handleGetEventTransaction", Status: 500, Arg: `errors.New("loading transaction events failed")`, Message: "loading transaction events failed", Condition: "the store fails while loading the anchor transaction's sibling events"},
	{File: "handlers.go", Func: "Server.handleGetEvent", Status: 400, Arg: `err`, Message: "unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)", Condition: "the ?fields= allowlist names a field the event object does not have"},
	{File: "handlers.go", Func: "Server.handleGetEvent", Status: 404, Arg: `fmt.Errorf("event %q not found", id)`, Count: 2, Message: "event %q not found", Condition: "the event is not stored, or is invisible to the caller's scope (both the unconditional and conditional paths)"},
	{File: "handlers.go", Func: "Server.handleGetEvent", Status: 500, Arg: `errors.New("loading event failed")`, Count: 2, Message: "loading event failed", Condition: "the store fails while probing existence or loading the event"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 400, Arg: `fmt.Errorf( "invalid sort %q (want %s, %s, %s, %s, or %s)", f.SortKey, store.SortByContractID, store.SortByActivity, store.SortByFirstLedger, store.SortByLastLedger, store.SortByLastSeen)`, Message: "invalid sort %q (want %s, %s, %s, %s, or %s)", Condition: "GET /contracts is asked to sort by a column it does not offer"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 400, Arg: `fmt.Errorf("invalid cursor %q", f.Cursor)`, Count: 2, Message: "invalid cursor %q", Condition: "the contracts cursor fails the charset check, or passes it but does not decode"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 400, Arg: `fmt.Errorf("invalid order %q (want asc or desc)", f.Order)`, Message: "invalid order %q (want asc or desc)", Condition: "GET /contracts is asked to order by something other than asc or desc"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 400, Arg: `fmt.Errorf("limit must be an integer in [1,%d]", maxContractsListLimit)`, Message: "limit must be an integer in [1,%d]", Condition: "the contracts page size is outside [1, limit]"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 500, Arg: `errors.New("listing contracts failed")`, Message: "listing contracts failed", Condition: "the store fails while listing contracts"},
	{File: "handlers.go", Func: "Server.handleListContracts", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not support listing contracts (ErrUnsupported)"},
	{File: "handlers.go", Func: "Server.handleListDeadLetters", Status: 400, Arg: `fmt.Errorf("invalid cursor %q", f.Cursor)`, Message: "invalid cursor %q", Condition: "the dead-letter cursor is not a cursor this endpoint produced"},
	{File: "handlers.go", Func: "Server.handleListDeadLetters", Status: 400, Arg: `fmt.Errorf("limit must be an integer in [1,%d]", maxLimit)`, Message: "limit must be an integer in [1,%d]", Condition: "the dead-letter page size is outside [1, limit]"},
	{File: "handlers.go", Func: "Server.handleListDeadLetters", Status: 500, Arg: `errors.New("listing dead letters failed")`, Message: "listing dead letters failed", Condition: "the store fails while listing dead letters"},
	{File: "handlers.go", Func: "Server.handleListEventsStream", Status: 500, Arg: `errors.New("querying events failed")`, Message: "querying events failed", Condition: "the store fails on the first NDJSON batch, before any 200 has been committed"},
	{File: "handlers.go", Func: "Server.handleListWatchedChains", Status: 500, Arg: `errors.New("loading watched contracts failed")`, Message: "loading watched contracts failed", Condition: "the store fails while listing the watch list"},
	{File: "handlers.go", Func: "Server.handleRemoveWatchedChain", Status: 400, Arg: `errors.New( "removing the last watched contract would switch ingestion from "+ "'a specific list' back to 'all contract events' — pass ?confirm=true to acknowledge")`, Message: "removing the last watched contract would switch ingestion from 'a specific list' back to 'all contract events' — pass ?confirm=true to acknowledge", Condition: "removing the last watch would silently widen ingestion back to all contracts, and the caller did not pass ?confirm=true"},
	{File: "handlers.go", Func: "Server.handleRemoveWatchedChain", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", id)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers.go", Func: "Server.handleRemoveWatchedChain", Status: 404, Arg: `fmt.Errorf("contract %q is not in the watch list", id)`, Message: "contract %q is not in the watch list", Condition: "the contract to stop watching is not on the watch list"},
	{File: "handlers.go", Func: "Server.handleRemoveWatchedChain", Status: 500, Arg: `errors.New("loading watched contracts failed")`, Message: "loading watched contracts failed", Condition: "the store fails while reading the watch list to decide the mode transition"},
	{File: "handlers.go", Func: "Server.handleRemoveWatchedChain", Status: 500, Arg: `errors.New("removing watched contract failed")`, Message: "removing watched contract failed", Condition: "the store fails while removing the contract from the watch list"},
	{File: "handlers.go", Func: "Server.handleStats", Status: 500, Arg: `errors.New("loading stats failed")`, Message: "loading stats failed", Condition: "the store fails while aggregating the stats counters"},
	{File: "handlers.go", Func: "Server.serveEvents", Status: 400, Arg: `fmt.Errorf( "invalid cursor for order_by=%s; use the cursor returned by the same ordering", filter.OrderBy)`, Message: "invalid cursor for order_by=%s; use the cursor returned by the same ordering", Condition: "a cursor is replayed against a different ordering than the one that produced it (store.ErrInvalidCursor)"},
	{File: "handlers.go", Func: "Server.serveEvents", Status: 500, Arg: `errors.New("querying events failed")`, Message: "querying events failed", Condition: "the store fails while querying a page of events"},
	{File: "handlers.go", Func: "writeFilterError", Status: 400, Arg: `err`, Message: "event-filter validation message (see \"Event-filter validation messages\" below)", Condition: "a list-shaped endpoint rejects a malformed query (shared filter validation); the forbidden-contract case is split out to 403 by the same helper"},
	{File: "handlers.go", Func: "writeForbiddenContract", Status: 403, Arg: `errForbiddenContract{contractID: contractID}`, Message: "contract %s is not granted to this tenant", Condition: "the caller named a contract outside its grants — 403, not 404, because contract IDs are public and hiding an existing contract behind 404 would send operators debugging missing grants as missing data"},
	// decodeJSONBody is the shared request-body decoder. Its two captured
	// writeError sites (400 for a malformed/empty/trailing body, 413 for an
	// oversized body) can surface on every body-accepting route, so both
	// entries are SpecExempt + AnyNonProbe and are checked as status sets
	// rather than per route.
	{File: "handlers.go", Func: "Server.decodeJSONBody", Status: 400, Arg: `err`, Count: 2, Message: "request body is empty; invalid JSON body: <json error>; invalid JSON body: unexpected data after the JSON value; reading request body: <io error>", Condition: "the shared request-body decoder rejects a malformed, empty, or trailing-data body", SpecExempt: true, AnyNonProbe: true},
	{File: "handlers.go", Func: "Server.decodeJSONBody", Status: 413, Arg: `err`, Message: "request body exceeds <limit> bytes", Condition: "the shared request-body decoder rejects an oversized body", SpecExempt: true, AnyNonProbe: true},
	// --- handlers_contracts.go ---
	{File: "handlers_contracts.go", Func: "Server.handleContractStats", Status: 400, Arg: `fmt.Errorf("invalid contract ID %q", contractID)`, Message: "invalid contract ID %q", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_contracts.go", Func: "Server.handleContractStats", Status: 404, Arg: `fmt.Errorf("contract %q not found", contractID)`, Message: "contract %q not found", Condition: "the contract has no indexed events"},
	{File: "handlers_contracts.go", Func: "Server.handleContractStats", Status: 500, Arg: `errors.New("loading contract stats failed")`, Message: "loading contract stats failed", Condition: "the store fails while aggregating the contract's stats"},
	{File: "handlers_contracts.go", Func: "Server.handleContractStats", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend cannot count a contract's events (ErrUnsupported, e.g. ClickHouse)"},
	{File: "handlers_contracts.go", Func: "Server.handleGetContract", Status: 400, Arg: `fmt.Errorf("invalid contract ID %q", contractID)`, Message: "invalid contract ID %q", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_contracts.go", Func: "Server.handleGetContract", Status: 404, Arg: `fmt.Errorf("contract %q not found", contractID)`, Message: "contract %q not found", Condition: "the contract has no indexed events"},
	{File: "handlers_contracts.go", Func: "Server.handleGetContract", Status: 500, Arg: `errors.New("loading contract summary failed")`, Message: "loading contract summary failed", Condition: "the store fails while loading the contract's summary"},
	{File: "handlers_contracts.go", Func: "Server.handleGetContract", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not support contract summaries (ErrUnsupported)"},

	// --- handlers_spec_override.go ---
	{File: "handlers_spec_override.go", Func: "Server.handleDeleteContractSpecOverride", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_spec_override.go", Func: "Server.handleDeleteContractSpecOverride", Status: 500, Arg: `errors.New("deleting contract spec override failed")`, Message: "deleting contract spec override failed", Condition: "the store fails while deleting the override"},
	{File: "handlers_spec_override.go", Func: "Server.handleDeleteContractSpecOverride", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement contract spec overrides (ErrUnsupported)"},
	{File: "handlers_spec_override.go", Func: "Server.handleGetContractSpecOverride", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_spec_override.go", Func: "Server.handleGetContractSpecOverride", Status: 404, Arg: `fmt.Errorf("no spec override for contract %q", contractID)`, Message: "no spec override for contract %q", Condition: "no override was ever stored for this contract"},
	{File: "handlers_spec_override.go", Func: "Server.handleGetContractSpecOverride", Status: 500, Arg: `errors.New("loading contract spec override failed")`, Message: "loading contract spec override failed", Condition: "the store fails while loading the override"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 400, Arg: `err`, Message: "parsing override spec JSON: <json error>; override spec: \"events\" array is required; override spec: event #<n>: <detail>; override spec: duplicate event name %q", Condition: "the override document parses as JSON but is not a valid contract spec (spec.ParseOverrideSpec)"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 400, Arg: `errors.New("request body must contain a \"spec\" object")`, Message: "request body must contain a \"spec\" object", Condition: "the PUT body is valid JSON but carries neither a \"spec\" wrapper nor a bare spec object"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 400, Arg: `fmt.Errorf("invalid JSON body: %w", err)`, Message: "invalid JSON body: <json error>", Condition: "neither wrapped nor bare body could be decoded as JSON"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 500, Arg: `errors.New("saving contract spec override failed")`, Message: "saving contract spec override failed", Condition: "the store fails while persisting the override"},
	{File: "handlers_spec_override.go", Func: "Server.handlePutContractSpecOverride", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement contract spec overrides (ErrUnsupported)"},
	{File: "handlers_spec_override.go", Func: "Server.handleListContractSpecOverrideHistory", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_spec_override.go", Func: "Server.handleListContractSpecOverrideHistory", Status: 400, Arg: `fmt.Errorf("limit must be an integer in [1,%d]", maxLimit)`, Message: "limit must be an integer in [1,%d]", Condition: "the history page size is outside [1, limit]"},
	{File: "handlers_spec_override.go", Func: "Server.handleListContractSpecOverrideHistory", Status: 400, Arg: `err`, Message: "spec override history cursor is not valid base64url; spec override history cursor is not a version id", Condition: "the ?cursor= is not a cursor this endpoint produced"},
	{File: "handlers_spec_override.go", Func: "Server.handleListContractSpecOverrideHistory", Status: 500, Arg: `errors.New("listing contract spec override history failed")`, Message: "listing contract spec override history failed", Condition: "the store fails while listing the override history"},
	{File: "handlers_spec_override.go", Func: "Server.handleListContractSpecOverrideHistory", Status: 501, Arg: `err`, Message: "operation not supported by this store backend", Condition: "the store backend does not implement the override history (ErrUnsupported)"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 400, Arg: `fmt.Errorf("invalid contract id %q (want 56-char C... strkey)", contractID)`, Message: "invalid contract id %q (want 56-char C... strkey)", Condition: "the {id} path segment is not a strkey"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 400, Arg: `errors.New("version must be a positive integer")`, Message: "version must be a positive integer", Condition: "the body names no history version to restore"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 404, Arg: `fmt.Errorf("no spec override version %d for contract %q", versionID, contractID)`, Message: "no spec override version %d for contract %q", Condition: "the named version is not in the contract's override history"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 500, Arg: `errors.New("listing contract spec override history failed")`, Message: "listing contract spec override history failed", Condition: "the store fails while listing the override history to find the version to restore"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 500, Arg: `errors.New("reverting contract spec override failed")`, Message: "reverting contract spec override failed", Condition: "the store fails while writing the restored override"},
	{File: "handlers_spec_override.go", Func: "Server.handleRevertContractSpecOverride", Status: 501, Arg: `err`, Count: 2, Message: "operation not supported by this store backend", Condition: "the store backend does not implement the override history (ErrUnsupported)"},

	// --- handlers_subscription.go ---
	{File: "handlers_subscription.go", Func: "Server.handleCreateSubscription", Status: 400, Arg: `errors.New("secret is required")`, Message: "secret is required", Condition: "the subscription is created without the HMAC signing secret"},
	{File: "handlers_subscription.go", Func: "Server.handleCreateSubscription", Status: 400, Arg: `errors.New("url is required")`, Message: "url is required", Condition: "the subscription is created without a webhook URL"},
	{File: "handlers_subscription.go", Func: "Server.handleCreateSubscription", Status: 500, Arg: `errors.New("creating subscription failed")`, Message: "creating subscription failed", Condition: "the store fails while creating the subscription"},
	{File: "handlers_subscription.go", Func: "Server.handleDeleteSubscription", Status: 400, Arg: `err`, Message: "subscription id must be a positive integer, got %q", Condition: "DELETE /subscriptions/{id} is given a path id that is not a positive integer"},
	{File: "handlers_subscription.go", Func: "Server.handleDeleteSubscription", Status: 404, Arg: `fmt.Errorf("subscription %d not found", id)`, Message: "subscription %d not found", Condition: "the subscription does not exist or is not visible to the caller"},
	{File: "handlers_subscription.go", Func: "Server.handleDeleteSubscription", Status: 500, Arg: `errors.New("deleting subscription failed")`, Message: "deleting subscription failed", Condition: "the store fails while deleting the subscription"},
	{File: "handlers_subscription.go", Func: "Server.handleGetSubscription", Status: 400, Arg: `err`, Message: "subscription id must be a positive integer, got %q", Condition: "GET /subscriptions/{id} is given a path id that is not a positive integer"},
	{File: "handlers_subscription.go", Func: "Server.handleGetSubscription", Status: 404, Arg: `fmt.Errorf("subscription %d not found", id)`, Message: "subscription %d not found", Condition: "the subscription does not exist or is not visible to the caller"},
	{File: "handlers_subscription.go", Func: "Server.handleGetSubscription", Status: 500, Arg: `errors.New("getting subscription failed")`, Message: "getting subscription failed", Condition: "the store fails while loading the subscription"},
	{File: "handlers_subscription.go", Func: "Server.handleListDeliveries", Status: 400, Arg: `err`, Message: "subscription id must be a positive integer, got %q", Condition: "GET /subscriptions/{id}/deliveries is given a path id that is not a positive integer"},
	{File: "handlers_subscription.go", Func: "Server.handleListDeliveries", Status: 400, Arg: `fmt.Errorf("limit must be an integer in [1,%d]", maxLimit)`, Message: "limit must be an integer in [1,%d]", Condition: "the delivery-history page size is outside [1, limit]"},
	{File: "handlers_subscription.go", Func: "Server.handleListDeliveries", Status: 404, Arg: `fmt.Errorf("subscription %d not found", id)`, Message: "subscription %d not found", Condition: "the subscription does not exist or is not visible to the caller"},
	{File: "handlers_subscription.go", Func: "Server.handleListDeliveries", Status: 500, Arg: `errors.New("listing delivery attempts failed")`, Message: "listing delivery attempts failed", Condition: "the store fails while listing delivery attempts"},
	{File: "handlers_subscription.go", Func: "Server.handleListSubscriptions", Status: 500, Arg: `errors.New("listing subscriptions failed")`, Message: "listing subscriptions failed", Condition: "the store fails while listing subscriptions"},
	{File: "handlers_subscription.go", Func: "Server.handleUpdateSubscription", Status: 400, Arg: `err`, Message: "subscription id must be a positive integer, got %q", Condition: "PUT /subscriptions/{id} is given a path id that is not a positive integer"},
	{File: "handlers_subscription.go", Func: "Server.handleUpdateSubscription", Status: 400, Arg: `errors.New("url must not be empty")`, Message: "url must not be empty", Condition: "the update would empty the subscription's webhook URL"},
	{File: "handlers_subscription.go", Func: "Server.handleUpdateSubscription", Status: 404, Arg: `fmt.Errorf("subscription %d not found", id)`, Message: "subscription %d not found", Condition: "the subscription does not exist or is not visible to the caller"},
	{File: "handlers_subscription.go", Func: "Server.handleUpdateSubscription", Status: 500, Arg: `errors.New("getting subscription failed")`, Message: "getting subscription failed", Condition: "the store fails while loading the subscription before the merge"},
	{File: "handlers_subscription.go", Func: "Server.handleUpdateSubscription", Status: 500, Arg: `errors.New("updating subscription failed")`, Message: "updating subscription failed", Condition: "the store fails while persisting the merged subscription"},
	// __CATALOGUE_PART4__
	// --- handlers_tenant.go ---
	{File: "handlers_tenant.go", Func: "Server.handleAddOwnWatched", Status: 400, Arg: `fmt.Errorf("invalid contract_id %q", req.ContractID)`, Message: "invalid contract_id %q", Condition: "the body's contract_id is not a strkey"},
	{File: "handlers_tenant.go", Func: "Server.handleAddOwnWatched", Status: 429, Arg: `err`, Message: "quota exceeded", Condition: "the tenant's max_watched_contracts quota would be exceeded (store.ErrQuotaExceeded)"},
	{File: "handlers_tenant.go", Func: "Server.handleAddOwnWatched", Status: 500, Arg: `errors.New("adding watched contract failed")`, Message: "adding watched contract failed", Condition: "the store fails while recording the tenant's watch"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenantKey", Status: 500, Arg: `errors.New("creating api key failed")`, Message: "creating api key failed", Condition: "the store fails while persisting the tenant key"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenantKey", Status: 500, Arg: `errors.New("generating api key failed")`, Message: "generating api key failed", Condition: "the server cannot generate the tenant key material"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenant", Status: 400, Arg: `err`, Message: "rate_limit_rps must be non-negative; rate_limit_burst must be non-negative; max_watched_contracts must be non-negative; rate_limit_rps and rate_limit_burst must be set together", Condition: "the tenant's quota fields are inconsistent or negative (validateTenantQuotas)"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenant", Status: 400, Arg: `errors.New("name is required")`, Message: "name is required", Condition: "the tenant is created without a name"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenant", Status: 409, Arg: `err`, Message: "already exists", Condition: "a tenant with the same name exists (store.ErrDuplicate)"},
	{File: "handlers_tenant.go", Func: "Server.handleCreateTenant", Status: 500, Arg: `errors.New("creating tenant failed")`, Message: "creating tenant failed", Condition: "the store fails while creating the tenant"},
	{File: "handlers_tenant.go", Func: "Server.handleDeleteTenant", Status: 404, Arg: `fmt.Errorf("tenant %d not found", id)`, Message: "tenant %d not found", Condition: "the tenant to delete does not exist"},
	{File: "handlers_tenant.go", Func: "Server.handleDeleteTenant", Status: 409, Arg: `errors.New("a tenant cannot delete itself")`, Message: "a tenant cannot delete itself", Condition: "a tenant is authenticated as tries to delete its own account"},
	{File: "handlers_tenant.go", Func: "Server.handleDeleteTenant", Status: 500, Arg: `errors.New("deleting tenant failed")`, Message: "deleting tenant failed", Condition: "the store fails while deleting the tenant"},
	{File: "handlers_tenant.go", Func: "Server.handleGrantContract", Status: 400, Arg: `fmt.Errorf("invalid contract_id %q", req.ContractID)`, Message: "invalid contract_id %q", Condition: "the grant names a contract_id that is not a strkey"},
	{File: "handlers_tenant.go", Func: "Server.handleGrantContract", Status: 500, Arg: `errors.New("granting contract failed")`, Message: "granting contract failed", Condition: "the store fails while recording the grant"},
	{File: "handlers_tenant.go", Func: "Server.handleListTenantKeys", Status: 500, Arg: `errors.New("listing api keys failed")`, Message: "listing api keys failed", Condition: "the store fails while listing the tenant's keys"},
	{File: "handlers_tenant.go", Func: "Server.handleListTenants", Status: 500, Arg: `errors.New("listing tenants failed")`, Message: "listing tenants failed", Condition: "the store fails while listing tenants"},
	{File: "handlers_tenant.go", Func: "Server.handleRemoveOwnWatched", Status: 400, Arg: `fmt.Errorf("invalid contract_id %q", contractID)`, Message: "invalid contract_id %q", Condition: "the {contract_id} path segment is not a strkey"},
	{File: "handlers_tenant.go", Func: "Server.handleRemoveOwnWatched", Status: 500, Arg: `errors.New("removing watched contract failed")`, Message: "removing watched contract failed", Condition: "the store fails while dropping the tenant's watch"},
	{File: "handlers_tenant.go", Func: "Server.handleRevokeContract", Status: 500, Arg: `errors.New("revoking contract failed")`, Message: "revoking contract failed", Condition: "the store fails while revoking the grant"},
	{File: "handlers_tenant.go", Func: "Server.handleRevokeTenantKey", Status: 404, Arg: `fmt.Errorf("api key %d not found", id)`, Message: "api key %d not found", Condition: "the tenant key to revoke does not exist"},
	{File: "handlers_tenant.go", Func: "Server.handleRevokeTenantKey", Status: 500, Arg: `errors.New("revoking api key failed")`, Message: "revoking api key failed", Condition: "the store fails while revoking the tenant key"},
	{File: "handlers_tenant.go", Func: "Server.handleUpdateTenant", Status: 400, Arg: `err`, Message: "rate_limit_rps must be non-negative; rate_limit_burst must be non-negative; max_watched_contracts must be non-negative; rate_limit_rps and rate_limit_burst must be set together", Condition: "the update would leave the tenant's quota fields inconsistent or negative (validateTenantQuotas)"},
	{File: "handlers_tenant.go", Func: "Server.handleUpdateTenant", Status: 409, Arg: `err`, Message: "already exists", Condition: "the update would rename the tenant onto an existing name (store.ErrDuplicate)"},
	{File: "handlers_tenant.go", Func: "Server.handleUpdateTenant", Status: 500, Arg: `errors.New("updating tenant failed")`, Message: "updating tenant failed", Condition: "the store fails while updating the tenant"},
	{File: "handlers_tenant.go", Func: "Server.tenantFromPath", Status: 404, Arg: `fmt.Errorf("tenant %d not found", id)`, Message: "tenant %d not found", Condition: "the {id} tenant does not exist"},
	{File: "handlers_tenant.go", Func: "Server.tenantFromPath", Status: 500, Arg: `errors.New("loading tenant failed")`, Message: "loading tenant failed", Condition: "the store fails while loading the {id} tenant"},
	{File: "handlers_tenant.go", Func: "Server.writeGrants", Status: 500, Arg: `errors.New("listing grants failed")`, Message: "listing grants failed", Condition: "the store fails while listing a tenant's grants (on a read after the mutation, so the mutation itself may have landed)"},
	{File: "handlers_tenant.go", Func: "Server.writeOwnWatched", Status: 500, Arg: `errors.New("listing watched contracts failed")`, Message: "listing watched contracts failed", Condition: "the store fails while listing the tenant's own watch list (on a read after the mutation, so the mutation itself may have landed)"},
	{File: "handlers_tenant.go", Func: "Server.writeUsage", Status: 400, Arg: `fmt.Errorf("days must be an integer in [1,%d]", maxUsageDays)`, Message: "days must be an integer in [1,%d]", Condition: "the usage window is outside [1, days]"},
	{File: "handlers_tenant.go", Func: "Server.writeUsage", Status: 500, Arg: `errors.New("listing usage failed")`, Message: "listing usage failed", Condition: "the store fails while tallying usage"},
	{File: "handlers_tenant.go", Func: "pathInt64", Status: 400, Arg: `fmt.Errorf("invalid %s %q", name, raw)`, Message: "invalid %s %q", Condition: "an admin path id ({id} or {key_id}) is not an integer"},

	// --- middleware.go ---
	{File: "middleware.go", Func: "apiKeyAuth", Status: 401, Arg: `errAuthFailed`, Message: "invalid or missing X-API-Key header", Condition: "a management endpoint is called without the configured X-API-Key, or with the wrong one"},
	{File: "middleware.go", Func: "apiKeyAuth", Status: 503, Arg: `errAuthNotConfigured`, Message: "API_KEY env var must be set to access watched-contracts endpoints", Condition: "a management endpoint is called on an instance with no API_KEY — fail-closed, so writes are never open on a misconfigured deployment"},

	// --- ratelimit.go ---
	{File: "ratelimit.go", Func: "RateLimiter.writeLimited", Status: 429, Arg: `errors.New("rate limit exceeded; retry later")`, Message: "rate limit exceeded; retry later", Condition: "the caller exceeded its request rate or burst (or, for tenants, its hourly/daily quota)", SpecExempt: true, AnyNonProbe: true, Notes: "Retry-After plus the X-RateLimit-* headers; /health, /livez, /readyz and /metrics are never limited"},

	// --- recoverer.go ---
	{File: "recoverer.go", Func: "Recoverer.Middleware", Status: 500, Arg: `errors.New(http.StatusText(http.StatusInternalServerError))`, Message: "Internal Server Error", Condition: "a handler panicked; the recovery middleware answers with the generic sentence and logs the stack against X-Request-ID", SpecExempt: true, AnyNonProbe: true},

	// --- server.go ---
	{File: "server.go", Func: "Server.router", Status: 503, Arg: `errors.New("metrics not enabled")`, Message: "metrics not enabled", Condition: "/metrics is scraped on an instance with no metrics collector wired in"},

	// --- Conditions outside writeError (builtins): documented here so the
	// catalogue is complete, excluded from the source comparison. ---
	{File: "<router>", Func: "chi.Router", Status: 404, Builtin: true, Kind: "plain", SpecExempt: true, Message: "404 page not found", Condition: "any path the router does not register", Notes: "plain text written by chi, not the JSON envelope; the spec documents 404 per operation for resource-level misses, not for unknown paths"},
	{File: "<router>", Func: "chi.Router", Status: 405, Builtin: true, Kind: "plain", SpecExempt: true, Message: "(empty body)", Condition: "a registered path is called with a method that path does not accept — chi answers with the status and an Allow header naming the methods that would work, and writes no body at all", Notes: "no body and not even a Content-Type, so a client branches on the status alone; not enumerated per operation in the spec"},
	{File: "<probes>", Func: "Server.handleHealth", Status: 503, Builtin: true, Kind: "health", Routes: []string{"GET /health"}, Message: "{\"status\":\"degraded\"|\"unready\",\"checks\":{…}}", Condition: "a dependency check behind /health fails: \"degraded\" when a dependency answered but reports a suboptimal state, \"unready\" when one did not answer at all — either way 503, with per-dependency status rather than an error envelope", Notes: "documented inline in the spec (not via components/responses), because its shape is the health body"},
	{File: "<probes>", Func: "Server.handleReadyz", Status: 503, Builtin: true, Kind: "health", Routes: []string{"GET /readyz"}, Message: "{\"status\":\"degraded\"|\"unready\",\"checks\":{…}}", Condition: "a dependency check behind /readyz fails or ingestion lags past the readiness threshold — \"degraded\" or \"unready\" in the body, either way 503, with per-dependency status rather than an error envelope", Notes: "documented inline in the spec, because its shape is the health body"},
	{File: "<websocket>", Func: "Server.handleEventStreamWS", Status: 400, Builtin: true, Kind: "plain", Routes: []string{"GET /events/ws"}, Message: "unsupported WebSocket protocol version (only 13 is supported): %q; WebSocket protocol violation: missing Sec-WebSocket-Key; WebSocket protocol violation: multiple Sec-WebSocket-Key headers; WebSocket protocol violation: invalid Sec-WebSocket-Key %q, must be a 16 byte base64 encoded string", Condition: "the request is a websocket upgrade attempt but its handshake is malformed — an unsupported protocol version, or a missing, duplicated or invalid Sec-WebSocket-Key — and the library rejects it after filter validation", Notes: "plain text written by the websocket library during the upgrade, not the JSON envelope; origin checking is disabled (InsecureSkipVerify), so the library's own 403 never fires, and its hijack-failure 500 can only follow the 101, so neither surfaces as a status"},
	{File: "<websocket>", Func: "Server.handleEventStreamWS", Status: 426, Builtin: true, Kind: "plain", Routes: []string{"GET /events/ws"}, Message: "WebSocket protocol violation: handshake request must be at least HTTP/1.1: %q; WebSocket protocol violation: Connection header %q does not contain Upgrade; WebSocket protocol violation: Upgrade header %q does not contain websocket", Condition: "the request is not a websocket upgrade attempt at all — HTTP/1.0, or the Connection and Upgrade headers are missing or do not name websocket — so the library refuses before negotiating (an HTTP/2 request, where connection headers are forbidden, lands here)", Notes: "plain text written by the websocket library, not the JSON envelope"},
	{File: "<graphql>", Func: "graphql.writeErrorEnvelope", Status: 400, Builtin: true, Kind: "graphql", SpecExempt: true, Message: "{\"errors\":[{\"message\":…}]}", Condition: "the GraphQL transport cannot read or parse the request body", Notes: "GraphQL responses use the GraphQL envelope, not errorResponse; /graphql is not in the spec's path list"},
	// __CATALOGUE_END__
}

// funcRoutes maps a call site's enclosing function to the operations its
// errors can surface on. Helper functions (writeFilterError, the auth
// middlewares, the tenant path/body helpers) list the union of their
// callers' routes; TestErrorCatalogueMatchesTheSpec expands these into
// per-operation status claims in both directions.
//
// Kept as one table rather than repeated per entry so a new route
// registration updates its functions' surfaces in exactly one place.
var funcRoutes = map[string][]string{
	// Events, contracts and their exports.
	"Server.handleListEvents":          {"GET /events"},
	"Server.handleListEventsStream":    {"GET /events"},
	"Server.handleCountEvents":         {"GET /events/count"},
	"Server.handleAggregateEvents":     {"GET /events/aggregate"},
	"Server.handleGetEvent":            {"GET /events/{id}"},
	"Server.handleGetEventRaw":         {"GET /events/{id}/raw"},
	"Server.handleGetEventTransaction": {"GET /events/{id}/transaction"},
	"Server.handleEventsCSV":           {"GET /events.csv"},
	"Server.handleListContracts":       {"GET /contracts"},
	"Server.handleGetContract":         {"GET /contracts/{id}"},
	"Server.handleContractEvents":      {"GET /contracts/{id}/events"},
	"Server.handleContractStats":       {"GET /contracts/{id}/stats"},
	"Server.handleContractExport":      {"GET /contracts/{id}/export"},
	"Server.streamExportOpts":          {"GET /events.csv", "GET /contracts/{id}/export"},
	"Server.serveEvents":               {"GET /events", "GET /contracts/{id}/events"},
	// writeFilterError is shared by every list-shaped filter build (and by
	// the subscription filter authorizer); writeForbiddenContract is its
	// 403 branch plus the direct checks in the contract routes.
	"writeFilterError":            {"GET /events", "GET /events/count", "GET /events.csv", "GET /events/ws", "GET /contracts/{id}/events", "GET /contracts/{id}/export", "POST /subscriptions", "PUT /subscriptions/{id}"},
	"writeForbiddenContract":      {"GET /events", "GET /events/count", "GET /events.csv", "GET /events/ws", "GET /contracts/{id}/events", "GET /contracts/{id}/export", "POST /subscriptions", "PUT /subscriptions/{id}"},
	"Server.handleAddressEvents":  {"GET /addresses/{address}/events"},
	"Server.handleAddressSummary": {"GET /addresses/{address}/summary"},
	"Server.handleStats":          {"GET /stats"},
	"Server.handleDeleteEvents":   {"DELETE /events"},
	"Server.handleEventStreamWS":  {"GET /events/ws"},

	// Operator watch list, spec overrides, dead letters.
	"Server.handleListWatchedChains":               {"GET /watched-contracts"},
	"Server.handleAddWatchedChain":                 {"POST /watched-contracts"},
	"Server.handleRemoveWatchedChain":              {"DELETE /watched-contracts/{id}"},
	"Server.handlePutContractSpecOverride":         {"PUT /contracts/{id}/spec"},
	"Server.handleGetContractSpecOverride":         {"GET /contracts/{id}/spec"},
	"Server.handleDeleteContractSpecOverride":      {"DELETE /contracts/{id}/spec"},
	"Server.handleListContractSpecOverrideHistory": {"GET /contracts/{id}/spec/history"},
	"Server.handleRevertContractSpecOverride":      {"POST /contracts/{id}/spec/revert"},
	"Server.handleListDeadLetters":                 {"GET /dead-letters"},
	"Server.handleDeleteDeadLetter":                {"DELETE /dead-letters/{id}"},

	// API keys and subscriptions (requireAPIKey / protect groups).
	"Server.handleCreateAPIKey":       {"POST /apikeys"},
	"Server.handleListAPIKeys":        {"GET /apikeys"},
	"Server.handleRevokeAPIKey":       {"DELETE /apikeys/{id}"},
	"Server.requireAPIKey":            {"POST /apikeys", "GET /apikeys", "DELETE /apikeys/{id}", "GET /events/ws", "POST /subscriptions", "PUT /subscriptions/{id}", "DELETE /subscriptions/{id}", "GET /subscriptions", "GET /subscriptions/{id}", "GET /subscriptions/{id}/deliveries"},
	"Server.writeUnauthorized":        {"POST /apikeys", "GET /apikeys", "DELETE /apikeys/{id}", "GET /events/ws", "POST /subscriptions", "PUT /subscriptions/{id}", "DELETE /subscriptions/{id}", "GET /subscriptions", "GET /subscriptions/{id}", "GET /subscriptions/{id}/deliveries"},
	"Server.handleCreateSubscription": {"POST /subscriptions"},
	"Server.handleGetSubscription":    {"GET /subscriptions/{id}"},
	"Server.handleListSubscriptions":  {"GET /subscriptions"},
	"Server.handleUpdateSubscription": {"PUT /subscriptions/{id}"},
	"Server.handleDeleteSubscription": {"DELETE /subscriptions/{id}"},
	"Server.handleListDeliveries":     {"GET /subscriptions/{id}/deliveries"},

	// Operator management (apiKeyAuth group): fail-closed 503 + 401.
	"apiKeyAuth": {"DELETE /events", "GET /watched-contracts", "POST /watched-contracts", "DELETE /watched-contracts/{id}", "PUT /contracts/{id}/spec", "GET /contracts/{id}/spec", "DELETE /contracts/{id}/spec", "GET /contracts/{id}/spec/history", "POST /contracts/{id}/spec/revert", "GET /dead-letters", "DELETE /dead-letters/{id}"},

	// Tenant self-service.
	"Server.handleWhoAmI":           {"GET /tenant"},
	"Server.handleOwnUsage":         {"GET /tenant/usage"},
	"Server.handleListOwnWatched":   {"GET /tenant/watch"},
	"Server.handleAddOwnWatched":    {"POST /tenant/watch"},
	"Server.handleRemoveOwnWatched": {"DELETE /tenant/watch/{contract_id}"},
	"Server.writeUsage":             {"GET /tenant/usage", "GET /admin/tenants/{id}/usage"},
	"Server.writeOwnWatched":        {"GET /tenant/watch", "POST /tenant/watch", "DELETE /tenant/watch/{contract_id}"},
	"Server.requireTenant":          {"GET /tenant", "GET /tenant/usage", "GET /tenant/watch", "POST /tenant/watch", "DELETE /tenant/watch/{contract_id}"},

	// Tenant administration.
	"Server.handleCreateTenant":     {"POST /admin/tenants"},
	"Server.handleListTenants":      {"GET /admin/tenants"},
	"Server.handleGetTenant":        {"GET /admin/tenants/{id}"},
	"Server.handleUpdateTenant":     {"PATCH /admin/tenants/{id}"},
	"Server.handleDeleteTenant":     {"DELETE /admin/tenants/{id}"},
	"Server.handleTenantUsage":      {"GET /admin/tenants/{id}/usage"},
	"Server.handleListTenantGrants": {"GET /admin/tenants/{id}/grants"},
	"Server.handleGrantContract":    {"POST /admin/tenants/{id}/grants"},
	"Server.handleRevokeContract":   {"DELETE /admin/tenants/{id}/grants/{contract_id}"},
	"Server.handleListTenantKeys":   {"GET /admin/tenants/{id}/keys"},
	"Server.handleCreateTenantKey":  {"POST /admin/tenants/{id}/keys"},
	"Server.handleRevokeTenantKey":  {"DELETE /admin/keys/{key_id}"},
	"Server.requireAdmin":           {"POST /admin/tenants", "GET /admin/tenants", "GET /admin/tenants/{id}", "PATCH /admin/tenants/{id}", "DELETE /admin/tenants/{id}", "GET /admin/tenants/{id}/usage", "GET /admin/tenants/{id}/grants", "POST /admin/tenants/{id}/grants", "DELETE /admin/tenants/{id}/grants/{contract_id}", "GET /admin/tenants/{id}/keys", "POST /admin/tenants/{id}/keys", "DELETE /admin/keys/{key_id}"},
	// tenantFromPath backs every admin route addressed by {id}.
	"Server.tenantFromPath": {"GET /admin/tenants/{id}", "PATCH /admin/tenants/{id}", "GET /admin/tenants/{id}/usage", "GET /admin/tenants/{id}/grants", "POST /admin/tenants/{id}/grants", "DELETE /admin/tenants/{id}/grants/{contract_id}", "GET /admin/tenants/{id}/keys", "POST /admin/tenants/{id}/keys"},
	// pathInt64 is tenantFromPath's parse step plus the two routes that
	// address a non-tenant id directly.
	"pathInt64":          {"GET /admin/tenants/{id}", "PATCH /admin/tenants/{id}", "GET /admin/tenants/{id}/usage", "GET /admin/tenants/{id}/grants", "POST /admin/tenants/{id}/grants", "DELETE /admin/tenants/{id}/grants/{contract_id}", "GET /admin/tenants/{id}/keys", "POST /admin/tenants/{id}/keys", "DELETE /admin/tenants/{id}", "DELETE /admin/keys/{key_id}"},
	"Server.writeGrants": {"GET /admin/tenants/{id}/grants", "POST /admin/tenants/{id}/grants", "DELETE /admin/tenants/{id}/grants/{contract_id}"},
	// decodeJSONBody is the shared request-body decoder; its 400/413
	// rejections can surface on any body-accepting route.
	"Server.decodeJSONBody": nil,

	// Scrape endpoint.
	"Server.router": {"GET /metrics"},

	// Middleware-wide rejections: reachability is "any request the
	// middleware wraps", so their entries are SpecExempt+AnyNonProbe and
	// are checked as sets in TestErrorCatalogueMiddlewareCoverage.
	"writeUnauthorized":        nil, // authenticate's 401 (multi-tenant)
	"Server.authenticate":      nil, // 401/403/500 (multi-tenant)
	"RateLimiter.writeLimited": nil, // 429
	"Recoverer.Middleware":     nil, // panic → 500
}

// routeOf expands a catalogue entry's operation set: the entry's own
// Routes when set (builtin entries), funcRoutes[Func] otherwise.
func routeOf(e errorCondition) ([]string, bool) {
	if e.Routes != nil {
		return e.Routes, true
	}
	routes, ok := funcRoutes[e.Func]
	return routes, ok
}

// statusIdents maps the http.Status* identifiers the handlers write to
// their numeric value, so an identifier the extractor does not know
// fails loudly instead of being silently skipped.
var statusIdents = map[string]int{
	"StatusBadRequest":            400,
	"StatusUnauthorized":          401,
	"StatusForbidden":             403,
	"StatusNotFound":              404,
	"StatusMethodNotAllowed":      405,
	"StatusConflict":              409,
	"StatusTooManyRequests":       429,
	"StatusNotImplemented":        501,
	"StatusServiceUnavailable":    503,
	"StatusInternalServerError":   500,
	"StatusRequestEntityTooLarge": 413,
}

// site is one extracted writeError/http.Error call.
type site struct {
	File   string
	Func   string
	Status int
	Arg    string
}

func (s site) key() string {
	return strings.Join([]string{s.File, s.Func, strconv.Itoa(s.Status), s.Arg}, "|")
}

func (s site) String() string {
	return fmt.Sprintf("%s: %s: writeError(%d, %s)", s.File, s.Func, s.Status, s.Arg)
}

// extractErrorCallSites parses every non-test .go file in this package
// and returns one site per writeError/w http.Error call, in sorted order.
func extractErrorCallSites(t *testing.T) []site {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	var sites []site
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		require.NoErrorf(t, err, "parse %s", name)

		src, err := os.ReadFile(name)
		require.NoErrorf(t, err, "read %s", name)

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			enclosing := funcName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				s, ok := callSite(fset, src, enclosing, call)
				if ok {
					sites = append(sites, s)
				}
				return true
			})
		}
	}

	sort.Slice(sites, func(i, j int) bool { return sites[i].key() < sites[j].key() })
	return sites
}

// funcName renders the enclosing function as "*Server.handleX" for
// methods and "name" for functions, which is how catalogue entries
// refer to call sites.
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	t := fn.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// callSite recognises writeError(w, status, arg) and http.Error(w, arg, status).
func callSite(fset *token.FileSet, src []byte, enclosing string, call *ast.CallExpr) (site, bool) {
	var statusExpr, argExpr ast.Expr
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if fun.Name == "writeError" && len(call.Args) == 3 {
			// writeError(w, status, arg)
			statusExpr, argExpr = call.Args[1], call.Args[2]
		} else {
			return site{}, false
		}
	case *ast.SelectorExpr:
		pkg, isIdent := fun.X.(*ast.Ident)
		if !isIdent || pkg.Name != "http" || fun.Sel.Name != "Error" || len(call.Args) != 3 {
			return site{}, false
		}
		// http.Error(w, arg, status)
		argExpr, statusExpr = call.Args[1], call.Args[2]
	default:
		return site{}, false
	}

	status, ok := statusCode(statusExpr)
	if !ok {
		return site{}, false
	}
	return site{
		File:   filepath.Base(fset.Position(call.Pos()).Filename),
		Func:   enclosing,
		Status: status,
		Arg:    normalizeSource(fset, src, argExpr),
	}, true
}

// statusCode resolves an http.Status* identifier or int literal.
func statusCode(e ast.Expr) (int, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.INT {
			return 0, false
		}
		n, err := strconv.Atoi(v.Value)
		return n, err == nil
	case *ast.SelectorExpr:
		if id, ok := v.X.(*ast.Ident); ok && id.Name == "http" {
			if code, ok := statusIdents[v.Sel.Name]; ok {
				return code, true
			}
		}
	}
	return 0, false
}

// normalizeSource renders an expression as written, with whitespace runs
// collapsed, so gofmt churn (indentation, line breaks) does not change
// the catalogue key.
func normalizeSource(fset *token.FileSet, src []byte, e ast.Expr) string {
	start := fset.Position(e.Pos()).Offset
	end := fset.Position(e.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return "<?>"
	}
	return strings.Join(strings.Fields(string(src[start:end])), " ")
}

// counts turns sites into a multiset of catalogue keys.
func counts(sites []site) map[string]int {
	out := make(map[string]int, len(sites))
	for _, s := range sites {
		out[s.key()]++
	}
	return out
}

// TestErrorCatalogueMatchesTheSource is the guard the issue asks for:
// every error the handlers can write is in the catalogue, and every
// catalogued condition still exists in the code.
func TestErrorCatalogueMatchesTheSource(t *testing.T) {
	sites := extractErrorCallSites(t)

	code := counts(sites)
	cat := map[string]int{}
	for _, e := range errorCatalogue {
		if e.Builtin {
			continue // no call site to match; documented separately
		}
		key := strings.Join([]string{e.File, e.Func, strconv.Itoa(e.Status), e.Arg}, "|")
		cat[key] += max(e.Count, 1)
	}

	// Code → catalogue: an undocumented call site is a new error nobody
	// has written down.
	var undocumented []string
	for key, n := range code {
		if n > cat[key] {
			missing := n - cat[key]
			undocumented = append(undocumented,
				fmt.Sprintf("%s (×%d missing)", strings.ReplaceAll(key, "|", " | "), missing))
		}
	}

	// Catalogue → code: an entry with no call site documents a condition
	// the server can no longer produce.
	var stale []string
	for key, n := range cat {
		if n > code[key] {
			stale = append(stale, fmt.Sprintf("%s (×%d extra)", strings.ReplaceAll(key, "|", " | "), n-code[key]))
		}
	}

	sort.Strings(undocumented)
	sort.Strings(stale)
	if len(undocumented) > 0 {
		t.Errorf("%d error call site(s) missing from errorCatalogue — catalogue each with Message, Condition and its funcRoutes entry:\n\t%s",
			len(undocumented), strings.Join(undocumented, "\n\t"))
	}
	if len(stale) > 0 {
		t.Errorf("%d catalogue entr(ies) with no matching call site in the source — remove or fix them:\n\t%s",
			len(stale), strings.Join(stale, "\n\t"))
	}
}

// TestErrorCatalogueEntriesAreWellFormed keeps the catalogue usable as
// documentation: every entry carries the sentence a reviewer reads, the
// message a client sees, and an operation set the spec check can expand.
func TestErrorCatalogueEntriesAreWellFormed(t *testing.T) {
	require.NotEmpty(t, errorCatalogue, "the catalogue must not be empty")

	ids := map[string]bool{}
	for i, e := range errorCatalogue {
		where := fmt.Sprintf("entry %d (%s:%s %d)", i, e.File, e.Func, e.Status)
		assert.Truef(t, strings.TrimSpace(e.Condition) != "" && e.Condition != "TODO",
			"%s has no Condition", where)
		assert.Truef(t, strings.TrimSpace(e.Message) != "" && !strings.Contains(e.Message, "???"),
			"%s has no Message", where)
		id := e.File + e.Func + strconv.Itoa(e.Status) + e.Arg
		assert.Falsef(t, ids[id], "%s duplicates another entry's identity — merge with Count instead", where)
		ids[id] = true
		assert.Greaterf(t, e.Status, 399, "%s must be an error status", where)
		if e.Builtin {
			continue
		}
		routes, ok := routeOf(e)
		if assert.Truef(t, ok, "%s: Func %q is not in funcRoutes — register its operations (or set Routes/SpecExempt)", where, e.Func) {
			if !e.SpecExempt {
				assert.NotEmptyf(t, routes, "%s has no routes and is not SpecExempt", where)
			}
		}
	}
}

// TestErrorCatalogueMatchesTheSpec ties the three together: every
// (operation, status) a handler can produce must be documented on that
// operation, and every documented 4xx/5xx must be one a handler (or the
// router) can actually produce — with the middleware-wide rejections and
// router defaults accounted for by their own rules (AnyNonProbe entries
// and TestErrorCatalogueMiddlewareCoverage's status sets).
//
// The direction that matters most for a generated client is the forward
// one: a status the code can return and the spec omits means a client
// meets an unexpected status in production.
func TestErrorCatalogueMatchesTheSpec(t *testing.T) {
	doc := loadSpec(t)

	// authExempt mirrors auth.go's publicPaths: the operations no
	// middleware authenticates, so no catalogued middleware rejection can
	// stand in for their documented statuses. /metrics is rate-limit
	// exempt but NOT auth exempt — its 401/500 are real.
	authExempt := map[string]bool{
		"GET /health": true,
		"GET /livez":  true,
		"GET /readyz": true,
	}

	// Forward: catalogue → spec.
	covered := map[string]bool{} // "METHOD /path status" documented by an entry
	for _, e := range errorCatalogue {
		routes, ok := routeOf(e)
		if !ok {
			continue // SpecExempt entries without routes are checked as sets
		}
		for _, route := range routes {
			method, path, found := strings.Cut(route, " ")
			require.Truef(t, found, "route %q must be \"METHOD /path\"", route)
			op, exists := doc.Paths[path][strings.ToLower(method)]
			if !assert.Truef(t, exists, "catalogue claims %s but the spec documents no such operation", route) {
				continue
			}
			key := route + " " + strconv.Itoa(e.Status)
			if e.SpecExempt {
				continue
			}
			covered[key] = true
			resp, documented := op.Responses[strconv.Itoa(e.Status)]
			if assert.Truef(t, documented,
				"%s can return %d (%s) but the spec does not document it",
				route, e.Status, e.Condition) {
				assert.NotEmpty(t, doc.resolve(t, resp).Description,
					"%s %d is documented but says nothing", route, e.Status)
			}
		}
	}

	// Reverse: spec → catalogue. Every documented 4xx/5xx must be
	// attributable to a catalogued condition on that operation, an
	// AnyNonProbe middleware rejection (non-probe operations only), or
	// the probes' health-body 503s.
	for route, pathItem := range doc.Paths {
		for method, op := range pathItem {
			if method == "parameters" {
				continue
			}
			normRoute := strings.ToUpper(method) + " " + route
			for statusStr := range op.Responses {
				code, err := strconv.Atoi(statusStr)
				require.NoError(t, err)
				if code < 400 {
					continue
				}
				key := normRoute + " " + statusStr
				if covered[key] {
					continue
				}
				// AnyNonProbe: a middleware-wide rejection may stand in for
				// any non-probe operation's documented status of its own value.
				matched := false
				if !authExempt[normRoute] {
					for _, e := range errorCatalogue {
						if e.SpecExempt && e.AnyNonProbe && e.Status == code {
							matched = true
							break
						}
					}
				}
				assert.Truef(t, matched,
					"%s %s is documented in the spec but no catalogued condition produces it",
					normRoute, statusStr)
			}
		}
	}
}

// TestErrorCatalogueMiddlewareCoverage checks the three statuses whose
// reachability is defined by middleware rather than by a route list, as
// exact sets over the spec: both directions (an operation the middleware
// wraps that forgets the status, and a probe that wrongly claims it) are
// drift, and the sets are how the reader learns which endpoints are
// exempt from what.
func TestErrorCatalogueMiddlewareCoverage(t *testing.T) {
	doc := loadSpec(t)

	ops := map[string]bool{}
	for route, pathItem := range doc.Paths {
		for method := range pathItem {
			if method == "parameters" {
				continue
			}
			ops[strings.ToUpper(method)+" "+route] = true
		}
	}

	without := func(status int) []string {
		var out []string
		for op := range ops {
			method, route, _ := strings.Cut(op, " ")
			pathItem := doc.Paths[route]
			if pathItem == nil {
				continue
			}
			operation, registered := pathItem[strings.ToLower(method)]
			if !registered {
				continue
			}
			if _, ok := operation.Responses[strconv.Itoa(status)]; !ok {
				out = append(out, op)
			}
		}
		sort.Strings(out)
		return out
	}

	// The limiter wraps every route except the probes and the scrape
	// endpoint: throttling must not blind an operator to the throttling
	// itself. Every other operation can be refused with 429 whenever
	// RATE_LIMIT_RPS (or a tenant override) is configured.
	assert.Equal(t,
		[]string{"GET /health", "GET /livez", "GET /metrics", "GET /readyz"},
		without(429),
		"429 must be documented on exactly the rate-limited operations")

	// authenticate, apiKeyAuth and requireAPIKey run on everything but
	// auth.go's publicPaths (the three probes): under MULTI_TENANT every
	// non-probe operation can answer 401, and the management and key
	// surfaces answer it regardless of deployment shape.
	assert.Equal(t,
		[]string{"GET /health", "GET /livez", "GET /readyz"},
		without(401),
		"401 must be documented on exactly the authenticated operations")

	// The recovery middleware wraps every handler, so any operation can
	// answer 500 after a panic; the spec advertises that everywhere
	// except the probes, whose documented failure is their own health
	// body (503).
	assert.Equal(t,
		[]string{"GET /health", "GET /livez", "GET /readyz"},
		without(500),
		"500 must be documented on every non-probe operation")
}

// filterValidationMessages is the shared event-filter validation family:
// the concrete sentences behind every catalogue entry whose Message points
// at "Event-filter validation messages". They are produced inside
// filterFromQuery, parseFields and the subscription filter authorizer and
// surfaced through writeFilterError (400, or 403 for a contract outside
// the caller's grants) or a handler's own 400.
//
// The list is rendered into docs/errors.md and pinned there by
// TestErrorCatalogueDocumentation, so the prose cannot drift from the
// catalogue; a representative sample is asserted end to end by
// TestErrorCatalogueRuntimeMessages.
var filterValidationMessages = []string{
	`invalid contract_id %q`,
	`invalid cursor %q`,
	`invalid type %q (want contract|system|diagnostic)`,
	`invalid order %q (want asc or desc)`,
	`invalid order_by %q (want %s, %s or %s)`,
	`invalid tx_index %d (want a non-negative integer)`,
	`invalid op_index %d (want a non-negative integer)`,
	`limit must be an integer in [1,%d]`,
	`from_ledger %s` + ` / ` + `to_ledger %s (wrapping "must be a positive integer")`,
	`from_time %s` + ` / ` + `to_time %s (wrapping "must be an RFC 3339 timestamp (e.g. 2026-07-21T00:00:00Z)" or "sub-second precision is not supported")`,
	`from_ledger %d is after to_ledger %d`,
	`from_time %s is after to_time %s`,
	`topic and topic0..topic3 filters cannot be combined`,
	`topic: %w / topic0: %w … topic3: %w (wrapping the JSON-parse failure of that filter)`,
	`topic_contains must be valid JSON`,
	`unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)`,
	`unauthenticated (the subscription authorizer found no principal — POST/PUT /subscriptions)`,
	`filters.contract_id is required: a subscription must name one of the contracts granted to this tenant`,
}

// updateErrorDocs regenerates docs/errors.md from the catalogue:
//
//	go test ./internal/api -run TestErrorCatalogueDocumentation -update-error-catalogue
var updateErrorDocs = flag.Bool("update-error-catalogue", false, "rewrite docs/errors.md from errorCatalogue")

// TestErrorCatalogueDocumentation keeps the human catalogue byte-identical
// to what the test data describes: render, then compare with docs/errors.md.
func TestErrorCatalogueDocumentation(t *testing.T) {
	got := renderErrorCatalogue()
	path := filepath.Join("..", "..", "docs", "errors.md")
	if *updateErrorDocs {
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		t.Logf("updated %s", path)
		return
	}
	want, err := os.ReadFile(path)
	require.NoErrorf(t, err,
		"docs/errors.md does not exist; generate it with:\n"+
			"\tgo test ./internal/api -run TestErrorCatalogueDocumentation -update-error-catalogue")
	assert.Equal(t, string(want), got,
		"docs/errors.md is stale; regenerate with:\n"+
			"\tgo test ./internal/api -run TestErrorCatalogueDocumentation -update-error-catalogue")
}

// renderErrorCatalogue produces docs/errors.md: header, conventions, one
// table per status, the filter-validation family, then the responses that
// are not the shared envelope at all.
func renderErrorCatalogue() string {
	var b strings.Builder

	b.WriteString(`# API error catalogue

Every failure the SoroTrail API can return, in one place: the condition
that produces it, the HTTP status, and the exact message a client sees.
Error bodies are the shared JSON envelope {"error": "<message>"} (the
ErrorResponse schema in ` + "`api/openapi.yaml`" + `, with ` + "`Cache-Control: no-store`" + `) — the exceptions
are listed under "Responses outside the shared envelope" at the end.

This file is generated from the catalogue in
` + "`internal/api/error_catalogue_test.go`" + `, which is itself checked against
the handlers' source and the shipped OpenAPI spec, so a new error cannot
appear in the code without appearing here. Regenerate it with:

    go test ./internal/api -run TestErrorCatalogueDocumentation -update-error-catalogue

Messages are templates: ` + "`%q`" + `, ` + "`%s`" + ` and ` + "`%d`" + ` stand for the values the
server interpolates (the offending parameter, an id, a bound), and
` + "`<detail>`" + ` or ` + "`<json error>`" + ` stand for a wrapped underlying error.
Entries marked "middleware-wide" can surface on any operation the
middleware wraps; the OpenAPI spec documents them on the operations where
they matter rather than on every path.

## Conventions

- Every envelope error carries ` + "`Cache-Control: no-store`" + ` so a CDN cannot pool
  a 4xx/5xx behind a success response's validator.
- 429 carries ` + "`Retry-After`" + ` (delta-seconds, at least 1) plus the
  ` + "`X-RateLimit-*`" + ` family.
- 401 carries ` + "`WWW-Authenticate`" + `: ` + "`Bearer realm=\"sorotrail\"`" + ` on the tenant
  credential, ` + "`Bearer`" + ` on the instance key — both per RFC 7235 §4.1.
- 500 messages are deliberately generic; the specifics are logged
  against the request's ` + "`X-Request-ID`" + `.

## Errors by status

`)

	// Statuses in ascending order, from the source entries only.
	seenStatus := map[int]bool{}
	var statuses []int
	for _, e := range errorCatalogue {
		if e.Builtin {
			continue
		}
		if !seenStatus[e.Status] {
			seenStatus[e.Status] = true
			statuses = append(statuses, e.Status)
		}
	}
	sort.Ints(statuses)

	for _, status := range statuses {
		fmt.Fprintf(&b, "### %d %s\n\n", status, http.StatusText(status))
		b.WriteString("| Condition | Operations | Message |\n")
		b.WriteString("| --- | --- | --- |\n")
		for _, e := range errorCatalogue {
			if e.Builtin || e.Status != status {
				continue
			}
			fmt.Fprintf(&b, "| %s | %s | `%s` |\n",
				catalogueCell(e.Condition), catalogueCell(catalogueOperations(e)),
				catalogueCell(e.Message))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Event-filter validation messages\n\n")
	b.WriteString(`List-shaped endpoints validate their query through one shared filter
builder; a malformed value is a 400 carrying exactly one of these
sentences, and a contract outside the caller's grants is the same
condition reported as 403 (see the writeForbiddenContract entry above).
The subscription create/update paths re-check their merged filters
through the same helper.

`)
	for _, msg := range filterValidationMessages {
		fmt.Fprintf(&b, "- %s\n", "`"+msg+"`")
	}
	b.WriteString("\n")

	b.WriteString("## Responses outside the shared envelope\n\n")
	b.WriteString("| Status | Kind | Condition | Message | Operations |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, e := range errorCatalogue {
		if !e.Builtin {
			continue
		}
		fmt.Fprintf(&b, "| %d | %s | %s | `%s` | %s |\n",
			e.Status, e.Kind, catalogueCell(e.Condition),
			catalogueCell(e.Message), catalogueCell(catalogueOperations(e)))
	}
	b.WriteString(`
Chi's router answers unknown paths and wrong methods with plain text,
not JSON — a client must branch on Content-Type only for these two. The
probe bodies are the health envelope (per-dependency status), not error
messages. GraphQL responses use the GraphQL spec's own
` + "`{\"errors\": [...]}`" + ` envelope.
`)

	return b.String()
}

// catalogueOperations renders an entry's operation set for the docs.
func catalogueOperations(e errorCondition) string {
	routes, _ := routeOf(e)
	switch {
	case len(routes) > 0:
		return strings.Join(routes, ", ")
	case e.AnyNonProbe:
		return "any authenticated or rate-limited operation (middleware-wide)"
	case e.Func == "chi.Router":
		return "any path or method the router does not accept"
	default:
		return "not enumerated per operation in the spec"
	}
}

// catalogueCell keeps markdown tables from breaking: pipes escaped, no
// raw newlines.
func catalogueCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// catalogueCase drives one request through the real router and pins the
// exact canonical message a client receives — the runtime half of "one
// canonical message per condition", complementing the source-level check.
type catalogueCase struct {
	name   string
	method string
	target string
	// key is sent as X-API-Key when non-empty.
	key  string
	body string
	stub *stubStore
	// build overrides the generic server+request construction entirely.
	build  func(t *testing.T) (http.Handler, *http.Request)
	server func(t *testing.T, st *stubStore) *Server
	// pre runs against the same handler before the measured request
	// (used to burn a rate-limit token).
	pre        func(t *testing.T, h http.Handler)
	wantStatus int
	// wantMsg is the exact error envelope message (skipped when empty).
	wantMsg string
	// wantPlain, when non-empty, is the exact plain-text body asserted
	// alongside a text/plain content type.
	wantPlain string
	// wantEmptyBody asserts the failure carries neither a body nor a
	// Content-Type: chi's 405 answers with the status and an Allow header
	// only, which is the opposite convention and easy to regress.
	wantEmptyBody bool
	after         func(t *testing.T, rec *httptest.ResponseRecorder)
}

func TestErrorCatalogueRuntimeMessages(t *testing.T) {
	cases := []catalogueCase{
		{name: "filter rejects an invalid type", method: http.MethodGet, target: "/events?type=bogus",
			wantStatus: http.StatusBadRequest, wantMsg: `invalid type "bogus" (want contract|system|diagnostic)`},
		{name: "filter rejects a malformed contract_id", method: http.MethodGet, target: "/events?contract_id=not-a-strkey",
			wantStatus: http.StatusBadRequest, wantMsg: `invalid contract_id "not-a-strkey"`},
		{name: "aggregate requires its bucket", method: http.MethodGet, target: "/events/aggregate",
			wantStatus: http.StatusBadRequest, wantMsg: "bucket parameter is required"},
		{name: "unknown event id", method: http.MethodGet, target: "/events/0001099511627776-0000000001",
			stub:       &stubStore{eventErr: store.ErrNotFound},
			wantStatus: http.StatusNotFound, wantMsg: `event "0001099511627776-0000000001" not found`},
		{name: "management endpoint without the key", method: http.MethodDelete, target: "/events",
			wantStatus: http.StatusUnauthorized, wantMsg: "invalid or missing X-API-Key header"},
		{name: "bulk delete with a malformed bound", method: http.MethodDelete, target: "/events?before_ledger=abc", key: "test-key",
			wantStatus: http.StatusBadRequest, wantMsg: `before_ledger must be a positive integer, got "abc"`},
		{name: "management endpoint with the wrong key", method: http.MethodGet, target: "/watched-contracts", key: "not-the-key",
			wantStatus: http.StatusUnauthorized, wantMsg: "invalid or missing X-API-Key header"},
		{name: "management endpoint with no API_KEY configured", method: http.MethodGet, target: "/dead-letters",
			server:     func(t *testing.T, st *stubStore) *Server { return newTestServerWithKey(st, nil, "") },
			wantStatus: http.StatusServiceUnavailable, wantMsg: "API_KEY env var must be set to access watched-contracts endpoints"},
		{name: "instance-key surface without a credential", method: http.MethodGet, target: "/apikeys",
			wantStatus: http.StatusUnauthorized, wantMsg: "missing or invalid API key",
			after: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"),
					"the 401 must carry the RFC 7235 challenge")
			}},
		{name: "subscription id that is not a number", method: http.MethodGet, target: "/subscriptions/not-a-number",
			wantStatus: http.StatusBadRequest, wantMsg: `subscription id must be a positive integer, got "not-a-number"`},
		{name: "admin API inert on a single-tenant instance", method: http.MethodGet, target: "/admin/tenants",
			wantStatus: http.StatusNotFound, wantMsg: "admin API is available only when MULTI_TENANT=true"},
		{name: "tenant API inert on a single-tenant instance", method: http.MethodGet, target: "/tenant",
			wantStatus: http.StatusNotFound, wantMsg: "tenant API is available only when MULTI_TENANT=true"},
		{name: "contract id in the path that is not a strkey", method: http.MethodGet, target: "/contracts/CNOTACONTRACT",
			wantStatus: http.StatusBadRequest, wantMsg: `invalid contract ID "CNOTACONTRACT"`},
		{name: "spec override that carries no spec", method: http.MethodPut, key: "test-key",
			target: "/contracts/" + contractA + "/spec", body: `{}`,
			wantStatus: http.StatusBadRequest, wantMsg: `request body must contain a "spec" object`},
		{name: "metrics not wired in", method: http.MethodGet, target: "/metrics",
			server: func(t *testing.T, st *stubStore) *Server {
				s := newTestServer(st, nil)
				s.metrics = nil
				return s
			},
			wantStatus: http.StatusServiceUnavailable, wantMsg: "metrics not enabled"},
		{name: "rate limited after the burst", method: http.MethodGet, target: "/events",
			server: func(t *testing.T, st *stubStore) *Server {
				lim := NewRateLimiter(0.5, 1, false)
				t.Cleanup(startLimiter(t, lim))
				s := newTestServer(st, nil)
				s.SetRateLimiter(lim)
				return s
			},
			pre: func(t *testing.T, h http.Handler) {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
				require.Equal(t, http.StatusOK, rec.Code, "the first request must pass the limiter")
			},
			wantStatus: http.StatusTooManyRequests, wantMsg: "rate limit exceeded; retry later",
			after: func(t *testing.T, rec *httptest.ResponseRecorder) {
				secs, err := strconv.Atoi(rec.Header().Get("Retry-After"))
				require.NoError(t, err, "429 must carry an integer Retry-After")
				assert.GreaterOrEqual(t, secs, 1)
			}},
		{name: "path the router does not register", method: http.MethodGet, target: "/no-such-route",
			wantStatus: http.StatusNotFound, wantPlain: "404 page not found\n"},
		{name: "method the route does not accept", method: http.MethodDelete, target: "/version",
			wantStatus: http.StatusMethodNotAllowed, wantEmptyBody: true,
			after: func(t *testing.T, rec *httptest.ResponseRecorder) {
				assert.Contains(t, rec.Header().Get("Allow"), "GET",
					"chi's 405 names the methods the route does accept")
			}},
		{name: "websocket handshake without an upgrade", method: http.MethodGet, target: "/events/ws",
			server: func(t *testing.T, st *stubStore) *Server {
				s, _ := newServerWithBroadcaster(16)
				return s
			},
			wantStatus: http.StatusUpgradeRequired,
			wantPlain:  "WebSocket protocol violation: Connection header \"\" does not contain Upgrade\n"},
		{name: "websocket handshake with a missing key", build: func(t *testing.T) (http.Handler, *http.Request) {
			s, _ := newServerWithBroadcaster(16)
			req := httptest.NewRequest(http.MethodGet, "/events/ws", nil)
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			return s.Router(), req
		},
			wantStatus: http.StatusBadRequest,
			wantPlain:  "WebSocket protocol violation: missing Sec-WebSocket-Key\n"},
		{name: "probe reports a failed dependency", method: http.MethodGet, target: "/health",
			stub:       &stubStore{pingErr: errStoreDown},
			wantStatus: http.StatusServiceUnavailable,
			after: func(t *testing.T, rec *httptest.ResponseRecorder) {
				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body),
					"the probe answers with the health body: %s", rec.Body.String())
				assert.Equal(t, "unready", body["status"],
					"a dependency that did not answer leaves the probe unready; degraded is for one that answered suboptimally")
				checks, ok := body["checks"].(map[string]any)
				require.Truef(t, ok, "the health body must carry checks, got %T", body["checks"])
				db, ok := checks["database"].(map[string]any)
				require.Truef(t, ok, "the failed dependency must be named, got %T", checks["database"])
				assert.Equal(t, "unready", db["status"], "the named dependency reports why the probe failed")
			}},
		{name: "contract outside the tenant's grants",
			build: func(t *testing.T) (http.Handler, *http.Request) {
				f := newTenantFixture(t)
				req := httptest.NewRequest(http.MethodGet, "/events?contract_id="+contractB, nil)
				req.Header.Set("Authorization", "Bearer "+f.keyA)
				return f.srv, req
			},
			wantStatus: http.StatusForbidden,
			wantMsg:    fmt.Sprintf("contract %s is not granted to this tenant", contractB)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h http.Handler
			var req *http.Request
			if tc.build != nil {
				h, req = tc.build(t)
			} else {
				st := tc.stub
				if st == nil {
					st = &stubStore{}
				}
				var s *Server
				if tc.server != nil {
					s = tc.server(t, st)
				} else {
					s = newTestServer(st, nil)
				}
				h = s.Router()
				req = httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
				if tc.key != "" {
					req.Header.Set("X-API-Key", tc.key)
				}
			}
			if tc.pre != nil {
				tc.pre(t, h)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			t.Logf("body: %q content-type: %q", rec.Body.String(), rec.Header().Get("Content-Type"))

			assert.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			if tc.wantPlain != "" {
				assert.Contains(t, rec.Header().Get("Content-Type"), "text/plain",
					"router-level rejections are plain text, not the envelope")
				assert.Equal(t, tc.wantPlain, rec.Body.String())
			} else if tc.wantEmptyBody {
				assert.Empty(t, rec.Body.String(),
					"chi's 405 answers with the status and an Allow header, and no body")
				assert.Empty(t, rec.Header().Get("Content-Type"),
					"an empty body gets no Content-Type")
			} else if tc.wantMsg != "" {
				var env struct {
					Error string `json:"error"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env),
					"envelope errors are JSON: %s", rec.Body.String())
				assert.Equal(t, tc.wantMsg, env.Error)
			}
			if tc.after != nil {
				tc.after(t, rec)
			}
		})
	}
}
