package api

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/store"
)

// param_validation_test.go pins the parameters the shared layer already owns.
// This file pins the ones it does not: request parameters whose *name* is
// shared across endpoints while their validation is not, which is the
// divergence class the issue is about.
//
// Each subtest asserts the behaviour that exists today and says what a fix
// would change, so folding a parameter into the shared layer fails the subtest
// that documents the gap rather than passing unnoticed. That is deliberate:
// these are pins, not endorsements, and #712's guidance is to report a real
// production divergence rather than silently rewriting it in a test PR.

func TestContractIDMeansTwoThingsOnTwoEndpoints(t *testing.T) {
	// GET /events treats contract_id as a list of strkeys and refuses a typo,
	// so a misspelled ID is an error rather than an empty page. GET /contracts
	// reads the same parameter name as a literal prefix and validates nothing,
	// so a client cannot tell "no contracts match" from "that is not an ID".
	const nonsense = "not-a-valid-strkey"

	events := routeRegistry["GET /events"]
	status, message := requestURL(t, events, events.target+"contract_id="+nonsense)
	require.Equal(t, http.StatusBadRequest, status, "GET /events stopped rejecting a malformed contract_id")
	assert.Equal(t, fmt.Sprintf("invalid contract_id %q", nonsense), message)

	contracts := routeRegistry["GET /contracts"]
	status, message = requestURL(t, contracts, contracts.target+"contract_id="+nonsense)
	assert.Equalf(t, http.StatusOK, status,
		"GET /contracts now shape-checks contract_id: %s — move it into sharedParsedParameters and merge this subtest",
		message)
}

func TestSomeFilterParametersReachTheStoreUnvalidated(t *testing.T) {
	// tx_hash and contract_id_prefix are handed to queries.BuildEventFilter as
	// text and forwarded without a shape check, while contract_id in the same
	// request is checked element by element. The asymmetry is the finding: one
	// typo'd parameter is a 400 and its neighbours are an empty page.
	//
	// A store that rejects either of these today would fail this subtest, which
	// is the signal to delete it.
	for _, tc := range []struct{ param, value string }{
		{"tx_hash", "not-a-transaction-hash"},
		{"contract_id_prefix", "Cnot-a-prefix!!"},
	} {
		tc := tc
		t.Run(tc.param, func(t *testing.T) {
			route := routeRegistry["GET /events"]
			status, message := requestURL(t, route, route.target+tc.param+"="+tc.value)
			require.Equalf(t, http.StatusOK, status,
				"?%s=%s is now rejected: %s — it has joined the validated set, so reclassify it", tc.param, tc.value, message)
			assert.Empty(t, message)
		})
	}
}

func TestBeforeLedgerRestatesTheSharedPositiveIntegerRule(t *testing.T) {
	// DELETE /events parses ?before_ledger= with its own strconv call, so the
	// sentence a client sees differs from the one queries.ParseLedgerParam
	// produces for the identical mistake on ?from_ledger=. Both are correct
	// today; the point is that they are two sources of truth, and this is the
	// test that notices when one of them moves.
	_, sharedErr := queries.ParseLedgerParam("0")
	require.Error(t, sharedErr)
	sharedMessage := "before_ledger " + sharedErr.Error()

	admin := paramEndpoint{name: "DELETE /events", target: "/events?"}
	status, message := requestMethodURL(t, admin, http.MethodDelete, admin.target+"before_ledger=0")
	require.Equal(t, http.StatusBadRequest, status, "DELETE /events accepted before_ledger=0")

	// Same class of invalid input, so the shared wording has to be in there …
	assert.Containsf(t, message, sharedErr.Error(),
		"before_ledger no longer explains itself the way the shared parser does: %q", message)
	// … and it is not yet the shared wording, which is the gap.
	assert.NotEqualf(t, sharedMessage, message,
		"before_ledger is now parsed by the shared layer (%q) — delete this subtest's NotEqual and move the "+
			"parameter into sharedParsedParameters", message)
}

func TestBooleanParametersSplitBetweenStrictAndPermissiveParsing(t *testing.T) {
	// Two kinds of flag exist on the events endpoints and they fail in opposite
	// directions. in_successful_call/has_value/decoded are parsed by a switch
	// that 400s on anything it does not recognise; envelope/include_xdr/pretty
	// compare against the literal "true" and treat every other spelling as
	// false. A client that writes ?envelope=yes gets a 200 and a body shaped
	// differently from the one it asked for, silently.
	//
	// The permissive side is pinned rather than fixed here because flipping it
	// to strict is a breaking change for clients already sending 1/yes, which
	// is a decision for the maintainers rather than a test PR.
	strict := routeRegistry["GET /events"]
	permissive := routeRegistry["GET /contracts/{id}/events"]

	for _, value := range []string{"yes", "1", "TRUE"} {
		value := value
		t.Run(value, func(t *testing.T) {
			status, message := requestURL(t, strict, strict.target+"has_value="+value)
			require.Equalf(t, http.StatusBadRequest, status,
				"the strict tri-state parser accepted ?has_value=%s: %s", value, message)
			assert.Contains(t, message, "has_value must be true or false")

			status, message = requestURL(t, permissive, permissive.target+"envelope="+value)
			assert.Equalf(t, http.StatusOK, status,
				"?envelope=%s is now rejected rather than read as false: %s — the two families have converged",
				value, message)
		})
	}
}

func TestTheContractsListingHasItsOwnOrderingEnum(t *testing.T) {
	// GET /contracts validates its sort column with store.ValidContractsSortKey
	// while the event endpoints validate order_by through
	// queries.BuildEventFilter. The two enums overlap (both can order by
	// ledger) without either being a subset of the other, so a client cannot
	// reuse one list of accepted values across endpoints.
	contracts := routeRegistry["GET /contracts"]
	status, message := requestURL(t, contracts, contracts.target+"sort=recency")
	require.Equal(t, http.StatusBadRequest, status, "GET /contracts accepted an unknown sort key")
	assert.Contains(t, message, `invalid sort "recency"`)

	// The same endpoint does read the shared `order` parameter, which is why
	// this is a missing-parameter note rather than a missing-layer one: one
	// request mixes a handler-local enum with a shared one.
	_, sharedErr := queries.BuildEventFilter(queries.EventFilterArgs{OrderBy: "recency"})
	require.Error(t, sharedErr)
	assert.NotContains(t, message, sharedErr.Error(),
		"sort and order_by would have to be the same parameter for their messages to match")

	// Pin the accepted values too, so a future move of the contract listing
	// onto the shared layer cannot drop one unnoticed. The names come from the
	// store's own constants rather than literals here.
	for _, key := range []string{
		store.SortByContractID, store.SortByActivity, store.SortByFirstLedger,
		store.SortByLastLedger, store.SortByLastSeen,
	} {
		key := key
		t.Run("accepts "+key, func(t *testing.T) {
			status, message := requestURL(t, contracts, contracts.target+"sort="+key)
			assert.Equalf(t, http.StatusOK, status, "GET /contracts rejects its own documented sort key %q: %s", key, message)
		})
	}
}
