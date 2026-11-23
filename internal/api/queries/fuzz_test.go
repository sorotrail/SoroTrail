package queries

// Fuzz targets for the query-parameter parsers and the filter builder. They
// sit directly in front of SQL, so each target checks an invariant: a value
// that validates round-trips, rejected input yields an error rather than a
// usable zero value, and the filter builder never drops or widens a
// constraint it was given.

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

func FuzzParseTopic(f *testing.F) {
	f.Add("")
	f.Add("   ")
	f.Add("transfer")
	f.Add(`"transfer"`)
	f.Add(`{"symbol":"transfer"}`)
	f.Add(`[{"symbol":"transfer"},{"address":"CAAA"}]`)
	f.Add("123")
	f.Add("-1")
	f.Add("true")
	f.Add("null")
	f.Add("tru")
	f.Add(`{"symbol":`)
	f.Add("[1,2")
	f.Add("hello world")
	f.Add("bad\xffutf8")
	f.Add(" \t{\"a\":1}\n")

	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseTopic(raw)
		trimmed := strings.TrimSpace(raw)

		if err != nil {
			if got != nil {
				t.Fatalf("ParseTopic(%q) returned %q alongside error %v", raw, got, err)
			}
			if json.Valid([]byte(trimmed)) {
				t.Fatalf("ParseTopic(%q) rejected a valid JSON value: %v", raw, err)
			}
			return
		}

		if trimmed == "" {
			if got != nil {
				t.Fatalf("blank topic %q must mean \"no filter\", got %q", raw, got)
			}
			return
		}
		if !json.Valid(got) {
			t.Fatalf("ParseTopic(%q) = %q, which is not valid JSON and would break the SQL jsonb cast", raw, got)
		}
		if json.Valid([]byte(trimmed)) {
			if string(got) != trimmed {
				t.Fatalf("a valid JSON topic must pass through unchanged: %q became %q", trimmed, got)
			}
			return
		}
		// A bare word is quoted into a JSON string that decodes back to the
		// word (invalid UTF-8 is replaced by the encoder, so only compare
		// when the input was text).
		var word string
		if uerr := json.Unmarshal(got, &word); uerr != nil {
			t.Fatalf("bare word %q was not turned into a JSON string: %q", raw, got)
		}
		if utf8.ValidString(trimmed) && word != trimmed {
			t.Fatalf("bare word %q round-tripped to %q", trimmed, word)
		}
	})
}

func FuzzParseTopicContains(f *testing.F) {
	f.Add("")
	f.Add(`"transfer"`)
	f.Add(`{"symbol":"transfer"}`)
	f.Add(`[1,2,3]`)
	f.Add("transfer")
	f.Add("{")
	f.Add("null")
	f.Add(" 1 ")

	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseTopicContains(raw)
		switch {
		case raw == "":
			if err != nil || got != nil {
				t.Fatalf("empty input must be (nil, nil), got (%q, %v)", got, err)
			}
		case err != nil:
			if got != nil {
				t.Fatalf("ParseTopicContains(%q) returned %q alongside error %v", raw, got, err)
			}
			if json.Valid([]byte(raw)) {
				t.Fatalf("ParseTopicContains(%q) rejected a valid JSON value", raw)
			}
		default:
			if string(got) != raw {
				t.Fatalf("ParseTopicContains(%q) changed the value to %q", raw, got)
			}
			if !json.Valid(got) {
				t.Fatalf("ParseTopicContains(%q) accepted invalid JSON", raw)
			}
		}
	})
}

func FuzzParseLedgerParam(f *testing.F) {
	for _, s := range []string{"", "1", "42", "0", "-1", "+7", "007", " 5", "9223372036854775807", "9223372036854775808", "1e3", "0x10", "1_0"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		n, err := ParseLedgerParam(raw)
		if err != nil {
			if n != 0 {
				t.Fatalf("ParseLedgerParam(%q) = %d with error %v", raw, n, err)
			}
			return
		}
		if raw == "" {
			if n != 0 {
				t.Fatalf("empty input must mean \"no constraint\" (0), got %d", n)
			}
			return
		}
		if n <= 0 {
			t.Fatalf("ParseLedgerParam(%q) = %d: a non-positive ledger was accepted", raw, n)
		}
		back, perr := strconv.ParseInt(strconv.FormatInt(n, 10), 10, 64)
		if perr != nil || back != n {
			t.Fatalf("accepted ledger %d does not survive formatting", n)
		}
	})
}

func FuzzParseTimeParam(f *testing.F) {
	for _, s := range []string{
		"", "2026-07-21T00:00:00Z", "2026-07-21T00:00:00+01:00", "2026-07-21T00:00:00.5Z",
		"2026-07-21", "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "0000-01-01T00:00:00Z",
		"9999-12-31T23:59:59Z", "not a time", "2026-07-21T00:00:00Z ",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseTimeParam(raw)
		if err != nil {
			if !got.IsZero() {
				t.Fatalf("ParseTimeParam(%q) = %s with error %v", raw, got, err)
			}
			return
		}
		if raw == "" {
			if !got.IsZero() {
				t.Fatalf("empty input must mean \"no constraint\", got %s", got)
			}
			return
		}
		if got.Nanosecond() != 0 {
			t.Fatalf("ParseTimeParam(%q) kept sub-second precision", raw)
		}
		again, perr := time.Parse(time.RFC3339, got.Format(time.RFC3339))
		if perr != nil || !again.Equal(got) {
			t.Fatalf("accepted time %s does not survive formatting (%v)", got, perr)
		}
	})
}

func FuzzParseTypes(f *testing.F) {
	for _, s := range []string{"", "contract", "contract,system", " contract , diagnostic ", ",,", "contract,", "bogus", "contract,bogus", "CONTRACT"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseTypes(raw)
		if err != nil {
			if got != nil {
				t.Fatalf("ParseTypes(%q) returned %v alongside error %v", raw, got, err)
			}
			return
		}
		for _, typ := range got {
			switch typ {
			case "contract", "system", "diagnostic":
			default:
				t.Fatalf("ParseTypes(%q) accepted type %q", raw, typ)
			}
		}
		again, err := ParseTypes(strings.Join(got, ","))
		if err != nil || strings.Join(again, ",") != strings.Join(got, ",") {
			t.Fatalf("ParseTypes(%q) = %v does not round-trip: %v, %v", raw, got, again, err)
		}
	})
}

// filterViolation reports the first reason BuildEventFilter must reject args,
// or "" if they are all acceptable. It restates the documented rules
// independently of the implementation.
func filterViolation(a EventFilterArgs) string {
	switch {
	case a.ContractID != "" && !config.ValidContractID(a.ContractID):
		return "contract_id shape"
	case anyInvalidContract(a.ContractIDs):
		return "contract_ids shape"
	case a.ContractIDPrefix != "" && (a.ContractID != "" || len(a.ContractIDs) > 0):
		return "prefix combined with contract id"
	case a.Cursor != "" && !config.ValidCursor(a.Cursor):
		return "cursor"
	case a.TxIndex != nil && *a.TxIndex < 0:
		return "tx_index"
	case a.OpIndex != nil && *a.OpIndex < 0:
		return "op_index"
	case anyInvalidType(a.Types):
		return "type"
	case a.Order != "" && a.Order != "asc" && a.Order != "desc":
		return "order"
	case !store.ValidOrderBy(a.OrderBy):
		return "order_by"
	case len(a.Topic) > 0 && (len(a.T0) > 0 || len(a.T1) > 0 || len(a.T2) > 0 || len(a.T3) > 0):
		return "topic combined with positional topics"
	case a.FromLedger > 0 && a.ToLedger > 0 && a.FromLedger > a.ToLedger:
		return "ledger range"
	case !a.FromTime.IsZero() && !a.ToTime.IsZero() && a.FromTime.After(a.ToTime):
		return "time range"
	case a.Limit != 0 && (a.Limit < 1 || a.Limit > MaxPageSize):
		return "limit"
	}
	return ""
}

func anyInvalidContract(ids []string) bool {
	for _, id := range ids {
		if !config.ValidContractID(id) {
			return true
		}
	}
	return false
}

func anyInvalidType(types []string) bool {
	for _, typ := range types {
		if typ != "contract" && typ != "system" && typ != "diagnostic" {
			return true
		}
	}
	return false
}

func FuzzBuildEventFilter(f *testing.F) {
	const goodContract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// contract, second contract, prefix, type, order, order_by, cursor,
	// topic, t0, limit, from_ledger, to_ledger, from_unix, to_unix, tx_index
	f.Add(goodContract, "", "", "contract", "asc", "id", "", "", "", 10, int64(1), int64(2), int64(0), int64(0), int32(0))
	f.Add("", goodContract, "", "system", "desc", "ledger", "abc:def", `"transfer"`, "", 0, int64(0), int64(0), int64(100), int64(200), int32(1))
	f.Add("", "", "CAAA", "", "", "", "", "", "", 500, int64(5), int64(4), int64(0), int64(0), int32(-1))
	f.Add(goodContract, "", "CAAA", "diagnostic", "up", "created_at", "bad cursor", `"a"`, `"b"`, 501, int64(0), int64(0), int64(200), int64(100), int32(0))
	f.Add("not-a-contract", "", "", "bogus", "", "nope", "'; DROP TABLE events;--", "{", "[", -1, int64(-1), int64(-1), int64(-1), int64(-1), int32(0))

	f.Fuzz(func(t *testing.T, contract, second, prefix, typ, order, orderBy, cursor, topic, t0 string,
		limit int, fromLedger, toLedger, fromUnix, toUnix int64, txIndex int32) {
		args := EventFilterArgs{
			ContractID:       contract,
			ContractIDPrefix: prefix,
			Topic:            rawOrNil(topic),
			T0:               rawOrNil(t0),
			Order:            order,
			OrderBy:          orderBy,
			Cursor:           cursor,
			Limit:            limit,
			FromLedger:       fromLedger,
			ToLedger:         toLedger,
			TxIndex:          &txIndex,
		}
		if second != "" {
			args.ContractIDs = []string{second}
		}
		if typ != "" {
			args.Types = []string{typ}
		}
		if fromUnix > 0 && fromUnix < 1<<40 {
			args.FromTime = time.Unix(fromUnix, 0).UTC()
		}
		if toUnix > 0 && toUnix < 1<<40 {
			args.ToTime = time.Unix(toUnix, 0).UTC()
		}

		got, err := BuildEventFilter(args)
		reason := filterViolation(args)

		if reason != "" {
			if err == nil {
				t.Fatalf("BuildEventFilter accepted input that breaks the %s rule: %+v", reason, args)
			}
			return
		}
		if err != nil {
			t.Fatalf("BuildEventFilter rejected acceptable input (%v): %+v", err, args)
		}

		// Nothing the caller constrained may be dropped or widened.
		if got.ContractID != args.ContractID || got.ContractIDPrefix != args.ContractIDPrefix {
			t.Fatalf("contract constraint changed: %+v -> %+v", args, got)
		}
		if strings.Join(got.ContractIDs, ",") != strings.Join(args.ContractIDs, ",") {
			t.Fatalf("contract_ids changed: %v -> %v", args.ContractIDs, got.ContractIDs)
		}
		if strings.Join(got.Types, ",") != strings.Join(args.Types, ",") {
			t.Fatalf("types changed: %v -> %v", args.Types, got.Types)
		}
		if !bytes.Equal(got.Topic, args.Topic) || !bytes.Equal(got.Topic0, args.T0) {
			t.Fatalf("topic constraint changed: %+v -> %+v", args, got)
		}
		if got.Cursor != args.Cursor || got.Order != args.Order || got.OrderBy != args.OrderBy {
			t.Fatalf("paging changed: %+v -> %+v", args, got)
		}
		if got.FromLedger != args.FromLedger || got.ToLedger != args.ToLedger ||
			!got.FromTime.Equal(args.FromTime) || !got.ToTime.Equal(args.ToTime) {
			t.Fatalf("range widened or narrowed: %+v -> %+v", args, got)
		}
		if got.TxIndex == nil || *got.TxIndex != txIndex {
			t.Fatalf("tx_index dropped: %+v", got)
		}
		if got.Limit < 1 || got.Limit > MaxPageSize {
			t.Fatalf("limit %d outside [1,%d]", got.Limit, MaxPageSize)
		}
		if args.Limit != 0 && got.Limit != args.Limit {
			t.Fatalf("an explicit limit %d became %d", args.Limit, got.Limit)
		}
		if args.Limit == 0 && got.Limit != DefaultPageSize {
			t.Fatalf("an absent limit became %d, want the default %d", got.Limit, DefaultPageSize)
		}
	})
}

func rawOrNil(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	return json.RawMessage(s)
}
