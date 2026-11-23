package config

// Fuzz targets for the hand-written validators that sit in front of SQL and
// the RPC client: contract IDs, pagination cursors and START_LEDGER. Each one
// checks an invariant rather than just "does not panic": the validator must
// agree with an independent definition of what it accepts, and anything it
// accepts must survive a trip through a URL query string unchanged.

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var (
	contractIDShape = regexp.MustCompile(`^C[A-Z2-7]{55}$`)
	cursorShape     = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
)

// roundTripsThroughQuery reports whether v comes back byte for byte after
// being encoded into a query string and parsed again, which is how every
// client-supplied value reaches the handlers.
func roundTripsThroughQuery(t *testing.T, v string) bool {
	t.Helper()
	parsed, err := url.ParseQuery(url.Values{"v": {v}}.Encode())
	if err != nil {
		t.Fatalf("encoded query for %q does not parse: %v", v, err)
	}
	return parsed.Get("v") == v
}

func FuzzValidContractID(f *testing.F) {
	f.Add("CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Add("CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT")
	f.Add("")
	f.Add("C")
	f.Add("GCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT") // account, not contract
	f.Add("cccc67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT")
	f.Add("CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXX1") // '1' is not base32
	f.Add("CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT\n")
	f.Add("C" + strings.Repeat("é", 27) + "A") // 56 bytes, multibyte

	f.Fuzz(func(t *testing.T, s string) {
		got := ValidContractID(s)
		if want := contractIDShape.MatchString(s); got != want {
			t.Fatalf("ValidContractID(%q) = %v, want %v", s, got, want)
		}
		if got && !roundTripsThroughQuery(t, s) {
			t.Fatalf("valid contract ID %q changes when sent in a query string", s)
		}
	})
}

func FuzzValidCursor(f *testing.F) {
	f.Add("0000000000000424242-0000000001")
	f.Add("abc:def_ghi.jkl-mno")
	f.Add("")
	f.Add(strings.Repeat("a", 128))
	f.Add(strings.Repeat("a", 129))
	f.Add("with space")
	f.Add("semi;colon")
	f.Add("quote'--")
	f.Add("a\x00b")
	f.Add("ünïcode")

	f.Fuzz(func(t *testing.T, s string) {
		got := ValidCursor(s)
		if want := cursorShape.MatchString(s); got != want {
			t.Fatalf("ValidCursor(%q) = %v, want %v", s, got, want)
		}
		if !got {
			return
		}
		if len(s) == 0 || len(s) > 128 {
			t.Fatalf("ValidCursor accepted a cursor of length %d", len(s))
		}
		if !roundTripsThroughQuery(t, s) {
			t.Fatalf("valid cursor %q changes when sent in a query string", s)
		}
	})
}

func FuzzParseStartLedger(f *testing.F) {
	f.Add("", uint32(0))
	f.Add("  ", uint32(100))
	f.Add("2", uint32(0))
	f.Add("1", uint32(0))
	f.Add("0", uint32(0))
	f.Add("4294967295", uint32(0))
	f.Add("4294967296", uint32(0))
	f.Add("latest-1000", uint32(5000))
	f.Add("LATEST-1000", uint32(5000))
	f.Add("latest-1000", uint32(10))
	f.Add("latest-1000", uint32(0))
	f.Add("latest-0", uint32(5000))
	f.Add("latest-", uint32(5000))
	f.Add("latest--5", uint32(5000))
	f.Add("-5", uint32(5000))
	f.Add("+5", uint32(5000))
	f.Add("0x10", uint32(0))
	f.Add("1_000", uint32(0))

	f.Fuzz(func(t *testing.T, raw string, latest uint32) {
		for _, args := range [][]uint32{nil, {latest}} {
			got, err := ParseStartLedger(raw, args...)
			if err != nil {
				if got != 0 {
					t.Fatalf("ParseStartLedger(%q, %v) = %d with error %v: a rejected value must not leave a usable ledger", raw, args, got, err)
				}
				continue
			}

			trimmed := strings.TrimSpace(raw)
			switch {
			case trimmed == "":
				if got != 0 {
					t.Fatalf("blank input must mean \"not set\" (0), got %d", got)
				}
			case got < 2:
				t.Fatalf("ParseStartLedger(%q, %v) = %d: a start ledger below 2 was accepted", raw, args, got)
			}

			if n, perr := strconv.ParseUint(trimmed, 10, 32); perr == nil {
				if uint64(got) != n {
					t.Fatalf("absolute ledger %q parsed as %d", trimmed, got)
				}
				continue
			}

			if trimmed == "" {
				continue
			}
			// Everything else that succeeded is a relative offset, which
			// needs a known latest ledger and may never go past it.
			if len(args) == 0 || args[0] == 0 {
				t.Fatalf("relative offset %q accepted without a latest ledger", raw)
			}
			offset, perr := strconv.ParseUint(trimmed[len("latest-"):], 10, 32)
			if perr != nil || offset == 0 {
				t.Fatalf("relative offset %q accepted but its offset is not a positive integer", raw)
			}
			want := int64(args[0]) - int64(offset)
			if want < 2 {
				want = 2
			}
			if int64(got) != want {
				t.Fatalf("ParseStartLedger(%q, %d) = %d, want %d", raw, args[0], got, want)
			}
		}
	})
}
