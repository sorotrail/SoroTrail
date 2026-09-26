package decode

// Golden-file coverage for the ScVal decoder. The input file contains
// representative, literal base64 XDR values; the matching golden file pins
// the JSON shape emitted for each value. Keeping both sides in testdata
// makes a wire-format or decoder-output change visible in a focused diff.
//
// Regenerate the expected JSON after an intentional decoder change with:
//
//	go test ./internal/decode -run TestXDRDecoder_GoldenFixtures -update-golden
//
// Review the resulting changes before committing them.

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var updateDecoderGolden = flag.Bool("update-golden", false, "rewrite ScVal decoder golden outputs")

type scvalFixture struct {
	Name string `json:"name"`
	XDR  string `json:"xdr"`
}

type scvalGolden map[string]json.RawMessage

func TestXDRDecoder_GoldenFixtures(t *testing.T) {
	fixtures := loadScvalFixtures(t)
	require.NotEmpty(t, fixtures, "decoder fixture file must not be empty")

	actual := make(scvalGolden, len(fixtures))
	seen := make(map[string]struct{}, len(fixtures))
	seenTypes := make(map[xdr.ScValType]struct{}, len(fixtures))
	for _, fixture := range fixtures {
		require.NotEmpty(t, fixture.Name, "fixture name must not be empty")
		require.NotEmpty(t, fixture.XDR, "fixture %q must contain an XDR blob", fixture.Name)
		_, duplicate := seen[fixture.Name]
		require.False(t, duplicate, "duplicate decoder fixture %q", fixture.Name)
		seen[fixture.Name] = struct{}{}

		// Validate the input independently of XDRDecoder. DecodeScVal
		// intentionally turns malformed input into a nil-error fallback, so
		// without this check a typo in the input manifest could be blessed
		// by the golden-update mode.
		var value xdr.ScVal
		require.NoErrorf(t, xdr.SafeUnmarshalBase64(fixture.XDR, &value), "fixture %q is not valid ScVal XDR", fixture.Name)
		canonical, err := xdr.MarshalBase64(value)
		require.NoErrorf(t, err, "re-encoding fixture %q", fixture.Name)
		require.Equalf(t, fixture.XDR, canonical, "fixture %q is not canonical base64 XDR", fixture.Name)
		seenTypes[value.Type] = struct{}{}

		got, err := (XDRDecoder{}).DecodeScVal(fixture.XDR)
		require.NoErrorf(t, err, "decoding fixture %q", fixture.Name)
		actual[fixture.Name] = append(json.RawMessage(nil), got...)
	}
	requireScvalFixtureTypeCoverage(t, seenTypes)

	if *updateDecoderGolden {
		writeScvalGolden(t, actual)
		return
	}

	golden := loadScvalGolden(t)
	require.Len(t, golden, len(fixtures), "every fixture must have exactly one golden output")
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			want, ok := golden[fixture.Name]
			require.Truef(t, ok, "missing golden output for %q", fixture.Name)
			got := actual[fixture.Name]
			assert.Equalf(t, decodeJSONForComparison(t, want), decodeJSONForComparison(t, got), "decoded shape drifted for fixture %q", fixture.Name)
		})
	}
	for name := range golden {
		_, ok := seen[name]
		assert.Truef(t, ok, "golden output %q has no matching fixture", name)
	}
}

func requireScvalFixtureTypeCoverage(t *testing.T, seen map[xdr.ScValType]struct{}) {
	t.Helper()
	// ScValType is currently a contiguous enum from bool through executable
	// tag. Requiring every member keeps the fixture set from silently
	// dropping a newly handled SDK variant; the ledger-key-contract-instance
	// value is intentionally represented by the unknown-output fixture.
	for typ := xdr.ScValType(0); typ <= xdr.ScValTypeScvExecutableTag; typ++ {
		_, ok := seen[typ]
		require.Truef(t, ok, "missing fixture for ScVal type %s", typ)
	}
	require.Lenf(t, seen, int(xdr.ScValTypeScvExecutableTag)+1, "unexpected number of ScVal fixture types")
}

func decodeJSONForComparison(t *testing.T, data []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	require.NoError(t, decoder.Decode(&value), "golden output is not valid JSON: %s", data)
	return value
}

func loadScvalFixtures(t *testing.T) []scvalFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "scval_fixtures.json"))
	require.NoError(t, err, "reading ScVal fixtures")
	var fixtures []scvalFixture
	require.NoError(t, json.Unmarshal(data, &fixtures), "parsing ScVal fixtures")
	return fixtures
}

func loadScvalGolden(t *testing.T) scvalGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "golden", "scval_decoded.json"))
	require.NoError(t, err, "reading ScVal golden outputs")
	var golden scvalGolden
	require.NoError(t, json.Unmarshal(data, &golden), "parsing ScVal golden outputs")
	return golden
}

func writeScvalGolden(t *testing.T, golden scvalGolden) {
	t.Helper()
	data, err := json.MarshalIndent(golden, "", "  ")
	require.NoError(t, err, "marshaling ScVal golden outputs")
	data = append(data, '\n')
	path := filepath.Join("testdata", "golden", "scval_decoded.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o644))
	t.Logf("updated ScVal decoder golden file %s", path)
}
