package telemetry

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestVersionFrom covers every branch of the version decision.
//
// It is a table over versionFrom rather than buildVersion because
// debug.ReadBuildInfo() cannot be varied from a test: under `go test` it always
// reports "(devel)", which makes the two interesting cases -- a real tagged
// version, and a binary carrying no build info -- unreachable through
// buildVersion alone.
func TestVersionFrom(t *testing.T) {
	tests := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{
			name: "tagged release",
			info: &debug.BuildInfo{Main: debug.Module{Version: "v1.4.0"}},
			ok:   true,
			want: "v1.4.0",
		},
		{
			name: "pseudo-version from a commit",
			info: &debug.BuildInfo{Main: debug.Module{Version: "v0.0.0-20260827120000-abcdef123456"}},
			ok:   true,
			want: "v0.0.0-20260827120000-abcdef123456",
		},
		{
			name: "pre-release tag",
			info: &debug.BuildInfo{Main: debug.Module{Version: "v2.0.0-rc.1"}},
			ok:   true,
			want: "v2.0.0-rc.1",
		},
		{
			name: "go run / go test reports (devel)",
			info: &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}},
			ok:   true,
			want: "dev",
		},
		{
			name: "version present but empty",
			info: &debug.BuildInfo{Main: debug.Module{Version: ""}},
			ok:   true,
			want: "dev",
		},
		{
			name: "no build info at all",
			info: nil,
			ok:   false,
			want: "dev",
		},
		{
			name: "ok but nil info",
			info: nil,
			ok:   true,
			want: "dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, versionFrom(tt.info, tt.ok))
		})
	}
}

// TestVersionFromNeverReturnsEmpty is the property that matters downstream:
// the result becomes the service.version resource attribute on every exported
// span. An empty attribute is worse than "dev" -- a collector may drop it, and
// an operator then cannot tell which build produced the span at all.
func TestVersionFromNeverReturnsEmpty(t *testing.T) {
	cases := []struct {
		info *debug.BuildInfo
		ok   bool
	}{
		{nil, false},
		{nil, true},
		{&debug.BuildInfo{}, true},
		{&debug.BuildInfo{Main: debug.Module{Version: ""}}, true},
		{&debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true},
		{&debug.BuildInfo{Main: debug.Module{Version: "v1.0.0"}}, false},
	}

	for _, c := range cases {
		assert.NotEmpty(t, versionFrom(c.info, c.ok))
	}
}

// TestVersionFromIgnoresBuildInfoWhenNotOk pins that `ok` is honoured even when
// a caller hands over a populated struct — the version must not be read from
// build info the runtime said was unavailable.
func TestVersionFromIgnoresBuildInfoWhenNotOk(t *testing.T) {
	info := &debug.BuildInfo{Main: debug.Module{Version: "v9.9.9"}}

	assert.Equal(t, "dev", versionFrom(info, false))
}

// TestBuildVersionUnderTest ties the wrapper to the split-out decision: the
// test binary reports "(devel)", so this is the one branch reachable here, and
// asserting it proves buildVersion really delegates.
func TestBuildVersionUnderTest(t *testing.T) {
	assert.Equal(t, "dev", buildVersion())

	bi, ok := debug.ReadBuildInfo()
	assert.Equal(t, versionFrom(bi, ok), buildVersion())
}
