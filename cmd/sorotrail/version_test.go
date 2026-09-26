package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFormatVersion verifies the version report layout: one value per
// line, clearly labelled, with a trailing newline so scripts can rely on
// a complete last line.
func TestFormatVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		info versionInfo
		want string
	}{
		{
			name: "fully populated build info",
			info: versionInfo{Version: "v0.1.0", Commit: "3700e2e", BuildDate: "2026-09-24T10:00:00Z"},
			want: "version\tv0.1.0\ncommit\t3700e2e\nbuild-date\t2026-09-24T10:00:00Z\n",
		},
		{
			name: "unknown defaults from a plain build",
			info: versionInfo{Version: "unknown", Commit: "unknown", BuildDate: "unknown"},
			want: "version\tunknown\ncommit\tunknown\nbuild-date\tunknown\n",
		},
		{
			name: "unset fields produce empty labels",
			info: versionInfo{},
			want: "version\t\ncommit\t\nbuild-date\t\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, formatVersion(tt.info))
		})
	}
}

// TestRunVersionTo covers the argument handling of the version
// subcommand: the default call prints every field, --help returns nil
// (usage already printed), and an unexpected positional argument is
// rejected.
func TestRunVersionTo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		args        []string
		wantErr     bool
		wantContent []string
	}{
		{
			name:        "prints all three fields",
			wantContent: []string{"version\t", "commit\t", "build-date\t"},
		},
		{
			name:        "help short",
			args:        []string{"-h"},
			wantErr:     false,
			wantContent: nil,
		},
		{
			name:    "help long",
			args:    []string{"--help"},
			wantErr: false,
		},
		{
			name:    "unexpected positional argument",
			args:    []string{"extra"},
			wantErr: true,
		},
		{
			name:    "multiple unexpected arguments",
			args:    []string{"one", "two"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			err := runVersionTo(&buf, tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			for _, want := range tt.wantContent {
				assert.Contains(t, buf.String(), want)
			}
		})
	}
}

// TestVersionOutputReferencesBuildInfo ensures the report actually reads
// from internal/buildinfo, so ldflags-injected values (Makefile,
// Dockerfile, GoReleaser) surface on `sorotrail version`.
func TestVersionOutputReferencesBuildInfo(t *testing.T) {
	t.Parallel()
	got := currentVersion()
	assert.Contains(t, formatVersion(got), got.Commit)
	assert.Contains(t, formatVersion(got), got.BuildDate)
}

// TestDispatchVersionRoute guards that both the version subcommand and
// the top-level --version / -V flags dispatch to the version printer
// without an error (so main exits 0).
func TestDispatchVersionRoute(t *testing.T) {
	// Not parallel: this test temporarily redirects the process-wide
	// os.Stdout, which would race with other parallel tests.
	tests := []struct {
		name string
		args []string
	}{
		{name: "subcommand", args: []string{"version"}},
		{name: "long flag", args: []string{"--version"}},
		{name: "short flag", args: []string{"-V"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// dispatch reads the global buildinfo vars; capturing stdout
			// here asserts the route resolves and prints the report.
			old := os.Stdout
			r, w, err := os.Pipe()
			require.NoError(t, err)
			os.Stdout = w
			derr := dispatch(tt.args)
			closeErr := w.Close()
			os.Stdout = old
			require.NoError(t, closeErr)
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r)
			assert.NoError(t, derr)
			assert.Contains(t, buf.String(), "version\t")
			assert.True(t, strings.HasSuffix(buf.String(), "\n"),
				"output must end with a newline")
		})
	}
}
