package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompletionScriptShells verifies each supported shell emits a
// non-empty script that mentions every known subcommand.
func TestCompletionScriptShells(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		shell string
		want  []string
	}{
		{
			name:  "bash",
			shell: "bash",
			want:  []string{"complete -F _sorotrail", "sorotrail"},
		},
		{
			name:  "zsh",
			shell: "zsh",
			want:  []string{"#compdef sorotrail", "_describe"},
		},
		{
			name:  "fish",
			shell: "fish",
			want:  []string{"complete -c sorotrail", "__fish_use_subcommand"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			script, err := completionScript(tt.shell)
			require.NoError(t, err)
			assert.NotEmpty(t, script)
			for _, want := range tt.want {
				assert.Contains(t, script, want)
			}
			// Every dispatch subcommand must be completable.
			for _, name := range completionNames() {
				assert.Contains(t, script, name,
					"script for %s missing subcommand %q", tt.shell, name)
			}
		})
	}
}

// TestCompletionScriptUnsupportedShell verifies unknown shells produce
// an error naming the supported set rather than an empty script.
func TestCompletionScriptUnsupportedShell(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		shell string
	}{
		{name: "unknown", shell: "powershell"},
		{name: "empty", shell: ""},
		{name: "bash alias", shell: "sh"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := completionScript(tt.shell)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "bash, zsh, or fish")
		})
	}
}

// TestRunCompletionTo covers the argument handling of the completion
// subcommand itself: valid shell prints a script, bad invocations
// return errors, and --help returns nil (usage already printed).
func TestRunCompletionTo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		wantOut string
	}{
		{name: "bash", args: []string{"bash"}, wantOut: "# bash completion for sorotrail"},
		{name: "zsh", args: []string{"zsh"}, wantOut: "#compdef sorotrail"},
		{name: "fish", args: []string{"fish"}, wantOut: "# fish completion for sorotrail"},
		{name: "missing arg", args: nil, wantErr: true},
		{name: "too many args", args: []string{"bash", "zsh"}, wantErr: true},
		{name: "unknown shell", args: []string{"ksh"}, wantErr: true},
		{name: "help short", args: []string{"-h"}, wantErr: false},
		{name: "help long", args: []string{"--help"}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			err := runCompletionTo(&buf, tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.wantOut != "" {
				assert.Contains(t, buf.String(), tt.wantOut)
			}
		})
	}
}

// TestCompletionMatchesDispatch guards the sync between the completion
// command list and the dispatch switch: any subcommand dispatch accepts
// must be listed for completion, and completion must not advertise a
// subcommand dispatch doesn't know.
func TestCompletionMatchesDispatch(t *testing.T) {
	t.Parallel()
	dispatched := []string{
		"replay", "apikey", "contracts", "backfill", "index-addresses",
		"migrate", "healthcheck", "schema-inspect", "migrate-status",
		"completion", "version", "config", "preflight", "help",
	}
	completion := completionNames()
	assert.ElementsMatch(t, dispatched, completion,
		"completionCommands must mirror the subcommands dispatch() routes")
}

// TestCompletionScriptSyntax validates that the generated completion scripts
// parse without syntax errors in their target shells. If a shell is not
// installed on the host running the test, the test case skips visibly.
func TestCompletionScriptSyntax(t *testing.T) {
	shells := []struct {
		shell string
		bin   string
		flag  string
	}{
		{shell: "bash", bin: "bash", flag: "-n"},
		{shell: "zsh", bin: "zsh", flag: "-n"},
		{shell: "fish", bin: "fish", flag: "-n"},
	}

	for _, tt := range shells {
		t.Run(tt.shell, func(t *testing.T) {
			path, err := exec.LookPath(tt.bin)
			if err != nil {
				t.Skipf("shell %s not installed; skipping syntax validation", tt.bin)
			}

			script, err := completionScript(tt.shell)
			require.NoError(t, err)
			require.NotEmpty(t, script)

			cmd := exec.Command(path, tt.flag)
			cmd.Stdin = strings.NewReader(script)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "syntax check failed for %s completion script: %s", tt.shell, string(out))
		})
	}
}

// TestRunCompletionDirect asserts that runCompletion functions as the
// entrypoint writing directly to stdout and correctly reporting errors.
func TestRunCompletionDirect(t *testing.T) {
	// 1. Valid invocation capturing stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w

	runErr := runCompletion([]string{"bash"})
	w.Close()
	os.Stdout = oldStdout

	require.NoError(t, runErr)
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	assert.Contains(t, buf.String(), "complete -F _sorotrail sorotrail")

	// 2. Unsupported shell error through runCompletion
	err = runCompletion([]string{"unsupported-shell"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bash, zsh, or fish")

	// 3. Help invocation returns nil
	err = runCompletion([]string{"--help"})
	require.NoError(t, err)
}
