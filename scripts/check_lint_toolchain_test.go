package scripts_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guardResult captures the exit code and combined output of one guard run.
type guardResult struct {
	code   int
	output string
}

// runGuard executes scripts/check_lint_toolchain.sh with --root pointed at a
// fixture directory, so the assertions never depend on the repository's own
// go.mod or its pinned linter.
func runGuard(t *testing.T, root string, args ...string) guardResult {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)
	script := filepath.Join(wd, "check_lint_toolchain.sh")

	cmdArgs := append([]string{script, "--root", root}, args...)
	out, err := exec.Command("bash", cmdArgs...).CombinedOutput()

	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("running guard: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return guardResult{code: code, output: string(out)}
}

// writeFixture creates a throwaway "repository" holding just the two files the
// guard reads. An empty pin means the pin file is deliberately absent.
func writeFixture(t *testing.T, gomod, pin string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644))
	if pin != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".golangci-lint-version"), []byte(pin), 0o644))
	}
	return dir
}

func requireBash(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("the guard is a bash script; CI runs it on Linux")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
}

func TestCheckLintToolchain_Compatibility(t *testing.T) {
	requireBash(t)

	const pin = "v2.12.2 go1.26.2\n"

	tests := []struct {
		name       string
		gomod      string
		wantCode   int
		wantOutput string
	}{
		{
			name:       "go directive at the linter's ceiling",
			gomod:      "module example.com/x\n\ngo 1.26.0\n",
			wantCode:   0,
			wantOutput: "[PASS]",
		},
		{
			name:       "older go directive",
			gomod:      "module example.com/x\n\ngo 1.25.0\n",
			wantCode:   0,
			wantOutput: "[PASS]",
		},
		{
			name:       "toolchain within the linter's minor",
			gomod:      "module example.com/x\n\ngo 1.25.0\n\ntoolchain go1.26.5\n",
			wantCode:   0,
			wantOutput: "[PASS]",
		},
		{
			// The trap from the issue: the go directive is untouched, yet the
			// toolchain line alone makes the pinned linter refuse to start.
			name:       "toolchain directive past the linter's ceiling",
			gomod:      "module example.com/x\n\ngo 1.25.0\n\ntoolchain go1.27.1\n",
			wantCode:   1,
			wantOutput: "toolchain go1.27.1",
		},
		{
			name:       "go directive past the linter's ceiling",
			gomod:      "module example.com/x\n\ngo 1.27.1\n",
			wantCode:   1,
			wantOutput: "targets Go 1.27",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeFixture(t, tt.gomod, pin)
			got := runGuard(t, dir)

			assert.Equal(t, tt.wantCode, got.code, "output:\n%s", got.output)
			assert.Contains(t, got.output, tt.wantOutput)
		})
	}
}

func TestCheckLintToolchain_MalformedPin(t *testing.T) {
	requireBash(t)

	tests := []struct {
		name       string
		pin        string
		wantOutput string
	}{
		{name: "missing pin file", pin: "", wantOutput: "is missing"},
		{name: "pin without a Go version", pin: "v2.12.2\n", wantOutput: "must hold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeFixture(t, "module example.com/x\n\ngo 1.25.0\n", tt.pin)
			got := runGuard(t, dir)

			assert.Equal(t, 1, got.code, "output:\n%s", got.output)
			assert.Contains(t, got.output, tt.wantOutput)
		})
	}
}

func TestCheckLintToolchain_PrintVersion(t *testing.T) {
	requireBash(t)

	dir := writeFixture(t, "module example.com/x\n\ngo 1.25.0\n", "v2.12.2 go1.26.2\n")
	got := runGuard(t, dir, "--print-version")

	require.Equal(t, 0, got.code, "output:\n%s", got.output)
	assert.Equal(t, "v2.12.2\n", got.output)
}
