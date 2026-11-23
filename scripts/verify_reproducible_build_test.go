package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repoRootDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod found above %s", dir)
		dir = parent
	}
}

// buildSyntheticRepo creates a minimal git repository with the same module path,
// internal/buildinfo package, and `cmd/sorotrail version` subcommand so the
// reproducibility script can be exercised end-to-end (including ldflag stamping
// and `sorotrail version` output verification) in milliseconds.
func buildSyntheticRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "internal", "buildinfo"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "cmd", "sorotrail"), 0o755))

	goMod := "module github.com/sorotrail/sorotrail\n\ngo 1.23\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644))

	buildinfoSrc := `package buildinfo

var (
	Version   = "unknown"
	Commit    = "unknown"
	BuildDate = "unknown"
)
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "internal", "buildinfo", "buildinfo.go"), []byte(buildinfoSrc), 0o644))

	mainSrc := `package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/sorotrail/sorotrail/internal/buildinfo"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		_, file, _, _ := runtime.Caller(0)
		fmt.Printf("version\t%s\ncommit\t%s\nbuild-date\t%s\nsource\t%s\n",
			buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate, file)
	}
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cmd", "sorotrail", "main.go"), []byte(mainSrc), 0o644))

	gitCmds := [][]string{
		{"git", "init"},
		{"git", "config", "user.name", "test"},
		{"git", "config", "user.email", "test@example.com"},
		{"git", "add", "."},
		{"git", "commit", "-m", "initial commit"},
		{"git", "tag", "v0.1.0"},
	}
	for _, args := range gitCmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "running %v: %s", args, string(out))
	}

	return dir
}

func runReproScript(t *testing.T, sourceDir string, args ...string) (int, string) {
	t.Helper()
	requireScopeBash(t)

	root := repoRootDir(t)
	script := filepath.Join(root, "scripts", "verify_reproducible_build.sh")
	fullArgs := append([]string{script, "--source", sourceDir}, args...)
	cmd := exec.Command("bash", fullArgs...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "running verify_reproducible_build.sh: %v\n%s", err, string(out))
	return exitErr.ExitCode(), string(out)
}

// TestBuildConfigsPinReproducibilityInvariants verifies that Makefile,
// .goreleaser.yaml, Dockerfiles, workflows, and RELEASING.md do not regress
// back to wall-clock timestamps or untrimmed build paths.
func TestBuildConfigsPinReproducibilityInvariants(t *testing.T) {
	t.Parallel()
	root := repoRootDir(t)

	makefileBytes, err := os.ReadFile(filepath.Join(root, "Makefile"))
	require.NoError(t, err)
	makefile := string(makefileBytes)
	assert.Contains(t, makefile, "git show -s --format=%cd",
		"Makefile must derive BUILD_DATE from the commit timestamp rather than the wall clock")
	assert.Contains(t, makefile, "-trimpath",
		"Makefile must pass -trimpath so checkout paths do not leak into the binary")
	assert.Contains(t, makefile, "-buildvcs=false",
		"Makefile must pass -buildvcs=false so worktree VCS metadata cannot vary between builds")
	assert.Contains(t, makefile, "reproducible-check:",
		"Makefile must expose a reproducible-check target")

	goreleaserBytes, err := os.ReadFile(filepath.Join(root, ".goreleaser.yaml"))
	require.NoError(t, err)
	goreleaser := string(goreleaserBytes)
	assert.Contains(t, goreleaser, "BuildDate={{ .CommitDate }}",
		".goreleaser.yaml must stamp BuildDate from {{ .CommitDate }} instead of wall-clock {{ .Date }}")
	assert.NotContains(t, goreleaser, "BuildDate={{ .Date }}",
		".goreleaser.yaml must not stamp BuildDate from wall-clock {{ .Date }}")
	assert.Contains(t, goreleaser, "- -trimpath",
		".goreleaser.yaml must enable -trimpath")
	assert.Contains(t, goreleaser, "- -buildvcs=false",
		".goreleaser.yaml must disable automatic VCS stamping")

	for _, df := range []string{"Dockerfile", "Dockerfile.sorotrail"} {
		data, err := os.ReadFile(filepath.Join(root, df))
		require.NoError(t, err)
		text := string(data)
		assert.Contains(t, text, "-trimpath", "%s must build with -trimpath", df)
		assert.Contains(t, text, "-buildvcs=false", "%s must build with -buildvcs=false", df)
		assert.Contains(t, text, "@sha256:", "%s must pin base images by digest", df)
	}

	for _, wf := range []string{ciWorkflowPath, releaseWorkflowPath} {
		workflow := loadWorkflow(t, wf)
		job, ok := workflow.Jobs[goreleaserJobName]
		require.True(t, ok, "%s must define a %q job", wf, goreleaserJobName)
		found := false
		for _, step := range job.Steps {
			if strings.Contains(step.Run, "verify_reproducible_build.sh") {
				found = true
				break
			}
		}
		assert.True(t, found, "%s (%s job) must run verify_reproducible_build.sh", wf, goreleaserJobName)
	}

	releasingBytes, err := os.ReadFile(filepath.Join(root, "RELEASING.md"))
	require.NoError(t, err)
	releasing := string(releasingBytes)
	assert.Contains(t, releasing, "Independently reproducing a release binary",
		"RELEASING.md must document how a third party reproduces and compares a release binary")
	assert.Contains(t, releasing, "make reproducible-check",
		"RELEASING.md must reference make reproducible-check")
	assert.Contains(t, releasing, "Container image reproducibility scope",
		"RELEASING.md must state the container image reproducibility scope and limitations")
}

// TestVerifyReproducibleBuildScript_TwoEnvironmentsProduceIdenticalBytes builds
// the same commit twice in independent working directories at different path
// depths, with different HOSTNAME, USER, HOME, GOTMPDIR, and TZ values, and
// verifies the resulting binaries are byte-for-byte identical and retain valid
// version/commit/build-date stamps.
func TestVerifyReproducibleBuildScript_TwoEnvironmentsProduceIdenticalBytes(t *testing.T) {
	sourceDir := buildSyntheticRepo(t)
	code, out := runReproScript(t, sourceDir)
	require.Equal(t, 0, code, "expected reproducible build to succeed, got:\n%s", out)
	assert.Contains(t, out, "[PASS] reproducible build verified:")
	assert.Contains(t, out, "version=v0.1.0")
	assert.Contains(t, out, "sha256=")
}

// TestVerifyReproducibleBuildScript_DetectsAndExplainsDivergence proves the
// gate fails loudly and pinpoints where the binaries differ when a timestamp,
// hostname, or untrimmed path leaks into one of the builds.
func TestVerifyReproducibleBuildScript_DetectsAndExplainsDivergence(t *testing.T) {
	sourceDir := buildSyntheticRepo(t)

	tests := []struct {
		mode        string
		wantInError string
	}{
		{
			mode:        "timestamp",
			wantInError: "2099-12-31T23:59:59Z",
		},
		{
			mode:        "hostname",
			wantInError: "host-bravo",
		},
		{
			mode:        "untrimmed-path",
			wantInError: "Go buildid comparison",
		},
	}

	for _, tc := range tests {
		t.Run(tc.mode, func(t *testing.T) {
			code, out := runReproScript(t, sourceDir, "--inject-nondeterminism", tc.mode)
			require.Equal(t, 1, code, "mode %s must fail reproducibility verification:\n%s", tc.mode, out)
			assert.Contains(t, out, "[FAIL] reproducible build mismatch")
			assert.Contains(t, out, "Go buildid comparison")
			assert.Contains(t, out, "First 20 differing byte offsets")
			assert.Contains(t, out, tc.wantInError)
		})
	}
}
