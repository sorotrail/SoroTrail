// Package sorotrail_test covers the Helm chart's rendered output (issue
// #832). CI lints the chart and renders it once, but nothing asserted
// what the rendered manifests actually contain, so a template change
// could silently drop a probe or a resource limit.
//
// These tests render the chart with `helm template` and compare the
// output byte-for-byte against committed golden manifests under
// testdata/, plus a set of structural invariants (probes pointing at the
// real endpoints, DATABASE_URL coming from a Secret, resources present
// when configured). Any template change that alters the output fails the
// golden comparison — regenerating the goldens is a documented one-liner
// (see the TestRenderGolden_* doc comments).
//
// The tests skip when the helm binary is unavailable, so `go test ./...`
// stays green on machines without Helm; CI's runner ships Helm and runs
// them for real.
package sorotrail_test

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "regenerate golden files instead of comparing")

// chartDir resolves the chart directory relative to this test file, so
// the tests run from any working directory.
func chartDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

// helmPath returns the helm binary, skipping the test when it is not
// installed.
func helmPath(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not installed; install Helm (https://helm.sh) to run the chart golden tests")
	}
	return p
}

// renderChart runs `helm template` with the given extra args and returns
// the rendered manifests. The database.url value is required by the
// chart (secret.yaml), so every render passes it unless the test
// overrides it via existingSecret.
func renderChart(t *testing.T, helm string, extraArgs ...string) string {
	t.Helper()
	args := append([]string{
		"template", "sorotrail", chartDir(),
		"--namespace", "sorotrail-test",
		"--set", "database.url=postgres://user:pass@host:5432/db",
	}, extraArgs...)
	out, err := exec.Command(helm, args...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	return string(out)
}

// TestRenderGolden_DefaultValues pins the full render of the chart with
// the defaults from values.yaml (plus the required database.url). This
// is the "a template change silently drops something" guard: any drift
// in the rendered manifests fails the byte-for-byte comparison.
//
// Regenerate after an intentional template change with:
//
//	go test ./deploy/helm/sorotrail -run TestRenderGolden -update
func TestRenderGolden_DefaultValues(t *testing.T) {
	helm := helmPath(t)
	got := renderChart(t, helm)
	golden := filepath.Join(chartDir(), "testdata", "default.golden.yaml")
	assertGolden(t, golden, got)
}

// TestRenderGolden_RepresentativeOverrides pins a render with a
// representative override set: resource requests/limits, an existing
// database Secret (so the chart must NOT create its own and must not
// inline the URL), a ServiceMonitor, and a non-default ServiceAccount
// name. Each override exercises a different template branch.
//
// Regenerate with:
//
//	go test ./deploy/helm/sorotrail -run TestRenderGolden -update
func TestRenderGolden_RepresentativeOverrides(t *testing.T) {
	helm := helmPath(t)
	got := renderChart(t, helm,
		"--values", filepath.Join(chartDir(), "testdata", "overrides.yaml"),
	)
	golden := filepath.Join(chartDir(), "testdata", "overrides.golden.yaml")
	assertGolden(t, golden, got)
}

func assertGolden(t *testing.T, golden, got string) {
	t.Helper()
	if *update {
		require.NoError(t, os.WriteFile(golden, []byte(got), 0o644))
		t.Logf("updated golden file %s", golden)
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("reading golden file %s: %v (run with -update to create it)", golden, err)
	}
	assert.Equal(t, string(want), got,
		"rendered manifests drifted from %s; if the change is intentional, regenerate with:\n\tgo test ./deploy/helm/sorotrail -run TestRenderGolden -update", golden)
}

// TestRender_ManifestInvariants asserts the structural properties the
// operators rely on, independently of the goldens so a drift is reported
// with a precise message rather than a whole-file diff:
//
//   - probes point at the real endpoints (/health and /readyz)
//   - DATABASE_URL is mounted from a Secret, never inlined in the pod
//   - resource requests and limits land when configured
//   - the chart creates a Secret only when no existingSecret is given
func TestRender_ManifestInvariants(t *testing.T) {
	helm := helmPath(t)

	// Default render: chart-managed Secret, no resources.
	rendered := renderChart(t, helm)
	deployment := manifest(t, rendered, "Deployment")

	t.Run("probes hit the real endpoints", func(t *testing.T) {
		assert.Contains(t, deployment, "livenessProbe:", "liveness probe must exist")
		assert.Contains(t, deployment, "readinessProbe:", "readiness probe must exist")
		// The probes must point at the documented health endpoints, not
		// at a path the app never serves — a renamed route would make
		// every pod perpetually unhealthy without failing a deploy.
		assert.Contains(t, deployment, `path: /health`, "liveness probe must hit /health")
		assert.Contains(t, deployment, `path: /readyz`, "readiness probe must hit /readyz")
	})

	t.Run("database url comes from a secret, never inlined", func(t *testing.T) {
		assert.Contains(t, deployment, "secretKeyRef:", "DATABASE_URL must come from a Secret")
		assert.Contains(t, deployment, "DATABASE_URL", "DATABASE_URL env must be declared")
		assert.NotContains(t, deployment, "postgres://user:pass@host:5432/db",
			"the connection string must never be inlined into the pod spec")
		assert.NotContains(t, deployment, "value: \"postgres://",
			"no env var may carry the connection string as a plain value")
		assert.Contains(t, manifest(t, rendered, "Secret"), "stringData:",
			"default render must create the chart-managed Secret")
	})

	t.Run("resources land when configured", func(t *testing.T) {
		withResources := renderChart(t, helm,
			"--values", filepath.Join(chartDir(), "testdata", "overrides.yaml"),
		)
		dep := manifest(t, withResources, "Deployment")
		assert.Contains(t, dep, "requests:", "resource requests must render when configured")
		assert.Contains(t, dep, "limits:", "resource limits must render when configured")
		assert.Contains(t, dep, "cpu:", "CPU request/limit must render when configured")
		assert.Contains(t, dep, "memory:", "memory request/limit must render when configured")
		// The overrides file names an existing Secret: the chart must not
		// synthesize one, and the deployment must reference it by name.
		assert.NotContains(t, withResources, "kind: Secret",
			"an existingSecret must suppress the chart-managed Secret")
		assert.Contains(t, dep, "existing-db-secret", "deployment must reference the existing Secret")
	})

	t.Run("serviceMonitor renders when enabled", func(t *testing.T) {
		withMonitor := renderChart(t, helm,
			"--values", filepath.Join(chartDir(), "testdata", "overrides.yaml"),
		)
		sm := manifest(t, withMonitor, "ServiceMonitor")
		assert.Contains(t, sm, "path: /metrics", "ServiceMonitor must scrape /metrics")
		assert.Contains(t, sm, "interval: 15s", "ServiceMonitor must carry the configured interval")
	})

}

// manifest extracts the first block for the given Kind from a rendered
// chart output (manifests are separated by "---" lines).
func manifest(t *testing.T, rendered, kind string) string {
	t.Helper()
	for _, block := range strings.Split(rendered, "\n---\n") {
		if strings.Contains(block, "kind: "+kind) {
			return block
		}
	}
	t.Fatalf("rendered output contains no %s manifest", kind)
	return ""
}
