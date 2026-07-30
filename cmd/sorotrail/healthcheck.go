package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// healthcheckTimeoutDefault is the per-probe HTTP timeout. Sized so a
// transient connection-pool drain on startup or a slow RPC health
// call (the issue's "sleep-and-hope" failure mode the docker
// HEALTHCHECK exists to catch) comfortably fits inside the timeout
// without letting a stuck process hold the probe forever.
//
// 3s is also below the typical 5s docker HEALTHCHECK timeout, so
// even with a custom --timeout that exceeds ours the kernel still
// gets a clean exit.
const healthcheckTimeoutDefault = 3 * time.Second

// healthcheckAddrDefault is the fallback probe target when no
// --addr flag is passed and HTTP_ADDR is unset. Matches the bind
// address the published Dockerfile / docker-compose.yml expose.
//
// We deliberately bind to 127.0.0.1 (not 0.0.0.0) — the probe is
// running inside the same container as the server, so it must
// never depend on a network-accessible interface.
const healthcheckAddrDefault = "127.0.0.1:8080"

// healthcheckEndpointDefault is the default HTTP path the probe
// hits. /health is the same endpoint the Helm chart's
// liveness/readiness probes target, so docker HEALTHCHECK,
// docker compose, and k8s probes all examine the same signal.
const healthcheckEndpointDefault = "/health"

// healthcodeExitUsage is returned by runHealthcheck when flag
// parsing fails or required arguments are missing. main.go maps 2
// to os.Exit(2).
const healthcodeExitUsage = 2

// runHealthcheck implements `sorotrail healthcheck`: GET the
// configured endpoint and exit 0 if it returns 2xx, 1 otherwise.
//
// The subcommand exists so the docker HEALTHCHECK directive (and
// anything else that can't ship curl/wget inside the container)
// has a probe binary it can call. The indexer binary is
// statically linked with CGO_ENABLED=0, so calling it as a
// subprocess inherits the same container filesystem and runs
// without any extra dependencies — there's nothing to install
// and nothing else to ship in the image.
//
// Exit codes:
//
//	0  the endpoint returned 2xx — the indexer is healthy
//	1  the endpoint returned non-2xx, the probe timed out, or a
//	   network error prevented reaching the endpoint
//	2  flag/usage error (subcommand invoked with bad arguments)
//
// Exit codes 0 and 1 deliberately take different paths through
// main.go's switch so docker's HEALTHCHECK CMD mapping sees
// healthy / unhealthy without main's "sorotrail: %s" prefix
// noise polluting the stderr that `docker inspect` surfaces.
func runHealthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail healthcheck [flags]

Probes the indexer's HTTP /health endpoint and exits 0 on a 2xx
response, 1 on any failure (non-2xx, network error, timeout). Used
as the docker HEALTHCHECK probe so the container has a real health
signal instead of "the process is running", and so docker compose
can gate dependent services on actual readiness instead of a sleep.

flags:
`)
		fs.PrintDefaults()
	}
	addrFlag := fs.String("addr", "",
		"host:port to probe (defaults to $HTTP_ADDR or 127.0.0.1:8080)")
	endpoint := fs.String("endpoint", healthcheckEndpointDefault,
		"URL path probed on the indexer (e.g. /health, /livez)")
	timeout := fs.Duration("timeout", healthcheckTimeoutDefault,
		"HTTP client timeout for the probe")
	if err := fs.Parse(args); err != nil {
		// flag.ContinueOnError already printed the usage to stderr.
		return healthcodeExitUsage
	}
	if *endpoint == "" || !strings.HasPrefix(*endpoint, "/") {
		fmt.Fprintln(fs.Output(),
			"healthcheck: --endpoint must be a path beginning with '/'")
		return healthcodeExitUsage
	}
	if *timeout <= 0 {
		fmt.Fprintln(fs.Output(),
			"healthcheck: --timeout must be positive")
		return healthcodeExitUsage
	}

	target := resolveHealthcheckAddr(*addrFlag)
	url := "http://" + target + *endpoint

	client := &http.Client{Timeout: *timeout}
	// We deliberately don't follow redirects: /health is a fixed
	// local path, and a 3xx would mean the indexer is misconfigured,
	// not "the answer is elsewhere". Following a redirect could mask
	// a real misconfiguration behind a successful probe.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		// Only malformed URLs hit this path; we constructed url
		// ourselves, so it's effectively unreachable in practice,
		// but we still surface the error cleanly.
		fmt.Fprintf(os.Stderr, "healthcheck: build request: %v\n", err)
		return 1
	}
	req.Header.Set("User-Agent", "sorotrail-healthcheck/1")

	resp, err := client.Do(req)
	if err != nil {
		// Connection refused (server not listening yet), timeout
		// (probe hung), DNS failure (broken --addr). All are
		// "not healthy right now" — same exit code, terse message
		// so `docker inspect` shows a useful one-liner without an
		// avalanche of stack frames.
		fmt.Fprintf(os.Stderr, "healthcheck: probe %s failed: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	// Drain a bounded prefix so the connection can be reused by
	// the keep-alive pool. /health responses are tiny (a small
	// JSON envelope) but we cap the read rather than reading
	// until EOF, so a malicious or misbehaving server can't pin
	// the process open after we've already decided the probe's
	// outcome from the status line.
	_, _ = io.CopyN(io.Discard, resp.Body, 4096)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr,
			"healthcheck: probe %s returned status %d\n",
			url, resp.StatusCode)
		return 1
	}
	return 0
}

// resolveHealthcheckAddr picks the probe target from (in order):
//
//  1. the --addr flag, if non-empty
//  2. the HTTP_ADDR environment variable, if non-empty
//  3. healthcheckAddrDefault (127.0.0.1:8080)
//
// A bare ":PORT" form (the curl of HTTP_ADDR=":8080" operators
// usually set) is rewritten to "127.0.0.1:PORT" — the probe can
// only talk to the server via loopback, so binding to 0.0.0.0 is
// both unnecessary and surprising inside the container.
func resolveHealthcheckAddr(flagAddr string) string {
	a := flagAddr
	if a == "" {
		a = os.Getenv("HTTP_ADDR")
	}
	if a == "" {
		return healthcheckAddrDefault
	}
	if strings.HasPrefix(a, ":") {
		return "127.0.0.1" + a
	}
	return a
}
