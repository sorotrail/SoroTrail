package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// The health vocabulary distinguishes a dependency that misbehaves from
// one that answers at all, because operators act differently on each:
//
//   - ok       the dependency answered within its budget and reported
//     itself healthy.
//   - degraded the dependency answered, but reports a suboptimal state —
//     Stellar RPC's getHealth is not "healthy", or ingestion has spread
//     past the lag threshold. It is reachable, so the response carries
//     the detail and the caller decides.
//   - unready  the dependency did not answer: it errored, or its check
//     blew the per-check budget. Nothing downstream of it can be trusted.
const (
	healthOK       = "ok"
	healthDegraded = "degraded"
	healthUnready  = "unready"
)

// healthCheckTimeout bounds EACH probe behind /health and /readyz, not
// the request: every check runs concurrently with its own budget derived
// from the request context, so a slow database cannot eat the time a
// contract-lag check needs — under a shared deadline the first slow
// dependency starves the rest into reporting failures they didn't cause.
// Five seconds matches the budget the handlers used to share; the Docker
// healthcheck allows 3s per probe, and a check that hangs longer than
// that already fails the probe at the transport level.
const healthCheckTimeout = 5 * time.Second

// readinessLagThreshold is how far ingestion may lag the chain head
// before /readyz reports ingestion_lag as degraded: more than this many
// ledgers behind means stored events are stale enough that the instance
// should be pulled from rotation. The guard on LastIngestedLedger keeps
// a cold instance (ingested=0) from being judged against a chain head it
// has not started following yet.
const readinessLagThreshold = 100

// dependencyCheck is one dependency's entry in a health envelope: its
// state, why it isn't ok, and how long its own probe took.
type dependencyCheck struct {
	Status string `json:"status"`           // ok | degraded | unready
	Detail string `json:"detail,omitempty"` // why the status isn't ok
	// LatencyMS is this check's wall time alone — with the checks running
	// concurrently it is directly comparable across dependencies, which is
	// what makes a slow dependency visible before it becomes a failed one.
	LatencyMS int64 `json:"latency_ms"`
}

// healthResponse is the envelope /health, /livez and /readyz all share,
// so one decoder serves all three. Status aggregates the checks with
// unready dominating degraded dominating ok.
type healthResponse struct {
	Status string                     `json:"status"` // ok | degraded | unready
	Checks map[string]dependencyCheck `json:"checks"`
}

// namedCheck pairs the dependency name reported in the envelope with the
// probe that produces its state.
type namedCheck struct {
	name string
	fn   func(ctx context.Context) (status, detail string)
}

// healthChecks is /health's probe set: the two external dependencies.
// A nil dependency — a hand-built or embedded Server that was never
// given one — is omitted rather than reported ok or probed into a panic:
// a missing witness must not masquerade as a passing one, nor take the
// process down from inside a probe goroutine the recovery middleware
// cannot reach.
func (s *Server) healthChecks() []namedCheck {
	checks := make([]namedCheck, 0, 2)
	if s.store != nil {
		checks = append(checks, namedCheck{name: "database", fn: s.checkDatabase})
	}
	if s.rpc != nil {
		checks = append(checks, namedCheck{name: "rpc", fn: s.checkRPC})
	}
	return checks
}

// readinessChecks extends /health's set with ingestion freshness, which
// only /readyz cares about: readiness is about serving current data, so
// an instance whose ingestion trailed the chain head must leave rotation
// while /health keeps reporting its dependencies as reachable.
func (s *Server) readinessChecks() []namedCheck {
	checks := s.healthChecks()
	if s.store != nil {
		checks = append(checks, namedCheck{name: "ingestion_lag", fn: s.checkIngestionLag})
	}
	return checks
}

func (s *Server) checkDatabase(ctx context.Context) (string, string) {
	b := s.StoreBreaker()
	if b != nil && !b.Allow() {
		if last := b.LastError(); last != nil {
			return healthUnready, last.Error()
		}
		return healthUnready, store.ErrUnavailable.Error()
	}
	if err := s.store.Ping(ctx); err != nil {
		if b != nil {
			b.RecordFailure(err)
		}
		return healthUnready, err.Error()
	}
	if b != nil {
		b.RecordSuccess()
	}
	if s.isDegraded() {
		return healthUnready, store.ErrUnavailable.Error()
	}
	return healthOK, ""
}

func (s *Server) checkRPC(ctx context.Context) (string, string) {
	health, err := s.rpc.GetHealth(ctx)
	if err != nil {
		return healthUnready, err.Error()
	}
	if health.Status != "healthy" {
		return healthDegraded, fmt.Sprintf("rpc reports %q", health.Status)
	}
	return healthOK, ""
}

// checkIngestionLag measures stored-vs-chain-head distance. It runs its
// own RPC and store calls under its own budget rather than reusing the
// rpc check's answer: a probe that shares another check's result stops
// being independently testable, and the second getHealth per probe is
// cheap next to the correctness it buys.
func (s *Server) checkIngestionLag(ctx context.Context) (string, string) {
	state, err := s.store.GetIngestionState(ctx)
	if errors.Is(err, store.ErrNotFound) {
		// Cold start: nothing ingested yet, so there is no lag to report.
		return healthOK, "no ingestion state recorded yet"
	}
	if err != nil {
		return healthUnready, err.Error()
	}
	if s.rpc == nil {
		return healthOK, "chain head unknown: no RPC client configured"
	}
	health, err := s.rpc.GetHealth(ctx)
	if err != nil {
		return healthUnready, "chain head unavailable: " + err.Error()
	}
	lag := ingestLagLedgers(int64(health.LatestLedger), state.LastIngestedLedger)
	if state.LastIngestedLedger > 0 && lag > readinessLagThreshold {
		return healthDegraded, fmt.Sprintf(
			"%d ledgers behind (last ingested: %d, chain head: %d)",
			lag, state.LastIngestedLedger, health.LatestLedger)
	}
	return healthOK, ""
}

// runChecks executes the probes concurrently, each under its own
// timeout, and returns the per-dependency results. A check that misses
// its budget says so explicitly instead of surfacing whatever generic
// context error the deadline produced, so the envelope names the cause.
func (s *Server) runChecks(r *http.Request, checks []namedCheck) map[string]dependencyCheck {
	out := make(map[string]dependencyCheck, len(checks))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, c := range checks {
		wg.Add(1)
		go func(c namedCheck) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), s.healthTimeout())
			defer cancel()

			start := time.Now()
			status, detail := c.fn(ctx)
			latency := time.Since(start)
			if status != healthOK && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				detail = fmt.Sprintf("timed out after %s", s.healthTimeout())
			}

			mu.Lock()
			out[c.name] = dependencyCheck{Status: status, Detail: detail, LatencyMS: latency.Milliseconds()}
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return out
}

// healthTimeout is the per-check budget: the zero value falls back to
// the production default, which is what lets tests shorten it so timeout
// paths don't sleep for five seconds.
func (s *Server) healthTimeout() time.Duration {
	if s.checkTimeout > 0 {
		return s.checkTimeout
	}
	return healthCheckTimeout
}

// aggregateHealth folds per-check states into the envelope status:
// unready dominates degraded, which dominates ok.
func aggregateHealth(checks map[string]dependencyCheck) string {
	overall := healthOK
	for _, c := range checks {
		switch c.Status {
		case healthUnready:
			return healthUnready
		case healthDegraded:
			overall = healthDegraded
		}
	}
	return overall
}

// writeHealth runs the given checks and writes the envelope. 200 only
// when every check is ok — a degraded OR unready check answers 503, so
// each endpoint keeps its historical status-code contract while the body
// now says which dependency failed and in what way. The endpoints differ
// in which checks they run, not in how they grade them.
func (s *Server) writeHealth(w http.ResponseWriter, r *http.Request, checks []namedCheck) {
	results := s.runChecks(r, checks)
	status := aggregateHealth(results)
	code := http.StatusOK
	if status != healthOK {
		code = http.StatusServiceUnavailable
	}
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, code, healthResponse{Status: status, Checks: results})
}

// handleHealth reports the API's view of its external dependencies
// (database, RPC). Unlike /livez it does consult them, and unlike /readyz
// it ignores ingestion freshness — it answers "are the things this
// process talks to alive", which is what dashboards alert on.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeHealth(w, r, s.healthChecks())
}

// handleLivez answers "is this process alive?" and consults no
// dependency at all. That independence is the whole point: a dead
// database must not get the binary restarted by a liveness probe — that
// turns an external outage into an outage of the thing that could have
// reported it. Probes that need dependency state point at /readyz.
func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, http.StatusOK, healthResponse{
		Status: healthOK,
		Checks: map[string]dependencyCheck{"process": {Status: healthOK}},
	})
}

// handleReadyz answers "should this instance receive traffic?" Every
// dependency gets its own entry — state, reason and latency — so a
// failing probe identifies the dependency that failed without anyone
// reading logs. Thresholds (also documented in api/openapi.yaml):
//
//   - any check unready  -> 503: a dependency gave no answer within its
//     budget, so requests that depend on it would fail too.
//   - degraded only      -> 503 as well: a degraded dependency answered,
//     but the instance is serving stale data (lag past
//     readinessLagThreshold) or a non-healthy RPC, and rotation should
//     move on until it recovers. The body's degraded/unready split tells
//     the operator whether to wait or to intervene.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.writeHealth(w, r, s.readinessChecks())
}
