package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/specgen"
)

// EndpointDeprecation describes the deprecation metadata declared for a single
// (method, path) operation in the OpenAPI specification.
type EndpointDeprecation struct {
	// Method is the upper-case HTTP verb (e.g. "GET").
	Method string
	// Path is the canonical OpenAPI route pattern (e.g. "/events/{id}").
	Path string
	// DeprecationHeader is the RFC 9745 Deprecation header value: either an
	// "@<unix-timestamp>" date value when x-deprecation-date is set, or "true".
	DeprecationHeader string
	// SunsetHeader is the RFC 8594 HTTP-date in GMT when x-sunset-date (or
	// x-sunset) is set on the operation, or "" when no removal date is scheduled.
	SunsetHeader string
	// SuccessorLink is the RFC 8594 / RFC 9745 Link header value
	// (`<URI>; rel="successor-version"`) when x-deprecation-replacement (or
	// x-successor-version) is set on the operation.
	SuccessorLink string
	// Notice is the human-readable deprecation summary from x-deprecation-notice.
	Notice string

	segments []string
}

// DeprecationPolicy holds the compiled set of deprecated HTTP operations parsed
// from an OpenAPI specification.
type DeprecationPolicy struct {
	exact       map[string]*EndpointDeprecation
	patternByM  map[string][]*EndpointDeprecation
	orderedList []*EndpointDeprecation
}

var (
	defaultPolicyOnce sync.Once
	defaultPolicy     *DeprecationPolicy
	defaultPolicyErr  error
)

// DefaultDeprecationPolicy returns the DeprecationPolicy compiled from the
// embedded OpenAPI specification (internal/api/openapi.json).
func DefaultDeprecationPolicy() (*DeprecationPolicy, error) {
	defaultPolicyOnce.Do(func() {
		defaultPolicy, defaultPolicyErr = ParseOpenAPIDeprecations(openapiSpec)
	})
	return defaultPolicy, defaultPolicyErr
}

type openAPIDeprecationDoc struct {
	Paths map[string]map[string]openAPIOperationDeprecation `json:"paths"`
}

type openAPIOperationDeprecation struct {
	OperationID      string `json:"operationId"`
	Deprecated       bool   `json:"deprecated"`
	DeprecationDate  string `json:"x-deprecation-date"`
	SunsetDate       string `json:"x-sunset-date"`
	Sunset           string `json:"x-sunset"`
	Replacement      string `json:"x-deprecation-replacement"`
	SuccessorVersion string `json:"x-successor-version"`
	Notice           string `json:"x-deprecation-notice"`
}

var validHTTPMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodPost:    true,
	http.MethodPut:     true,
	http.MethodPatch:   true,
	http.MethodDelete:  true,
	http.MethodHead:    true,
	http.MethodOptions: true,
}

// ParseOpenAPIDeprecations parses an OpenAPI 3.1 document (JSON or YAML) and
// builds the runtime DeprecationPolicy for every operation marked
// `deprecated: true`.
func ParseOpenAPIDeprecations(specBytes []byte) (*DeprecationPolicy, error) {
	trimmed := bytes.TrimSpace(specBytes)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty OpenAPI specification")
	}
	jsonBytes := trimmed
	if trimmed[0] != '{' {
		var err error
		jsonBytes, err = specgen.Render(trimmed)
		if err != nil {
			return nil, fmt.Errorf("rendering OpenAPI YAML: %w", err)
		}
	}

	var doc openAPIDeprecationDoc
	if err := json.Unmarshal(jsonBytes, &doc); err != nil {
		return nil, fmt.Errorf("parsing OpenAPI JSON: %w", err)
	}

	policy := &DeprecationPolicy{
		exact:      make(map[string]*EndpointDeprecation),
		patternByM: make(map[string][]*EndpointDeprecation),
	}

	paths := make([]string, 0, len(doc.Paths))
	for p := range doc.Paths {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, path := range paths {
		methodsMap := doc.Paths[path]
		methods := make([]string, 0, len(methodsMap))
		for m := range methodsMap {
			methods = append(methods, m)
		}
		sort.Strings(methods)

		for _, rawMethod := range methods {
			method := strings.ToUpper(rawMethod)
			if !validHTTPMethods[method] {
				continue
			}
			op := methodsMap[rawMethod]
			sunsetRaw := firstNonEmptyStr(op.SunsetDate, op.Sunset)
			if !op.Deprecated {
				if op.DeprecationDate != "" || sunsetRaw != "" {
					return nil, fmt.Errorf("%s %s sets deprecation/sunset date extension without deprecated: true", method, path)
				}
				continue
			}

			depHeader, err := formatDeprecationHeader(op.DeprecationDate)
			if err != nil {
				return nil, fmt.Errorf("%s %s invalid x-deprecation-date %q: %w", method, path, op.DeprecationDate, err)
			}
			sunsetHeader, err := formatSunsetHeader(sunsetRaw)
			if err != nil {
				return nil, fmt.Errorf("%s %s invalid x-sunset-date %q: %w", method, path, sunsetRaw, err)
			}
			linkHeader := formatSuccessorLink(firstNonEmptyStr(op.Replacement, op.SuccessorVersion))

			entry := &EndpointDeprecation{
				Method:            method,
				Path:              path,
				DeprecationHeader: depHeader,
				SunsetHeader:      sunsetHeader,
				SuccessorLink:     linkHeader,
				Notice:            strings.TrimSpace(op.Notice),
				segments:          splitRouteSegments(path),
			}
			policy.orderedList = append(policy.orderedList, entry)
			if strings.Contains(path, "{") {
				policy.patternByM[method] = append(policy.patternByM[method], entry)
			} else {
				policy.exact[method+" "+path] = entry
			}
		}
	}

	return policy, nil
}

// Entries returns all deprecated endpoint rules in deterministic (path, method) order.
func (p *DeprecationPolicy) Entries() []*EndpointDeprecation {
	if p == nil {
		return nil
	}
	out := make([]*EndpointDeprecation, len(p.orderedList))
	copy(out, p.orderedList)
	return out
}

// Lookup returns the EndpointDeprecation for an exact (method, canonicalPath)
// pair as declared in the OpenAPI spec (for example, ("GET", "/events/{id}")).
func (p *DeprecationPolicy) Lookup(method, canonicalPath string) (*EndpointDeprecation, bool) {
	if p == nil {
		return nil, false
	}
	m := strings.ToUpper(method)
	if dep, ok := p.exact[m+" "+canonicalPath]; ok {
		return dep, true
	}
	for _, dep := range p.patternByM[m] {
		if dep.Path == canonicalPath {
			return dep, true
		}
	}
	return nil, false
}

// Match resolves an incoming HTTP request's method and URL path against the
// policy. Both unprefixed routes (e.g. "/events/123") and "/api/v1"-prefixed
// routes (e.g. "/api/v1/events/123") match the canonical OpenAPI route pattern
// ("/events/{id}").
func (p *DeprecationPolicy) Match(method, urlPath string) (*EndpointDeprecation, bool) {
	if p == nil || (len(p.exact) == 0 && len(p.patternByM) == 0) {
		return nil, false
	}
	m := strings.ToUpper(method)
	normPath := normalizeVersionedPath(urlPath)

	if dep, ok := p.exact[m+" "+normPath]; ok {
		return dep, true
	}

	candidates := p.patternByM[m]
	if len(candidates) == 0 {
		return nil, false
	}
	reqSegs := splitRouteSegments(normPath)
	for _, dep := range candidates {
		if matchSegments(dep.segments, reqSegs) {
			return dep, true
		}
	}
	return nil, false
}

// SetDeprecationPolicy overrides the Server's OpenAPI deprecation policy. When
// unset (nil), the Server uses DefaultDeprecationPolicy() compiled from the
// embedded OpenAPI specification.
func (s *Server) SetDeprecationPolicy(policy *DeprecationPolicy) {
	s.deprecationMu.Lock()
	s.deprecationPolicy = policy
	s.deprecationMu.Unlock()
}

func (s *Server) effectiveDeprecationPolicy() *DeprecationPolicy {
	s.deprecationMu.RLock()
	p := s.deprecationPolicy
	s.deprecationMu.RUnlock()
	if p != nil {
		return p
	}
	def, _ := DefaultDeprecationPolicy()
	return def
}

// DeprecatedRequestCount returns the number of requests served by this Server
// for the given HTTP method and canonical route pattern.
func (s *Server) DeprecatedRequestCount(method, routePattern string) uint64 {
	s.deprecationMu.RLock()
	defer s.deprecationMu.RUnlock()
	if s.deprecatedCounts == nil {
		return 0
	}
	return s.deprecatedCounts[strings.ToUpper(method)+" "+routePattern]
}

func (s *Server) recordDeprecatedHit(dep *EndpointDeprecation) {
	metrics.DeprecatedHTTPRequestsTotal.WithLabelValues(dep.Path, dep.Method).Inc()
	s.deprecationMu.Lock()
	if s.deprecatedCounts == nil {
		s.deprecatedCounts = make(map[string]uint64)
	}
	s.deprecatedCounts[dep.Method+" "+dep.Path]++
	s.deprecationMu.Unlock()
}

// deprecationMiddleware emits RFC 9745 Deprecation, RFC 8594 Sunset, and
// successor-version Link headers on responses from deprecated endpoints, and
// increments sorotrail_http_deprecated_requests_total labelled by the canonical
// route pattern and HTTP method.
func (s *Server) deprecationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if policy := s.effectiveDeprecationPolicy(); policy != nil {
			if dep, ok := policy.Match(r.Method, r.URL.Path); ok {
				w.Header().Set("Deprecation", dep.DeprecationHeader)
				if dep.SunsetHeader != "" {
					w.Header().Set("Sunset", dep.SunsetHeader)
				}
				if dep.SuccessorLink != "" {
					w.Header().Add("Link", dep.SuccessorLink)
				}
				s.recordDeprecatedHit(dep)
			}
		}
		next.ServeHTTP(w, r)
	})
}

func normalizeVersionedPath(p string) string {
	if p == "" {
		return "/"
	}
	if p == "/api/v1" || p == "/api/v1/" {
		return "/"
	}
	if strings.HasPrefix(p, "/api/v1/") {
		return strings.TrimPrefix(p, "/api/v1")
	}
	return p
}

func splitRouteSegments(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func matchSegments(patternSegs, reqSegs []string) bool {
	if len(patternSegs) != len(reqSegs) {
		return false
	}
	for i, pSeg := range patternSegs {
		rSeg := reqSegs[i]
		if rSeg == "" {
			return false
		}
		if strings.HasPrefix(pSeg, "{") && strings.HasSuffix(pSeg, "}") {
			continue
		}
		if pSeg != rSeg {
			return false
		}
	}
	return true
}

func formatDeprecationHeader(raw string) (string, error) {
	val := strings.TrimSpace(raw)
	if val == "" || strings.EqualFold(val, "true") {
		return "true", nil
	}
	if strings.HasPrefix(val, "@") {
		sec, err := strconv.ParseInt(strings.TrimPrefix(val, "@"), 10, 64)
		if err != nil {
			return "", fmt.Errorf("expected integer unix timestamp after '@': %w", err)
		}
		return fmt.Sprintf("@%d", sec), nil
	}
	t, err := parsePolicyTime(val)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("@%d", t.UTC().Unix()), nil
}

func formatSunsetHeader(raw string) (string, error) {
	val := strings.TrimSpace(raw)
	if val == "" {
		return "", nil
	}
	t, err := parsePolicyTime(val)
	if err != nil {
		return "", err
	}
	return t.UTC().Format(http.TimeFormat), nil
}

func parsePolicyTime(val string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, val); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02", val); err == nil {
		return t.UTC(), nil
	}
	if t, err := http.ParseTime(val); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("must be RFC 3339 timestamp, YYYY-MM-DD date, or HTTP-date")
}

func formatSuccessorLink(raw string) string {
	val := strings.TrimSpace(raw)
	if val == "" {
		return ""
	}
	if strings.Contains(val, `rel="successor-version"`) {
		return val
	}
	fields := strings.Fields(val)
	if len(fields) == 2 && validHTTPMethods[strings.ToUpper(fields[0])] {
		val = fields[1]
	}
	val = strings.TrimPrefix(strings.TrimSuffix(val, ">"), "<")
	return fmt.Sprintf("<%s>; rel=\"successor-version\"", val)
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
