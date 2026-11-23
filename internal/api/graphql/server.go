package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Handler is the runtime for the GraphQL transport: one Resolver,
// one parsed schema, plus a slog logger. Safe for use across
// goroutines.
type Handler struct {
	resolver *Resolver
	log      *slog.Logger
	// playground, when true, serves GraphiQL at /graphiql.
	playground bool

	persisted    *PersistedRegistry
	manifestFile string

	mu            sync.RWMutex
	allowlistOnly bool
	apiKey        string
}

// Option configures optional features on the GraphQL Handler.
type Option func(*Handler)

// WithAllowlistOnly enables or disables persisted-query allowlist mode.
// When enabled, ad-hoc queries and unauthenticated on-the-fly APQ
// registrations are refused.
func WithAllowlistOnly(enabled bool) Option {
	return func(h *Handler) {
		h.SetAllowlistOnly(enabled)
	}
}

// WithAPIKey sets the operator API key used to authenticate persisted
// operation management requests (/graphql/persisted) and authenticated
// on-the-fly registrations in allowlist mode.
func WithAPIKey(apiKey string) Option {
	return func(h *Handler) {
		h.SetAPIKey(apiKey)
	}
}

// WithPersistedOperationsFile configures a JSON manifest file of persisted
// GraphQL operations to load at startup and update on registration changes.
func WithPersistedOperationsFile(path string) Option {
	return func(h *Handler) {
		h.manifestFile = strings.TrimSpace(path)
	}
}

// New builds a Handler from the API server's dependencies plus a flag
// gating dev-mode playground availability.
//
// The schema SDL is loaded from internal/api/graphql/schema.graphqls
// at startup. If the file is missing or unparseable, New returns an
// error — clients would otherwise see inconsistent behavior. The
// depth/complexity guards run independently of SDL validity since
// they only inspect the operation.
func New(s api.ServerDeps, log *slog.Logger, enablePlayground bool, opts ...Option) (*Handler, error) {
	sdl, err := loadSDL()
	if err != nil {
		return nil, fmt.Errorf("loading schema: %w", err)
	}
	// Parse the SDL only to verify it's syntactically valid at startup.
	// Per-operation validation runs in executeOperation's depth/complexity
	// guards; per-field argument shape checks live in the dispatcher.
	if _, err := parser.ParseSchema(&ast.Source{Name: "schema", Input: sdl}); err != nil {
		return nil, fmt.Errorf("parsing schema: %w", err)
	}

	h := &Handler{
		resolver: &Resolver{
			store:    s.Store,
			enricher: s.Enricher,
		},
		log:          log,
		playground:   enablePlayground,
		persisted:    NewPersistedRegistry(),
		manifestFile: strings.TrimSpace(os.Getenv("GRAPHQL_PERSISTED_OPERATIONS_FILE")),
		apiKey:       os.Getenv("API_KEY"),
	}
	if rawAllowlist := strings.TrimSpace(os.Getenv("GRAPHQL_PERSISTED_ONLY")); rawAllowlist != "" {
		enabled, err := strconv.ParseBool(rawAllowlist)
		if err != nil {
			return nil, fmt.Errorf("parsing GRAPHQL_PERSISTED_ONLY %q: %w", rawAllowlist, err)
		}
		h.allowlistOnly = enabled
	}
	UseResolver(h.resolver)
	registerRoutes(h.resolver)
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	if h.manifestFile != "" {
		if err := h.persisted.LoadManifestFile(h.manifestFile); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// SetAllowlistOnly enables or disables persisted-query allowlist mode at runtime.
func (h *Handler) SetAllowlistOnly(enabled bool) {
	h.mu.Lock()
	h.allowlistOnly = enabled
	h.mu.Unlock()
}

// AllowlistOnly reports whether persisted-query allowlist mode is active.
func (h *Handler) AllowlistOnly() bool {
	h.mu.RLock()
	v := h.allowlistOnly
	h.mu.RUnlock()
	return v
}

// SetAPIKey configures the operator API key required for persisted operation
// management endpoints.
func (h *Handler) SetAPIKey(apiKey string) {
	h.mu.Lock()
	h.apiKey = apiKey
	h.mu.Unlock()
}

func (h *Handler) getAPIKey() string {
	h.mu.RLock()
	v := h.apiKey
	h.mu.RUnlock()
	return v
}

// PersistedRegistry returns the handler's persisted operations registry.
func (h *Handler) PersistedRegistry() *PersistedRegistry {
	return h.persisted
}

// RegisterPersistedOperation validates and registers a single persisted query
// document in the handler's allowlist.
func (h *Handler) RegisterPersistedOperation(suppliedID, query string) (PersistedOperation, error) {
	return h.persisted.Register(suppliedID, query)
}

// ReplacePersistedOperations validates all entries and atomically replaces the
// handler's persisted query allowlist.
func (h *Handler) ReplacePersistedOperations(entries []PersistedRegistrationInput) ([]PersistedOperation, error) {
	return h.persisted.RegisterBatch(entries, true)
}

// DeletePersistedOperation removes a persisted query by its SHA-256 identifier.
func (h *Handler) DeletePersistedOperation(id string) (bool, error) {
	return h.persisted.Delete(id)
}

// ListPersistedOperations returns all registered persisted operations sorted by ID.
func (h *Handler) ListPersistedOperations() []PersistedOperation {
	return h.persisted.List()
}

// LoadPersistedOperationsFile loads and validates a JSON persisted-operations
// manifest from path.
func (h *Handler) LoadPersistedOperationsFile(path string) error {
	return h.persisted.LoadManifestFile(path)
}

// loadSDL reads the schema file from the package's data directory.
// Tests run from the package directory so the canonical path is fast
// to find; production builds find it via runtime.Caller as a stable
// reference point regardless of the working directory.
func loadSDL() (string, error) {
	here, err := runtimeFile()
	if err != nil {
		return "", err
	}
	candidates := []string{
		filepath.Join(here, "schema.graphqls"),
		filepath.Join(here, "internal", "api", "graphql", "schema.graphqls"),
	}
	for _, p := range candidates {
		if b, rerr := os.ReadFile(p); rerr == nil {
			return string(b), nil
		}
	}
	return "", fmt.Errorf("schema.graphqls not found (searched: %s)", strings.Join(candidates, ", "))
}

func runtimeFile() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	return filepath.Dir(file), nil
}

// registerRoutes wires the typed root Query fields to dispatcher
// funcs at init. Called once from New.
//
// Note on closures: each route binds `r` so the dispatcher can stay
// generic; this avoids a global resolverlookup or needing to thread
// the context through a Getter.
func registerRoutes(r *Resolver) {
	RegisterRoute("Query", "events", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		var a EventFilterArgs
		if len(args) > 0 {
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("invalid events argument: %w", err)
			}
		}
		return r.resolveEvents(ctx, a)
	})

	RegisterRoute("Query", "event", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		var a struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, fmt.Errorf("invalid event argument: %w", err)
		}
		return r.resolveEvent(ctx, a.ID)
	})

	RegisterRoute("Query", "tokenEvents", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		var a EventFilterArgs
		if len(args) > 0 {
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("invalid tokenEvents argument: %w", err)
			}
		}
		return r.resolveTokenEvents(ctx, a)
	})

	RegisterRoute("Query", "contracts", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		var a PageInput
		if len(args) > 0 {
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, fmt.Errorf("invalid contracts argument: %w", err)
			}
		}
		return r.resolveContracts(ctx, a)
	})

	RegisterRoute("Query", "contract", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		var a struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, fmt.Errorf("invalid contract argument: %w", err)
		}
		return r.resolveContract(ctx, a.ID)
	})
}

// ServeHTTP is the http.Handler entry point for POST /graphql. GET
// /graphql with no body returns a JSON message so accidental browser
// hits don't surface as cryptic request errors. Requests targeting
// /graphql/persisted (or /graphql/persisted/{id}) are routed to the
// persisted-operations management handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(store.WithSubsystem(r.Context(), store.SubsystemGraphQL))
	if r.URL != nil && isPersistedManagementPath(r.URL.Path) {
		h.handlePersisted(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
		return
	case http.MethodPost:
		h.handlePost(w, r)
		return
	default:
		w.Header().Set("Allow", "GET, POST")
		// The GraphQL contract fixes the response shape (data + errors), so
		// failures travel through this transport's own envelope rather than
		// the REST one — but always through writeErrorEnvelope, never bare.
		writeErrorEnvelope(w, http.StatusMethodNotAllowed, "method not allowed", nil)
	}
}

func isPersistedManagementPath(path string) bool {
	return path == "/graphql/persisted" ||
		strings.HasPrefix(path, "/graphql/persisted/") ||
		path == "/persisted" ||
		strings.HasPrefix(path, "/persisted/")
}

// handleGet handles GET /graphql — uses the `query` URL parameter (or
// persisted query identifier + optional `variables` / `extensions` JSON)
// so browser playground GETs and HTTP GET APQ requests work without a body.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	qp := r.URL.Query()
	req := GraphQLRequest{
		Query:         qp.Get("query"),
		OperationName: qp.Get("operationName"),
		ID:            qp.Get("id"),
		DocumentID:    qp.Get("documentId"),
		PersistedID:   qp.Get("persistedId"),
		SHA256Hash:    qp.Get("sha256Hash"),
	}
	if rawVars := qp.Get("variables"); rawVars != "" {
		if err := json.Unmarshal([]byte(rawVars), &req.Variables); err != nil {
			writeErrorEnvelope(w, http.StatusBadRequest, fmt.Sprintf("invalid variables JSON: %s", err.Error()), nil)
			return
		}
	}
	if rawExt := qp.Get("extensions"); rawExt != "" {
		var ext GraphQLExtensions
		if err := json.Unmarshal([]byte(rawExt), &ext); err != nil {
			writeErrorEnvelope(w, http.StatusBadRequest, fmt.Sprintf("invalid extensions JSON: %s", err.Error()), nil)
			return
		}
		req.Extensions = &ext
	}
	if req.Query == "" && req.persistedID() == "" && !h.AllowlistOnly() {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"POST {\"query\":\"…\"} to this endpoint"}`))
		return
	}
	h.runQueryWithRequest(w, r, &req)
}

// handlePost handles POST /graphql — accepts a JSON body matching
// GraphQLRequest.
func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErrorEnvelope(w, http.StatusBadRequest, fmt.Sprintf("read body: %s", err.Error()), nil)
		return
	}
	var req GraphQLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErrorEnvelope(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err.Error()), nil)
		return
	}
	h.runQueryWithRequest(w, r, &req)
}

func (h *Handler) runQueryWithRequest(w http.ResponseWriter, r *http.Request, req *GraphQLRequest) {
	if h.persisted == nil {
		h.persisted = NewPersistedRegistry()
	}
	if gerr := h.resolvePersistedRequest(r, req); gerr != nil {
		writeGraphQLError(w, http.StatusOK, *gerr)
		return
	}
	h.runQuery(w, r.Context(), req)
}

// runQuery is the common path: parse, validate, dispatch, serialize.
func (h *Handler) runQuery(w http.ResponseWriter, ctx context.Context, req *GraphQLRequest) {
	start := time.Now()
	resp, err := executeOperation(ctx, req)
	if err != nil {
		status := http.StatusOK
		if errors.Is(err, store.ErrPoolBudgetExhausted) {
			status = http.StatusServiceUnavailable
			retrySecs := 1
			var budgetErr *store.PoolBudgetExhaustedError
			if errors.As(err, &budgetErr) && budgetErr.RetryAfter > 0 {
				if s := int(budgetErr.RetryAfter.Seconds()); s > 0 {
					retrySecs = s
				}
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySecs))
		}
		// Operation-level error: no `data`, single entry in `errors`.
		writeErrorEnvelope(w, status, err.Error(), nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	_ = enc.Encode(resp)
	h.log.Info("graphql",
		"duration_ms", time.Since(start).Milliseconds(),
		"errors", len(resp.Errors),
	)
}

func writeErrorEnvelope(w http.ResponseWriter, status int, msg string, path []string) {
	writeGraphQLError(w, status, graphQLError{Message: msg, Path: path})
}

func writeGraphQLError(w http.ResponseWriter, status int, gerr graphQLError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseEnvelope{
		Data:   map[string]any{},
		Errors: errorsSlice{gerr},
	})
}

// PlaygroundHandler serves GraphiQL when Handler.playground is true.
// The HTML is fetched once from the inlined constant; the dev-mode
// gate lives in api.Server where the route is registered.
func (h *Handler) PlaygroundHandler() http.Handler {
	if !h.playground {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeErrorEnvelope(w, http.StatusNotFound, "playground is disabled", nil)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(graphiqlHTML))
	})
}
