package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/sorotrail/sorotrail/internal/api"
)

// Handler is the runtime for the GraphQL transport: one Resolver,
// one parsed schema, plus a slog logger. Safe for use across
// goroutines.
type Handler struct {
	resolver *Resolver
	log      *slog.Logger
	// playground, when true, serves GraphiQL at /graphiql.
	playground bool
}

// New builds a Handler from the API server's dependencies plus a flag
// gating dev-mode playground availability.
//
// The schema SDL is loaded from internal/api/graphql/schema.graphqls
// at startup. If the file is missing or unparseable, New returns an
// error — clients would otherwise see inconsistent behavior. The
// depth/complexity guards run independently of SDL validity since
// they only inspect the operation.
func New(s api.ServerDeps, log *slog.Logger, enablePlayground bool) (*Handler, error) {
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
		log:        log,
		playground: enablePlayground,
	}
	UseResolver(h.resolver)
	registerRoutes(h.resolver)
	return h, nil
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
// hits don't surface as cryptic request errors.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
		return
	case http.MethodPost:
		h.handlePost(w, r)
		return
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet handles GET /graphql — uses the `query` URL parameter so
// browser playground GETs work without a body.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("query")
	if q == "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"POST {\"query\":\"…\"} to this endpoint"}`))
		return
	}
	h.runQuery(w, r.Context(), &GraphQLRequest{Query: q})
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
	h.runQuery(w, r.Context(), &req)
}

// runQuery is the common path: parse, validate, dispatch, serialize.
func (h *Handler) runQuery(w http.ResponseWriter, ctx context.Context, req *GraphQLRequest) {
	start := time.Now()
	resp, err := executeOperation(ctx, req)
	if err != nil {
		// Operation-level error: no `data`, single entry in `errors`.
		writeErrorEnvelope(w, http.StatusOK, err.Error(), nil)
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseEnvelope{
		Data:   map[string]any{},
		Errors: errorsSlice{{Message: msg, Path: path}},
	})
}

// PlaygroundHandler serves GraphiQL when Handler.playground is true.
// The HTML is fetched once from the inlined constant; the dev-mode
// gate lives in api.Server where the route is registered.
func (h *Handler) PlaygroundHandler() http.Handler {
	if !h.playground {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "playground is disabled", http.StatusNotFound)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(graphiqlHTML))
	})
}
