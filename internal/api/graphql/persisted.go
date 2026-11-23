package graphql

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// Standard GraphQL error extension codes surfaced by persisted-query handling.
const (
	// CodePersistedQueryNotFound is returned in errors[].extensions.code when
	// a client executes a persisted query by identifier alone and the hash is
	// not in the registered allowlist. Clients implementing Automatic Persisted
	// Queries (APQ) inspect this code to know they should retry with the full
	// query document (when allowlist mode is off).
	CodePersistedQueryNotFound = "PERSISTED_QUERY_NOT_FOUND"

	// CodePersistedQueryHashMismatch is returned when a caller supplies both a
	// persisted identifier and a query document whose SHA-256 digest does not
	// match the supplied identifier.
	CodePersistedQueryHashMismatch = "PERSISTED_QUERY_HASH_MISMATCH"

	// CodePersistedQueryOnly is returned when persisted-query allowlist mode
	// (GRAPHQL_PERSISTED_ONLY=true) is active and a caller submits an ad-hoc
	// query document or attempts unauthenticated on-the-fly registration.
	CodePersistedQueryOnly = "PERSISTED_QUERY_ONLY"
)

var (
	// ErrPersistedEmptyDocument is returned when registering an empty query string.
	ErrPersistedEmptyDocument = errors.New("persisted query document must not be empty")
	// ErrPersistedHashMismatch is returned when a caller-supplied ID does not match
	// the canonical SHA-256 digest of the query document.
	ErrPersistedHashMismatch = errors.New("persisted query id does not match document sha256")
	// ErrPersistedInvalidDocument is returned when a query document fails GraphQL
	// parsing, operation-type validation, or static depth/complexity limits.
	ErrPersistedInvalidDocument = errors.New("invalid persisted GraphQL document")
)

// PersistedOperation represents a validated, registered GraphQL query document
// stored in the allowlist and addressed by its canonical lowercase hex SHA-256 digest.
type PersistedOperation struct {
	ID            string    `json:"id"`
	OperationName string    `json:"operationName,omitempty"`
	Query         string    `json:"query"`
	RegisteredAt  time.Time `json:"registeredAt"`
}

// PersistedRegistrationInput is one entry in a registration request or manifest.
type PersistedRegistrationInput struct {
	ID         string `json:"id,omitempty"`
	SHA256Hash string `json:"sha256Hash,omitempty"`
	Query      string `json:"query"`
}

func (in PersistedRegistrationInput) suppliedID() string {
	if s := strings.TrimSpace(in.ID); s != "" {
		return s
	}
	return strings.TrimSpace(in.SHA256Hash)
}

// ComputePersistedID computes the canonical identifier for a GraphQL query
// document: the lowercase hex-encoded SHA-256 digest of the exact document bytes.
func ComputePersistedID(query string) string {
	sum := sha256.Sum256([]byte(query))
	return hex.EncodeToString(sum[:])
}

// ValidatePersistedDocument verifies that query is a valid, single-operation
// read-only GraphQL Query document within the server's static depth and
// complexity limits, and that suppliedID (when non-empty) equals the canonical
// SHA-256 hex digest of query.
func ValidatePersistedDocument(suppliedID, query string) (PersistedOperation, error) {
	if strings.TrimSpace(query) == "" {
		return PersistedOperation{}, ErrPersistedEmptyDocument
	}

	canonicalID := ComputePersistedID(query)
	if trimmedID := strings.TrimSpace(suppliedID); trimmedID != "" {
		normalizedID := strings.ToLower(trimmedID)
		if normalizedID != canonicalID {
			return PersistedOperation{}, fmt.Errorf("%w: supplied %q, computed %q",
				ErrPersistedHashMismatch, trimmedID, canonicalID)
		}
	}

	doc, err := parser.ParseQuery(&ast.Source{Name: "persisted", Input: query})
	if err != nil {
		return PersistedOperation{}, fmt.Errorf("%w: parse: %v", ErrPersistedInvalidDocument, err)
	}
	if len(doc.Operations) == 0 {
		return PersistedOperation{}, fmt.Errorf("%w: no operation in document", ErrPersistedInvalidDocument)
	}
	if len(doc.Operations) > 1 {
		return PersistedOperation{}, fmt.Errorf("%w: multiple operations per document not supported", ErrPersistedInvalidDocument)
	}
	op := doc.Operations[0]
	if op.Operation != ast.Query {
		return PersistedOperation{}, fmt.Errorf("%w: only Query operations are supported (got %s)", ErrPersistedInvalidDocument, op.Operation)
	}
	if err := validateAndCount(op); err != nil {
		return PersistedOperation{}, fmt.Errorf("%w: %v", ErrPersistedInvalidDocument, err)
	}

	return PersistedOperation{
		ID:            canonicalID,
		OperationName: op.Name,
		Query:         query,
		RegisteredAt:  time.Now().UTC(),
	}, nil
}

// PersistedRegistry is a concurrency-safe store of registered persisted GraphQL
// operations, optionally backed by an on-disk JSON manifest file.
type PersistedRegistry struct {
	mu           sync.RWMutex
	ops          map[string]PersistedOperation
	manifestPath string
}

// NewPersistedRegistry creates an empty persisted operations registry.
func NewPersistedRegistry() *PersistedRegistry {
	return &PersistedRegistry{
		ops: make(map[string]PersistedOperation),
	}
}

// Register validates and inserts (or updates) a single persisted GraphQL operation.
// If suppliedID is empty, the canonical SHA-256 digest of query is used; if
// suppliedID is non-empty, it must match ComputePersistedID(query).
func (reg *PersistedRegistry) Register(suppliedID, query string) (PersistedOperation, error) {
	op, err := ValidatePersistedDocument(suppliedID, query)
	if err != nil {
		return PersistedOperation{}, err
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	if existing, ok := reg.ops[op.ID]; ok {
		op.RegisteredAt = existing.RegisteredAt
	}
	reg.ops[op.ID] = op
	if err := reg.saveLocked(); err != nil {
		return PersistedOperation{}, err
	}
	return op, nil
}

// RegisterBatch validates all entries first and then either atomically replaces
// the entire allowlist (when replace is true) or merges them into the existing
// allowlist (when replace is false). If any entry fails validation or hash
// verification, no changes are applied.
func (reg *PersistedRegistry) RegisterBatch(entries []PersistedRegistrationInput, replace bool) ([]PersistedOperation, error) {
	validated := make([]PersistedOperation, 0, len(entries))
	seen := make(map[string]PersistedOperation, len(entries))
	for i, entry := range entries {
		op, err := ValidatePersistedDocument(entry.suppliedID(), entry.Query)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: %w", i, err)
		}
		if _, dup := seen[op.ID]; !dup {
			validated = append(validated, op)
		}
		seen[op.ID] = op
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	prev := reg.ops
	var next map[string]PersistedOperation
	if replace {
		next = make(map[string]PersistedOperation, len(validated))
	} else {
		next = make(map[string]PersistedOperation, len(reg.ops)+len(validated))
		for k, v := range reg.ops {
			next[k] = v
		}
	}

	for _, op := range validated {
		if existing, ok := prev[op.ID]; ok {
			op.RegisteredAt = existing.RegisteredAt
		}
		next[op.ID] = op
	}
	reg.ops = next
	if err := reg.saveLocked(); err != nil {
		reg.ops = prev
		return nil, err
	}

	out := make([]PersistedOperation, 0, len(validated))
	for _, op := range validated {
		out = append(out, reg.ops[op.ID])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Get looks up a persisted operation by its SHA-256 hex identifier (case-insensitive).
func (reg *PersistedRegistry) Get(id string) (PersistedOperation, bool) {
	normalized := strings.ToLower(strings.TrimSpace(id))
	if normalized == "" {
		return PersistedOperation{}, false
	}
	reg.mu.RLock()
	op, ok := reg.ops[normalized]
	reg.mu.RUnlock()
	return op, ok
}

// Delete removes a persisted operation by ID and reports whether it was present.
func (reg *PersistedRegistry) Delete(id string) (bool, error) {
	normalized := strings.ToLower(strings.TrimSpace(id))
	if normalized == "" {
		return false, nil
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	existing, ok := reg.ops[normalized]
	if !ok {
		return false, nil
	}
	delete(reg.ops, normalized)
	if err := reg.saveLocked(); err != nil {
		reg.ops[normalized] = existing
		return false, err
	}
	return true, nil
}

// List returns all registered persisted operations sorted by ID ascending.
func (reg *PersistedRegistry) List() []PersistedOperation {
	reg.mu.RLock()
	out := make([]PersistedOperation, 0, len(reg.ops))
	for _, op := range reg.ops {
		out = append(out, op)
	}
	reg.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Count returns the number of registered persisted operations.
func (reg *PersistedRegistry) Count() int {
	reg.mu.RLock()
	n := len(reg.ops)
	reg.mu.RUnlock()
	return n
}

// LoadManifestFile loads persisted operations from a JSON file at path and binds
// the registry to that path for subsequent updates. If the file does not exist
// yet, the registry starts empty and will create the file when operations are
// registered.
//
// Supported JSON manifest shapes:
//  1. Object with operations list: {"operations": [{"id": "<sha256>", "query": "..."}]}
//  2. Object with manifest map:    {"manifest": {"<sha256>": "query ..."}}
//  3. Array of operation objects:  [{"id": "<sha256>", "query": "..."}]
//  4. Standard map (Apollo/Relay): {"<sha256>": "query ..."}
func (reg *PersistedRegistry) LoadManifestFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			reg.mu.Lock()
			reg.manifestPath = path
			reg.mu.Unlock()
			return nil
		}
		return fmt.Errorf("reading persisted operations file %q: %w", path, err)
	}

	entries, _, err := parseRegistrationPayload(raw, true)
	if err != nil {
		return fmt.Errorf("parsing persisted operations file %q: %w", path, err)
	}

	// Validate and load without writing back the file we just read.
	validated := make(map[string]PersistedOperation, len(entries))
	for i, entry := range entries {
		op, verr := ValidatePersistedDocument(entry.suppliedID(), entry.Query)
		if verr != nil {
			return fmt.Errorf("persisted operations file %q operation[%d]: %w", path, i, verr)
		}
		validated[op.ID] = op
	}

	reg.mu.Lock()
	reg.ops = validated
	reg.manifestPath = path
	reg.mu.Unlock()
	return nil
}

func (reg *PersistedRegistry) saveLocked() error {
	if reg.manifestPath == "" {
		return nil
	}
	ops := make([]PersistedOperation, 0, len(reg.ops))
	for _, op := range reg.ops {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID < ops[j].ID })

	payload := struct {
		Operations []PersistedOperation `json:"operations"`
	}{
		Operations: ops,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling persisted operations manifest: %w", err)
	}
	dir := filepath.Dir(reg.manifestPath)
	tmp, err := os.CreateTemp(dir, ".persisted-graphql-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp persisted operations manifest: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("writing persisted operations manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("closing persisted operations manifest: %w", err)
	}
	if err := os.Rename(tmpName, reg.manifestPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("replacing persisted operations manifest: %w", err)
	}
	return nil
}

// parseRegistrationPayload accepts any of the supported registration request /
// manifest JSON formats and returns the normalized slice of inputs plus whether
// the caller requested a full allowlist replacement.
func parseRegistrationPayload(raw []byte, defaultReplace bool) ([]PersistedRegistrationInput, bool, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, false, errors.New("request body is empty")
	}

	// Case 1: top-level JSON array of operation objects.
	if strings.HasPrefix(trimmed, "[") {
		var list []PersistedRegistrationInput
		if err := json.Unmarshal(raw, &list); err != nil {
			return nil, false, fmt.Errorf("invalid operations array JSON: %w", err)
		}
		return list, defaultReplace, nil
	}

	// Case 2: JSON object — may be a single operation, a batch {"operations": [...]},
	// a manifest wrapper {"manifest": {...}}, or a direct {"<sha256>": "<query>"} map.
	var envelope struct {
		ID         string                       `json:"id"`
		SHA256Hash string                       `json:"sha256Hash"`
		Query      *string                      `json:"query"`
		Operations []PersistedRegistrationInput `json:"operations"`
		Manifest   map[string]string            `json:"manifest"`
		Replace    *bool                        `json:"replace"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, false, fmt.Errorf("invalid JSON: %w", err)
	}

	replace := defaultReplace
	if envelope.Replace != nil {
		replace = *envelope.Replace
	}

	if envelope.Query != nil {
		return []PersistedRegistrationInput{{
			ID:         envelope.ID,
			SHA256Hash: envelope.SHA256Hash,
			Query:      *envelope.Query,
		}}, replace, nil
	}

	if envelope.Operations != nil {
		return envelope.Operations, replace, nil
	}

	if envelope.Manifest != nil {
		return mapToRegistrationInputs(envelope.Manifest), replace, nil
	}

	// Fallback: direct map of {"<sha256>": "query ..."} (Apollo/Relay manifest format).
	var directMap map[string]string
	if err := json.Unmarshal(raw, &directMap); err == nil && len(directMap) > 0 {
		return mapToRegistrationInputs(directMap), replace, nil
	}

	return nil, false, errors.New("registration payload must provide 'query', 'operations', or a manifest map")
}

func mapToRegistrationInputs(m map[string]string) []PersistedRegistrationInput {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]PersistedRegistrationInput, 0, len(keys))
	for _, k := range keys {
		out = append(out, PersistedRegistrationInput{
			ID:    k,
			Query: m[k],
		})
	}
	return out
}

// isAuthorizedOperator reports whether the HTTP request carries the configured
// operator API_KEY via X-API-Key or Authorization: Bearer.
func (h *Handler) isAuthorizedOperator(r *http.Request) (configured bool, authorized bool) {
	expected := h.getAPIKey()
	if expected == "" {
		return false, false
	}
	provided := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if provided == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			provided = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		}
	}
	if provided == "" {
		return true, false
	}
	match := subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
	return true, match
}

// resolvePersistedRequest enforces persisted-query lookup, APQ registration,
// and allowlist-only mode on incoming /graphql requests. It mutates req.Query
// to the registered document when the caller supplies a persisted ID alone.
// Returns a non-nil *graphQLError when the request must be refused before
// execution.
func (h *Handler) resolvePersistedRequest(r *http.Request, req *GraphQLRequest) *graphQLError {
	if req == nil {
		return nil
	}
	pid := strings.ToLower(req.persistedID())
	hasQuery := strings.TrimSpace(req.Query) != ""
	allowlistOnly := h.AllowlistOnly()

	switch {
	case pid != "" && !hasQuery:
		// Execution by persisted identifier alone.
		op, ok := h.persisted.Get(pid)
		if !ok {
			return &graphQLError{
				Message:    "PersistedQueryNotFound",
				Extensions: map[string]any{"code": CodePersistedQueryNotFound},
			}
		}
		req.Query = op.Query
		if req.OperationName == "" && op.OperationName != "" {
			req.OperationName = op.OperationName
		}
		return nil

	case pid != "" && hasQuery:
		// Both identifier and query document supplied (e.g. APQ registration or
		// client-side hash verification). Always verify the hash first so a
		// caller can never bind an arbitrary query to a chosen ID.
		computed := ComputePersistedID(req.Query)
		if computed != pid {
			return &graphQLError{
				Message: fmt.Sprintf("persisted query hash mismatch: supplied id %q does not match document sha256 %q", pid, computed),
				Extensions: map[string]any{
					"code":     CodePersistedQueryHashMismatch,
					"expected": computed,
					"supplied": pid,
				},
			}
		}
		if _, ok := h.persisted.Get(pid); ok {
			// Already in the allowlist and hash matches.
			return nil
		}
		if allowlistOnly {
			// In allowlist mode, unauthenticated callers cannot register new
			// queries on the fly via APQ. Only an authenticated operator
			// presenting the configured API_KEY may register on the fly.
			if _, ok := h.isAuthorizedOperator(r); !ok {
				return &graphQLError{
					Message:    "persisted query allowlist mode is enabled; unauthenticated callers cannot register new queries on the fly",
					Extensions: map[string]any{"code": CodePersistedQueryOnly},
				}
			}
		}
		if _, err := h.persisted.Register(pid, req.Query); err != nil {
			return &graphQLError{
				Message: err.Error(),
			}
		}
		return nil

	case pid == "" && hasQuery:
		// Ad-hoc query document without a persisted identifier.
		if allowlistOnly {
			return &graphQLError{
				Message:    "ad-hoc GraphQL queries are disabled in persisted-query allowlist mode; execute a registered operation by id",
				Extensions: map[string]any{"code": CodePersistedQueryOnly},
			}
		}
		return nil

	default:
		if allowlistOnly {
			return &graphQLError{
				Message:    "persisted query identifier is required when allowlist mode is enabled",
				Extensions: map[string]any{"code": CodePersistedQueryOnly},
			}
		}
		return nil
	}
}

// handlePersisted serves the persisted-operations inspection and management
// endpoints under /graphql/persisted and /graphql/persisted/{id}.
//
//   - GET /graphql/persisted: returns the ordered allowlist and allowlistOnly mode flag
//   - GET /graphql/persisted/{id}: returns a single persisted operation by SHA-256 ID
//   - POST /graphql/persisted: registers one or more persisted operations (merges by default, or replaces when "replace": true)
//   - PUT /graphql/persisted: atomically replaces the allowlist with a new release manifest (unless "replace": false)
//   - DELETE /graphql/persisted/{id}: removes a persisted operation by SHA-256 ID
func (h *Handler) handlePersisted(w http.ResponseWriter, r *http.Request) {
	subpath := strings.TrimPrefix(r.URL.Path, "/graphql/persisted")
	subpath = strings.TrimPrefix(subpath, "/persisted")
	subpath = strings.TrimPrefix(subpath, "/")
	idParam := strings.TrimSpace(subpath)
	if idParam == "" {
		idParam = strings.TrimSpace(r.URL.Query().Get("id"))
	}

	switch r.Method {
	case http.MethodGet:
		if idParam != "" {
			op, ok := h.persisted.Get(idParam)
			if !ok {
				writeJSONStatus(w, http.StatusNotFound, map[string]any{
					"error": "persisted operation not found",
					"id":    strings.ToLower(idParam),
				})
				return
			}
			writeJSONStatus(w, http.StatusOK, op)
			return
		}
		ops := h.persisted.List()
		writeJSONStatus(w, http.StatusOK, map[string]any{
			"allowlistOnly": h.AllowlistOnly(),
			"count":         len(ops),
			"operations":    ops,
		})

	case http.MethodPost, http.MethodPut:
		if !h.requireManagementAuth(w, r) {
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]any{
				"error": fmt.Sprintf("read body: %v", err),
			})
			return
		}
		defaultReplace := r.Method == http.MethodPut
		entries, replace, err := parseRegistrationPayload(body, defaultReplace)
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]any{
				"error": err.Error(),
			})
			return
		}
		if len(entries) == 1 && !replace && r.Method == http.MethodPost {
			op, rerr := h.persisted.Register(entries[0].suppliedID(), entries[0].Query)
			if rerr != nil {
				writeJSONStatus(w, http.StatusBadRequest, map[string]any{
					"error": rerr.Error(),
				})
				return
			}
			writeJSONStatus(w, http.StatusCreated, map[string]any{
				"operation": op,
				"count":     h.persisted.Count(),
			})
			return
		}
		ops, rerr := h.persisted.RegisterBatch(entries, replace)
		if rerr != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]any{
				"error": rerr.Error(),
			})
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]any{
			"operations": ops,
			"replaced":   replace,
			"count":      h.persisted.Count(),
		})

	case http.MethodDelete:
		if !h.requireManagementAuth(w, r) {
			return
		}
		if idParam == "" {
			writeJSONStatus(w, http.StatusBadRequest, map[string]any{
				"error": "persisted operation id is required",
			})
			return
		}
		deleted, err := h.persisted.Delete(idParam)
		if err != nil {
			writeJSONStatus(w, http.StatusInternalServerError, map[string]any{
				"error": err.Error(),
			})
			return
		}
		if !deleted {
			writeJSONStatus(w, http.StatusNotFound, map[string]any{
				"error": "persisted operation not found",
				"id":    strings.ToLower(idParam),
			})
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]any{
			"deleted": strings.ToLower(idParam),
			"count":   h.persisted.Count(),
		})

	default:
		w.Header().Set("Allow", "GET, POST, PUT, DELETE")
		writeJSONStatus(w, http.StatusMethodNotAllowed, map[string]any{
			"error": "method not allowed",
		})
	}
}

func (h *Handler) requireManagementAuth(w http.ResponseWriter, r *http.Request) bool {
	configured, authorized := h.isAuthorizedOperator(r)
	if !configured {
		writeJSONStatus(w, http.StatusServiceUnavailable, map[string]any{
			"error": "API_KEY env var must be set to manage persisted GraphQL operations",
		})
		return false
	}
	if !authorized {
		writeJSONStatus(w, http.StatusUnauthorized, map[string]any{
			"error": "invalid or missing X-API-Key header",
		})
		return false
	}
	return true
}

func writeJSONStatus(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
