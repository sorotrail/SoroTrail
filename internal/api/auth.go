// Package api — API key authentication.
//
// When API_KEY_AUTH_ENABLED=true, the router gates the write, streaming,
// subscription-management, and key-management routes behind a valid API
// key. Read-only endpoints (/events, /events/{id}, /stats, /health)
// stay open, so the public query surface keeps working unchanged.
//
// Keys have the shape `sorotrail_<prefix>_<secret>` (see
// internal/apikey). They are presented via the `Authorization: Bearer`
// header or the `X-API-Key` header. Only a bcrypt hash of the secret is
// stored; the prefix resolves the presented key to its database row,
// after which the secret is verified against the hash. Revoked keys are
// excluded at the SQL layer, so revocation takes effect on the next
// request.

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/apikey"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// errInvalidAPIKey is the single unauthenticated response for missing,
// malformed, unknown, revoked, and mismatched keys. Collapsing them
// denies attackers oracle information about which keys exist.
var errInvalidAPIKey = errors.New("missing or invalid API key")

// apiKeyFromRequest extracts the presented API key from the
// Authorization: Bearer header or the X-API-Key header (either is
// accepted; the latter is a convenience for clients that cannot set
// Authorization).
func apiKeyFromRequest(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		scheme, creds, found := cutBearer(auth)
		if found && scheme == "Bearer" {
			return creds
		}
	}
	return r.Header.Get("X-API-Key")
}

func cutBearer(auth string) (scheme, creds string, found bool) {
	for i := 0; i < len(auth); i++ {
		if auth[i] == ' ' {
			return auth[:i], auth[i+1:], true
		}
	}
	return auth, "", false
}

// requireAPIKey is the middleware that gates protected routes. It
// resolves the presented key to a stored key and verifies the secret;
// on any failure it writes a 401 before the wrapped handler runs.
func (s *Server) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := apiKeyFromRequest(r)
		if key == "" {
			s.writeUnauthorized(w)
			return
		}
		prefix, secret, ok := apikey.Parse(key)
		if !ok {
			s.writeUnauthorized(w)
			return
		}
		stored, err := s.store.LookupAPIKeyByPrefix(r.Context(), prefix)
		if errors.Is(err, store.ErrNotFound) {
			s.writeUnauthorized(w)
			return
		}
		if err != nil {
			s.log.Error("looking up API key", "error", err)
			writeError(w, http.StatusInternalServerError,
				errors.New("checking API key failed"))
			return
		}
		if !apikey.VerifySecret(secret, stored.KeyHash) {
			s.writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeUnauthorized sends the shared 401 with a WWW-Authenticate
// challenge so clients know how to authenticate.
func (s *Server) writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, http.StatusUnauthorized, errInvalidAPIKey)
}

// --- Key management handlers ---

// createAPIKeyRequest is the JSON body for POST /apikeys.
type createAPIKeyRequest struct {
	Name string `json:"name"`
}

// createAPIKeyResponse returns the stored key metadata plus the
// plaintext key — the only time the full key is ever visible.
type createAPIKeyResponse struct {
	store.APIKey
	Key string `json:"key"`
}

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var req createAPIKeyRequest
	// An empty body is fine (anonymous key); reject malformed JSON.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	if len(req.Name) > 100 {
		writeError(w, http.StatusBadRequest, errors.New("name must be at most 100 characters"))
		return
	}

	key, prefix, secret, err := apikey.Generate()
	if err != nil {
		s.log.Error("generating API key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("generating API key failed"))
		return
	}
	hash, err := apikey.HashSecret(secret)
	if err != nil {
		s.log.Error("hashing API key secret", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("creating API key failed"))
		return
	}

	created, err := s.store.CreateAPIKey(r.Context(), store.APIKey{
		Name:    req.Name,
		Prefix:  prefix,
		KeyHash: hash,
	})
	if err != nil {
		s.log.Error("persisting API key", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("creating API key failed"))
		return
	}
	writeJSON(w, http.StatusCreated, createAPIKeyResponse{APIKey: created, Key: key})
}

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.store.ListAPIKeys(r.Context())
	if err != nil {
		s.log.Error("listing API keys", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("listing API keys failed"))
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("API key id must be a positive integer, got %q", chi.URLParam(r, "id")))
		return
	}
	if err := s.store.RevokeAPIKey(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Errorf("API key %d not found", id))
			return
		}
		s.log.Error("revoking API key", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("revoking API key failed"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
