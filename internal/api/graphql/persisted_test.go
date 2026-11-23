package graphql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/store"
)

type persistedEnvelope struct {
	Data   map[string]any `json:"data"`
	Errors []struct {
		Message    string         `json:"message"`
		Path       []string       `json:"path"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func newPersistedTestHandler(t *testing.T, st store.Store, opts ...Option) *Handler {
	t.Helper()
	h, err := New(
		api.ServerDeps{Store: st},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
		opts...,
	)
	require.NoError(t, err)
	return h
}

func decodePersistedEnvelope(t *testing.T, rec *httptest.ResponseRecorder) persistedEnvelope {
	t.Helper()
	var env persistedEnvelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env), "body: %s", rec.Body.String())
	return env
}

func TestValidatePersistedDocument_HashAndASTValidation(t *testing.T) {
	validQuery := `query GetContract($id: ID!) { contract(id: $id) { contractId addedAt } }`
	wantID := ComputePersistedID(validQuery)

	t.Run("derives canonical sha256 when id omitted", func(t *testing.T) {
		op, err := ValidatePersistedDocument("", validQuery)
		require.NoError(t, err)
		assert.Equal(t, wantID, op.ID)
		assert.Equal(t, "GetContract", op.OperationName)
		assert.Equal(t, validQuery, op.Query)
	})

	t.Run("accepts matching id case-insensitively", func(t *testing.T) {
		op, err := ValidatePersistedDocument(strings.ToUpper(wantID), validQuery)
		require.NoError(t, err)
		assert.Equal(t, wantID, op.ID)
	})

	t.Run("rejects mismatched caller-supplied id", func(t *testing.T) {
		wrongID := ComputePersistedID(`{ contracts { totalCount } }`)
		_, err := ValidatePersistedDocument(wrongID, validQuery)
		require.ErrorIs(t, err, ErrPersistedHashMismatch)
	})

	t.Run("rejects empty document", func(t *testing.T) {
		_, err := ValidatePersistedDocument("", "   ")
		require.ErrorIs(t, err, ErrPersistedEmptyDocument)
	})

	t.Run("rejects syntax errors", func(t *testing.T) {
		_, err := ValidatePersistedDocument("", "query { unclosed")
		require.ErrorIs(t, err, ErrPersistedInvalidDocument)
	})

	t.Run("rejects mutations and subscriptions", func(t *testing.T) {
		_, err := ValidatePersistedDocument("", "mutation DoWrite { contract(id: \"C1\") { contractId } }")
		require.ErrorIs(t, err, ErrPersistedInvalidDocument)
		assert.Contains(t, err.Error(), "only Query operations are supported")
	})

	t.Run("rejects multi-operation documents", func(t *testing.T) {
		_, err := ValidatePersistedDocument("", "query A { contracts { totalCount } } query B { contracts { totalCount } }")
		require.ErrorIs(t, err, ErrPersistedInvalidDocument)
		assert.Contains(t, err.Error(), "multiple operations")
	})

	t.Run("rejects queries exceeding depth or static complexity limits", func(t *testing.T) {
		deepQuery := "{ a { b { c { d { e { f { g { h { i { j { k } } } } } } } } } } }"
		_, err := ValidatePersistedDocument("", deepQuery)
		require.ErrorIs(t, err, ErrPersistedInvalidDocument)
		assert.Contains(t, err.Error(), "depth")

		wide := createMassiveComplexityQuery(45)
		_, err = ValidatePersistedDocument("", wide)
		require.ErrorIs(t, err, ErrPersistedInvalidDocument)
		assert.Contains(t, err.Error(), "complexity")
	})
}

func TestPersistedExecution_MatchesFullDocumentAndHandlesUnknownID(t *testing.T) {
	addedAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	st := &stubStore{
		watchedList: []store.WatchedContract{
			{ContractID: knownContractID, AddedAt: addedAt},
		},
	}
	h := newPersistedTestHandler(t, st)

	query := `query GetContract($id: ID!) { contract(id: $id) { contractId addedAt } }`
	regOp, err := h.RegisterPersistedOperation("", query)
	require.NoError(t, err)

	// 1. Full document execution (baseline).
	fullBody, err := json.Marshal(GraphQLRequest{
		Query:         query,
		OperationName: "GetContract",
		Variables:     map[string]any{"id": knownContractID},
	})
	require.NoError(t, err)
	fullRec := httptest.NewRecorder()
	h.ServeHTTP(fullRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(fullBody)))
	require.Equal(t, http.StatusOK, fullRec.Code)

	// 2. Execution by APQ extensions.persistedQuery.sha256Hash alone (no query body).
	apqBody, err := json.Marshal(GraphQLRequest{
		OperationName: "GetContract",
		Variables:     map[string]any{"id": knownContractID},
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{
				Version:    1,
				SHA256Hash: regOp.ID,
			},
		},
	})
	require.NoError(t, err)
	apqRec := httptest.NewRecorder()
	h.ServeHTTP(apqRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(apqBody)))
	require.Equal(t, http.StatusOK, apqRec.Code)
	assert.JSONEq(t, fullRec.Body.String(), apqRec.Body.String())

	// 3. Execution by top-level id field alone.
	idBody, err := json.Marshal(GraphQLRequest{
		ID:        regOp.ID,
		Variables: map[string]any{"id": knownContractID},
	})
	require.NoError(t, err)
	idRec := httptest.NewRecorder()
	h.ServeHTTP(idRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(idBody)))
	require.Equal(t, http.StatusOK, idRec.Code)
	assert.JSONEq(t, fullRec.Body.String(), idRec.Body.String())

	// 4. Execution via GET /graphql with extensions and variables query params.
	extJSON, _ := json.Marshal(GraphQLExtensions{
		PersistedQuery: &PersistedQueryExtension{Version: 1, SHA256Hash: regOp.ID},
	})
	varsJSON, _ := json.Marshal(map[string]any{"id": knownContractID})
	getURL := "/graphql?extensions=" + url.QueryEscape(string(extJSON)) + "&variables=" + url.QueryEscape(string(varsJSON))
	getRec := httptest.NewRecorder()
	h.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, getURL, nil))
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.JSONEq(t, fullRec.Body.String(), getRec.Body.String())

	// 5. Unknown persisted ID returns distinguishable PERSISTED_QUERY_NOT_FOUND code.
	unknownID := ComputePersistedID(`{ contracts { totalCount } }`)
	missBody, err := json.Marshal(GraphQLRequest{
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{
				Version:    1,
				SHA256Hash: unknownID,
			},
		},
	})
	require.NoError(t, err)
	missRec := httptest.NewRecorder()
	h.ServeHTTP(missRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(missBody)))
	require.Equal(t, http.StatusOK, missRec.Code)
	env := decodePersistedEnvelope(t, missRec)
	require.Len(t, env.Errors, 1)
	assert.Equal(t, "PersistedQueryNotFound", env.Errors[0].Message)
	assert.Equal(t, CodePersistedQueryNotFound, env.Errors[0].Extensions["code"])
}

func TestPersistedAPQ_DefaultModeFlowAndHashVerification(t *testing.T) {
	h := newPersistedTestHandler(t, &stubStore{})
	query := `{ contracts { totalCount } }`
	hash := ComputePersistedID(query)

	// Step 1: Hash-only request before registration returns PERSISTED_QUERY_NOT_FOUND.
	step1Body, _ := json.Marshal(GraphQLRequest{
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{Version: 1, SHA256Hash: hash},
		},
	})
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(step1Body)))
	env1 := decodePersistedEnvelope(t, rec1)
	require.Len(t, env1.Errors, 1)
	assert.Equal(t, CodePersistedQueryNotFound, env1.Errors[0].Extensions["code"])

	// Step 2: Mismatched hash + query is rejected and does NOT register.
	badHash := ComputePersistedID(`{ event(id: "x") { id } }`)
	mismatchBody, _ := json.Marshal(GraphQLRequest{
		Query: query,
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{Version: 1, SHA256Hash: badHash},
		},
	})
	recMismatch := httptest.NewRecorder()
	h.ServeHTTP(recMismatch, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(mismatchBody)))
	envMismatch := decodePersistedEnvelope(t, recMismatch)
	require.Len(t, envMismatch.Errors, 1)
	assert.Equal(t, CodePersistedQueryHashMismatch, envMismatch.Errors[0].Extensions["code"])
	assert.Equal(t, 0, h.PersistedRegistry().Count())

	// Step 3: Matching hash + query registers and executes.
	step2Body, _ := json.Marshal(GraphQLRequest{
		Query: query,
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{Version: 1, SHA256Hash: hash},
		},
	})
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(step2Body)))
	env2 := decodePersistedEnvelope(t, rec2)
	assert.Empty(t, env2.Errors)
	assert.NotNil(t, env2.Data["contracts"])
	assert.Equal(t, 1, h.PersistedRegistry().Count())

	// Step 4: Subsequent hash-only request succeeds from registry.
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(step1Body)))
	env3 := decodePersistedEnvelope(t, rec3)
	assert.Empty(t, env3.Errors)
	assert.NotNil(t, env3.Data["contracts"])
}

func TestPersistedAllowlistMode_RefusesAdHocAndUnauthenticatedRegistration(t *testing.T) {
	const operatorKey = "test-operator-secret-key"
	h := newPersistedTestHandler(
		t,
		&stubStore{},
		WithAllowlistOnly(true),
		WithAPIKey(operatorKey),
	)

	registeredQuery := `query AllowedContracts { contracts { totalCount } }`
	regOp, err := h.RegisterPersistedOperation("", registeredQuery)
	require.NoError(t, err)

	// 1. Ad-hoc query without a persisted ID is refused in allowlist mode.
	adhocBody, _ := json.Marshal(GraphQLRequest{Query: registeredQuery})
	adhocRec := httptest.NewRecorder()
	h.ServeHTTP(adhocRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(adhocBody)))
	adhocEnv := decodePersistedEnvelope(t, adhocRec)
	require.Len(t, adhocEnv.Errors, 1)
	assert.Equal(t, CodePersistedQueryOnly, adhocEnv.Errors[0].Extensions["code"])

	// 2. GET /graphql with ad-hoc ?query=... is also refused in allowlist mode.
	getAdhocRec := httptest.NewRecorder()
	h.ServeHTTP(getAdhocRec, httptest.NewRequest(http.MethodGet, "/graphql?query={contracts{totalCount}}", nil))
	getAdhocEnv := decodePersistedEnvelope(t, getAdhocRec)
	require.Len(t, getAdhocEnv.Errors, 1)
	assert.Equal(t, CodePersistedQueryOnly, getAdhocEnv.Errors[0].Extensions["code"])

	// 3. Unauthenticated caller attempting on-the-fly APQ registration is refused.
	newQuery := `query NewQuery($id: ID!) { contract(id: $id) { contractId } }`
	newHash := ComputePersistedID(newQuery)
	apqRegisterBody, _ := json.Marshal(GraphQLRequest{
		Query:     newQuery,
		Variables: map[string]any{"id": knownContractID},
		Extensions: &GraphQLExtensions{
			PersistedQuery: &PersistedQueryExtension{Version: 1, SHA256Hash: newHash},
		},
	})
	unauthRec := httptest.NewRecorder()
	h.ServeHTTP(unauthRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(apqRegisterBody)))
	unauthEnv := decodePersistedEnvelope(t, unauthRec)
	require.Len(t, unauthEnv.Errors, 1)
	assert.Equal(t, CodePersistedQueryOnly, unauthEnv.Errors[0].Extensions["code"])
	_, exists := h.PersistedRegistry().Get(newHash)
	assert.False(t, exists, "unauthenticated APQ request must not register the query in allowlist mode")

	// 4. Pre-registered query executes by identifier alone for unauthenticated clients.
	execBody, _ := json.Marshal(GraphQLRequest{ID: regOp.ID})
	execRec := httptest.NewRecorder()
	h.ServeHTTP(execRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(execBody)))
	execEnv := decodePersistedEnvelope(t, execRec)
	assert.Empty(t, execEnv.Errors)
	assert.NotNil(t, execEnv.Data["contracts"])
}

func TestPersistedExecution_EnforcesVariableValidationAndVariableCostChecks(t *testing.T) {
	st := &stubStore{}
	h := newPersistedTestHandler(t, st, WithAllowlistOnly(true))

	// Register a parameterized query with a required non-null variable and a
	// connection query whose cost scales with $first.
	paramQuery := `query GetEvents($id: ID!, $page: PageInput) {
		event(id: $id) { id }
		c1: contracts(first: 10) { totalCount }
	}`
	paramOp, err := h.RegisterPersistedOperation("", paramQuery)
	require.NoError(t, err)

	t.Run("missing required non-null variable is rejected", func(t *testing.T) {
		body, _ := json.Marshal(GraphQLRequest{
			ID:        paramOp.ID,
			Variables: map[string]any{},
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body)))
		env := decodePersistedEnvelope(t, rec)
		require.Len(t, env.Errors, 1)
		assert.Contains(t, env.Errors[0].Message, "variable $id of non-null type ID! must not be null")
	})

	t.Run("wrong variable type is rejected before execution", func(t *testing.T) {
		body, _ := json.Marshal(GraphQLRequest{
			ID:        paramOp.ID,
			Variables: map[string]any{"id": 12345},
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body)))
		env := decodePersistedEnvelope(t, rec)
		require.Len(t, env.Errors, 1)
		assert.Contains(t, env.Errors[0].Message, "variable $id expected ID (string)")
	})

	t.Run("variable-dependent complexity check rejects high-cost runtime variables", func(t *testing.T) {
		// 5 connection fields: static cost is 5 * (25 + 1) + 5 = 135 <= 1000,
		// so registration succeeds. When executed with $first = 500 (10x default
		// page size of 50), each connection costs 250 -> 5 * 250 = 1250 > 1000.
		var sb strings.Builder
		sb.WriteString("query ScaledConnections($page: PageInput) {")
		for i := 0; i < 5; i++ {
			fmt.Fprintf(&sb, " e%d: events(page: $page) { totalCount }", i)
		}
		sb.WriteString(" }")
		scaledOp, rerr := h.RegisterPersistedOperation("", sb.String())
		require.NoError(t, rerr)

		// Small $first (<= 50) passes complexity check.
		okBody, _ := json.Marshal(GraphQLRequest{
			ID:        scaledOp.ID,
			Variables: map[string]any{"page": map[string]any{"first": 20}},
		})
		okRec := httptest.NewRecorder()
		h.ServeHTTP(okRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(okBody)))
		okEnv := decodePersistedEnvelope(t, okRec)
		assert.Empty(t, okEnv.Errors)

		// Large $first (500) exceeds ComplexityLimit (1000) and is rejected.
		expensiveBody, _ := json.Marshal(GraphQLRequest{
			ID:        scaledOp.ID,
			Variables: map[string]any{"page": map[string]any{"first": 500}},
		})
		expRec := httptest.NewRecorder()
		h.ServeHTTP(expRec, httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(expensiveBody)))
		expEnv := decodePersistedEnvelope(t, expRec)
		require.Len(t, expEnv.Errors, 1)
		assert.Contains(t, expEnv.Errors[0].Message, "exceeds complexity limit")
	})
}

func TestPersistedManagementHTTPAndManifestFileLifecycle(t *testing.T) {
	const apiKey = "release-pipeline-key"
	manifestPath := filepath.Join(t.TempDir(), "persisted-ops.json")

	q1 := `query OpOne { contracts { totalCount } }`
	q1ID := ComputePersistedID(q1)
	q2 := `query OpTwo($id: ID!) { contract(id: $id) { contractId } }`
	q2ID := ComputePersistedID(q2)

	// Seed initial manifest file in standard Apollo/Relay map format.
	initialManifest, err := json.Marshal(map[string]string{q1ID: q1})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, initialManifest, 0o600))

	h := newPersistedTestHandler(
		t,
		&stubStore{},
		WithAllowlistOnly(true),
		WithAPIKey(apiKey),
		WithPersistedOperationsFile(manifestPath),
	)

	// 1. Inspect initial allowlist via GET /graphql/persisted.
	listRec := httptest.NewRecorder()
	h.ServeHTTP(listRec, httptest.NewRequest(http.MethodGet, "/graphql/persisted", nil))
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp struct {
		AllowlistOnly bool                 `json:"allowlistOnly"`
		Count         int                  `json:"count"`
		Operations    []PersistedOperation `json:"operations"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	assert.True(t, listResp.AllowlistOnly)
	require.Len(t, listResp.Operations, 1)
	assert.Equal(t, q1ID, listResp.Operations[0].ID)

	// 2. Unauthenticated POST /graphql/persisted is rejected with 401.
	postBody, _ := json.Marshal(map[string]any{"id": q2ID, "query": q2})
	unauthRec := httptest.NewRecorder()
	h.ServeHTTP(unauthRec, httptest.NewRequest(http.MethodPost, "/graphql/persisted", bytes.NewReader(postBody)))
	assert.Equal(t, http.StatusUnauthorized, unauthRec.Code)

	// 3. Authenticated POST /graphql/persisted registers q2 and persists to file.
	authReq := httptest.NewRequest(http.MethodPost, "/graphql/persisted", bytes.NewReader(postBody))
	authReq.Header.Set("X-API-Key", apiKey)
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, authReq)
	require.Equal(t, http.StatusCreated, postRec.Code)
	assert.Equal(t, 2, h.PersistedRegistry().Count())

	// 4. GET /graphql/persisted/{id} inspects a single operation.
	getOneRec := httptest.NewRecorder()
	h.ServeHTTP(getOneRec, httptest.NewRequest(http.MethodGet, "/graphql/persisted/"+q2ID, nil))
	require.Equal(t, http.StatusOK, getOneRec.Code)

	// 5. PUT /graphql/persisted with a bad hash in the batch fails atomically without losing existing ops.
	badBatch, _ := json.Marshal(map[string]any{
		"operations": []map[string]string{
			{"id": q1ID, "query": q2}, // mismatched hash
		},
	})
	badPutReq := httptest.NewRequest(http.MethodPut, "/graphql/persisted", bytes.NewReader(badBatch))
	badPutReq.Header.Set("Authorization", "Bearer "+apiKey)
	badPutRec := httptest.NewRecorder()
	h.ServeHTTP(badPutRec, badPutReq)
	require.Equal(t, http.StatusBadRequest, badPutRec.Code)
	assert.Equal(t, 2, h.PersistedRegistry().Count(), "failed batch must leave existing allowlist intact")

	// 6. PUT /graphql/persisted with a valid release manifest replaces the allowlist atomically.
	releaseBatch, _ := json.Marshal(map[string]any{
		"operations": []map[string]string{
			{"id": q2ID, "query": q2},
		},
	})
	putReq := httptest.NewRequest(http.MethodPut, "/graphql/persisted", bytes.NewReader(releaseBatch))
	putReq.Header.Set("Authorization", "Bearer "+apiKey)
	putRec := httptest.NewRecorder()
	h.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)
	assert.Equal(t, 1, h.PersistedRegistry().Count())
	_, q1StillThere := h.PersistedRegistry().Get(q1ID)
	assert.False(t, q1StillThere, "replaced allowlist should remove retired operations")

	// 7. Reloading a new Handler from the persisted manifest file preserves the updated allowlist.
	hReloaded := newPersistedTestHandler(t, &stubStore{}, WithPersistedOperationsFile(manifestPath))
	assert.Equal(t, 1, hReloaded.PersistedRegistry().Count())
	_, q2Present := hReloaded.PersistedRegistry().Get(q2ID)
	assert.True(t, q2Present)

	// 8. DELETE /graphql/persisted/{id} removes the operation.
	delReq := httptest.NewRequest(http.MethodDelete, "/graphql/persisted/"+q2ID, nil)
	delReq.Header.Set("X-API-Key", apiKey)
	delRec := httptest.NewRecorder()
	h.ServeHTTP(delRec, delReq)
	require.Equal(t, http.StatusOK, delRec.Code)
	assert.Equal(t, 0, h.PersistedRegistry().Count())

	// 9. Environment variables GRAPHQL_PERSISTED_ONLY and GRAPHQL_PERSISTED_OPERATIONS_FILE
	// and api.Server route mounting (/graphql/persisted and /graphql/persisted/{id}).
	require.NoError(t, os.WriteFile(manifestPath, initialManifest, 0o600))
	t.Setenv("GRAPHQL_PERSISTED_ONLY", "true")
	t.Setenv("GRAPHQL_PERSISTED_OPERATIONS_FILE", manifestPath)

	hEnv := newPersistedTestHandler(t, &stubStore{})
	assert.True(t, hEnv.AllowlistOnly())
	assert.Equal(t, 1, hEnv.PersistedRegistry().Count())

	srv := api.New(&stubStore{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), apiKey)
	srv.SetGraphQLHandler(hEnv, hEnv.PlaygroundHandler())
	router := srv.Router()

	srvListRec := httptest.NewRecorder()
	router.ServeHTTP(srvListRec, httptest.NewRequest(http.MethodGet, "/graphql/persisted", nil))
	require.Equal(t, http.StatusOK, srvListRec.Code)

	srvItemRec := httptest.NewRecorder()
	router.ServeHTTP(srvItemRec, httptest.NewRequest(http.MethodGet, "/graphql/persisted/"+q1ID, nil))
	require.Equal(t, http.StatusOK, srvItemRec.Code)
}
