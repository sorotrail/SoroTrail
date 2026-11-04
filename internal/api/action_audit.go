package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/requestid"
	"github.com/sorotrail/sorotrail/internal/store"
)

const maxAuditBodyPeekBytes = 32 * 1024

type actionAuditHolderCtxKey struct{}

type actionAuditHolder struct {
	hasPrincipal bool
	principal    Principal
	apiKey       *store.APIKey
}

func auditHolderFrom(ctx context.Context) *actionAuditHolder {
	if ctx == nil {
		return nil
	}
	h, _ := ctx.Value(actionAuditHolderCtxKey{}).(*actionAuditHolder)
	return h
}

// SetActionAuditStore configures an explicit ActionAuditStore for the server.
// When unset, the server uses s.store if it implements store.ActionAuditStore,
// or falls back to an in-memory store.ActionAuditStore.
func (s *Server) SetActionAuditStore(as store.ActionAuditStore) {
	s.actionAuditStore = as
}

// ActionAuditStore returns the store.ActionAuditStore used by this server to
// record and query administrative action audit entries (#1277).
func (s *Server) ActionAuditStore() store.ActionAuditStore {
	if s.actionAuditStore != nil {
		return s.actionAuditStore
	}
	s.actionAuditOnce.Do(func() {
		if s.fallbackActionAudit == nil {
			s.fallbackActionAudit = store.NewMemoryActionAuditStore()
		}
	})
	if as, ok := s.store.(store.ActionAuditStore); ok && as != nil {
		return &fallbackActionAuditStore{
			primary:  as,
			fallback: s.fallbackActionAudit,
		}
	}
	return s.fallbackActionAudit
}

type fallbackActionAuditStore struct {
	primary  store.ActionAuditStore
	fallback *store.MemoryActionAuditStore
}

func (f *fallbackActionAuditStore) RecordActionAudit(ctx context.Context, rec store.ActionAuditRecord) (store.ActionAuditRecord, error) {
	out, err := f.primary.RecordActionAudit(ctx, rec)
	if errors.Is(err, store.ErrUnsupported) {
		return f.fallback.RecordActionAudit(ctx, rec)
	}
	return out, err
}

func (f *fallbackActionAuditStore) QueryActionAuditLogs(ctx context.Context, filter store.ActionAuditFilter) ([]store.ActionAuditRecord, error) {
	out, err := f.primary.QueryActionAuditLogs(ctx, filter)
	if errors.Is(err, store.ErrUnsupported) {
		return f.fallback.QueryActionAuditLogs(ctx, filter)
	}
	return out, err
}

func (f *fallbackActionAuditStore) PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	out, err := f.primary.PruneActionAuditLogsBefore(ctx, cutoff, limit)
	if errors.Is(err, store.ErrUnsupported) {
		return f.fallback.PruneActionAuditLogsBefore(ctx, cutoff, limit)
	}
	return out, err
}

// QueryActionAuditLogs queries administrative action audit entries with
// mandatory tenant scoping (#1277). When ctx carries an authenticated non-admin
// tenant Principal, the query is strictly restricted to that tenant's records
// regardless of what Scope was set on filter. When ctx carries an operator or
// admin Principal and filter.Scope is unset, OperatorActionAuditScope is used.
func (s *Server) QueryActionAuditLogs(ctx context.Context, filter store.ActionAuditFilter) ([]store.ActionAuditRecord, error) {
	if p, ok := PrincipalFrom(ctx); ok {
		if !p.Untenanted && !p.Tenant.Admin {
			if p.Tenant.ID <= 0 {
				return nil, store.ErrTenantScopeRequired
			}
			filter.Scope = store.TenantActionAuditScope(p.Tenant.ID)
		} else if !filter.Scope.IsOperator() {
			if _, hasTenant := filter.Scope.TenantID(); !hasTenant {
				filter.Scope = store.OperatorActionAuditScope()
			}
		}
	}
	return s.ActionAuditStore().QueryActionAuditLogs(ctx, filter)
}

// RecordBackfillAction writes an administrative action audit entry for a
// backfill run on the server's ActionAuditStore (#1277).
func (s *Server) RecordBackfillAction(ctx context.Context, rep audit.JobRunReport) (store.ActionAuditRecord, error) {
	return audit.RecordBackfillRun(ctx, s.ActionAuditStore(), rep)
}

// RecordReplayAction writes an administrative action audit entry for a replay
// run on the server's ActionAuditStore (#1277).
func (s *Server) RecordReplayAction(ctx context.Context, rep audit.JobRunReport) (store.ActionAuditRecord, error) {
	return audit.RecordReplayRun(ctx, s.ActionAuditStore(), rep)
}

type actionAuditResponseCapture struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	buf         bytes.Buffer
}

func (c *actionAuditResponseCapture) WriteHeader(code int) {
	if !c.wroteHeader {
		c.status = code
		c.wroteHeader = true
	}
	c.ResponseWriter.WriteHeader(code)
}

func (c *actionAuditResponseCapture) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.status = http.StatusOK
		c.wroteHeader = true
	}
	if rem := maxAuditBodyPeekBytes - c.buf.Len(); rem > 0 {
		if len(b) <= rem {
			c.buf.Write(b)
		} else {
			c.buf.Write(b[:rem])
		}
	}
	return c.ResponseWriter.Write(b)
}

func (c *actionAuditResponseCapture) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (c *actionAuditResponseCapture) Unwrap() http.ResponseWriter {
	return c.ResponseWriter
}

func (c *actionAuditResponseCapture) statusCode() int {
	if !c.wroteHeader {
		return http.StatusOK
	}
	return c.status
}

func isMutatingHTTPMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

type auditBodySniffer struct {
	io.ReadCloser
	buf bytes.Buffer
}

func (s *auditBodySniffer) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if n > 0 {
		if rem := maxAuditBodyPeekBytes - s.buf.Len(); rem > 0 {
			if n <= rem {
				s.buf.Write(p[:n])
			} else {
				s.buf.Write(p[:rem])
			}
		}
	}
	return n, err
}

func (s *Server) adminActionAuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutatingHTTPMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}

		var bodySniffer *auditBodySniffer
		if r.Body != nil && r.Body != http.NoBody {
			bodySniffer = &auditBodySniffer{ReadCloser: r.Body}
			r.Body = bodySniffer
		}

		holder := &actionAuditHolder{}
		ctx := context.WithValue(r.Context(), actionAuditHolderCtxKey{}, holder)
		r = r.WithContext(ctx)

		capture := &actionAuditResponseCapture{ResponseWriter: w}
		next.ServeHTTP(capture, r)

		spec, pathParams, ok := resolveAuditedAction(r)
		if !ok {
			return
		}

		var reqBodyBytes []byte
		if bodySniffer != nil {
			reqBodyBytes = bodySniffer.buf.Bytes()
		}

		rec := s.buildHTTPActionAuditRecord(r, holder, spec, pathParams, reqBodyBytes, capture)
		if _, err := s.ActionAuditStore().RecordActionAudit(r.Context(), rec); err != nil && s.log != nil {
			s.log.Error("recording administrative action audit", "action", rec.Action, "error", err)
		}
	})
}

func resolveAuditedAction(r *http.Request) (audit.ActionSpec, map[string]string, bool) {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pat := rctx.RoutePattern(); pat != "" {
			if spec, ok := audit.ClassifyHTTPMutation(r.Method, pat); ok {
				params := make(map[string]string, len(rctx.URLParams.Keys))
				for i, k := range rctx.URLParams.Keys {
					if i < len(rctx.URLParams.Values) {
						params[k] = rctx.URLParams.Values[i]
					}
				}
				return spec, params, true
			}
		}
	}
	return audit.MatchHTTPMutation(r.Method, r.URL.Path)
}

func (s *Server) buildHTTPActionAuditRecord(
	r *http.Request,
	holder *actionAuditHolder,
	spec audit.ActionSpec,
	pathParams map[string]string,
	reqBody []byte,
	capture *actionAuditResponseCapture,
) store.ActionAuditRecord {
	meta := make(map[string]any)
	reqMap := parseJSONObject(reqBody)
	respMap := parseJSONObject(capture.buf.Bytes())

	actorID, tenantID := s.resolveAuditActorAndTenant(r, holder, spec, pathParams, respMap, meta)
	actorType := audit.ClassifyActorType(
		r.Header.Get("X-Actor-Type"),
		r.Header.Get("User-Agent"),
		false,
	)

	resourceID := extractAuditResourceID(r, spec, pathParams, reqMap, respMap, meta)

	httpStatus := capture.statusCode()
	status := store.ActionAuditStatusSucceeded
	errCode := ""
	if httpStatus >= 400 || httpStatus == 0 {
		status = store.ActionAuditStatusFailed
		errCode = extractAuditErrorCode(httpStatus, respMap)
	}

	reqID := middleware.GetReqID(r.Context())
	if reqID == "" {
		reqID = requestid.FromContext(r.Context())
	}
	if reqID == "" {
		reqID = strings.TrimSpace(r.Header.Get("X-Request-ID"))
	}

	return store.ActionAuditRecord{
		ActorID:      actorID,
		ActorType:    actorType,
		TenantID:     tenantID,
		Action:       spec.Action,
		ResourceType: spec.ResourceType,
		ResourceID:   resourceID,
		Method:       spec.Method,
		Route:        spec.Route,
		RemoteAddr:   r.RemoteAddr,
		RequestID:    reqID,
		Status:       status,
		HTTPStatus:   httpStatus,
		ErrorCode:    errCode,
		Metadata:     meta,
	}
}

func (s *Server) resolveAuditActorAndTenant(
	r *http.Request,
	holder *actionAuditHolder,
	spec audit.ActionSpec,
	pathParams map[string]string,
	respMap map[string]any,
	meta map[string]any,
) (string, *int64) {
	var (
		actorID  string
		tenantID *int64
	)

	if holder != nil && holder.apiKey != nil {
		actorID = apikey.SchemePrefix + holder.apiKey.Prefix
		meta["actor_key_id"] = holder.apiKey.ID
		if holder.apiKey.Name != "" {
			meta["actor_key_name"] = holder.apiKey.Name
		}
	}

	if holder != nil && holder.hasPrincipal && !holder.principal.Untenanted {
		p := holder.principal
		if p.Tenant.ID > 0 {
			tid := p.Tenant.ID
			tenantID = &tid
		}
		if actorID == "" {
			if p.KeyPrefix != "" {
				actorID = apikey.TenantSchemePrefix + p.KeyPrefix
			} else if p.KeyID > 0 {
				actorID = fmt.Sprintf("tenant_key:%d", p.KeyID)
			} else if p.Tenant.ID > 0 {
				actorID = fmt.Sprintf("tenant:%d", p.Tenant.ID)
			}
		}
		if p.KeyID > 0 {
			meta["actor_key_id"] = p.KeyID
		}
	}

	if actorID == "" {
		cred := credentialFromRequest(r)
		if cred == "" {
			cred = apiKeyFromRequest(r)
		}
		if cred != "" {
			if redacted, ok := apikey.RedactIdentifier(cred); ok {
				actorID = redacted
			} else if s.apiKey != "" && subtle.ConstantTimeCompare([]byte(cred), []byte(s.apiKey)) == 1 {
				actorID = "operator:api_key"
			} else {
				actorID = "bearer:redacted"
			}
		} else if holder != nil && holder.hasPrincipal && holder.principal.Untenanted {
			actorID = "operator:local"
		} else {
			actorID = "anonymous"
		}
	}

	// For /admin/tenants/{id}* actions performed by an operator or admin, bind
	// target_tenant_id in metadata (and populate TenantID if unset) so records
	// targeting a tenant are queryable by that tenant's ID.
	if strings.HasPrefix(spec.Route, "/admin/tenants") {
		if idStr := pathParams["id"]; idStr != "" {
			if parsed, err := strconv.ParseInt(idStr, 10, 64); err == nil && parsed > 0 {
				meta["target_tenant_id"] = parsed
				if tenantID == nil {
					tid := parsed
					tenantID = &tid
				}
			}
		} else if idVal, ok := jsonInt64(respMap["id"]); ok && idVal > 0 {
			meta["target_tenant_id"] = idVal
			if tenantID == nil {
				tid := idVal
				tenantID = &tid
			}
		}
	}

	return actorID, tenantID
}

func extractAuditResourceID(
	r *http.Request,
	spec audit.ActionSpec,
	pathParams map[string]string,
	reqMap map[string]any,
	respMap map[string]any,
	meta map[string]any,
) string {
	switch spec.Action {
	case audit.ActionBulkDeleteEvents:
		before := strings.TrimSpace(r.URL.Query().Get("before_ledger"))
		if before != "" {
			meta["before_ledger"] = before
			return "before_ledger:" + before
		}
		return "events"

	case audit.ActionCreateAPIKey:
		if name, ok := reqMap["name"].(string); ok && name != "" {
			meta["name"] = name
		}
		if prefix, ok := respMap["prefix"].(string); ok && prefix != "" {
			meta["key_prefix"] = apikey.SchemePrefix + prefix
		}
		if id, ok := jsonInt64(respMap["id"]); ok && id > 0 {
			meta["key_id"] = id
			return strconv.FormatInt(id, 10)
		}
		if prefix, ok := respMap["prefix"].(string); ok && prefix != "" {
			return apikey.SchemePrefix + prefix
		}
		return ""

	case audit.ActionCreateTenantKey:
		if name, ok := reqMap["name"].(string); ok && name != "" {
			meta["name"] = name
		}
		if prefix, ok := respMap["prefix"].(string); ok && prefix != "" {
			meta["key_prefix"] = apikey.TenantSchemePrefix + prefix
		}
		if id, ok := jsonInt64(respMap["id"]); ok && id > 0 {
			meta["key_id"] = id
			return strconv.FormatInt(id, 10)
		}
		return pathParams["id"]

	case audit.ActionGrantContract:
		cid, _ := reqMap["contract_id"].(string)
		if cid != "" {
			meta["contract_id"] = cid
		}
		if tid := pathParams["id"]; tid != "" && cid != "" {
			return tid + ":" + cid
		}
		return pathParams["id"]

	case audit.ActionRevokeContract:
		tid := pathParams["id"]
		cid := pathParams["contract_id"]
		if cid != "" {
			meta["contract_id"] = cid
		}
		if tid != "" && cid != "" {
			return tid + ":" + cid
		}
		return cid

	case audit.ActionAddWatchedContract, audit.ActionAddTenantWatch:
		cid, _ := reqMap["contract_id"].(string)
		if cid == "" {
			cid, _ = respMap["contract_id"].(string)
		}
		if label, ok := reqMap["label"].(string); ok && label != "" {
			meta["label"] = label
		}
		if netName, ok := reqMap["network"].(string); ok && netName != "" {
			meta["network"] = netName
		}
		return cid

	case audit.ActionCreateSubscription, audit.ActionUpdateSubscription:
		if rawURL, ok := reqMap["url"].(string); ok && rawURL != "" {
			if u, err := url.Parse(rawURL); err == nil && u.User != nil {
				u.User = nil
				rawURL = u.String()
			}
			meta["url"] = rawURL
		}
		if cid, ok := reqMap["contract_id"].(string); ok && cid != "" {
			meta["contract_id"] = cid
		}
		if id := pathParams["id"]; id != "" {
			return id
		}
		if idStr, ok := respMap["id"].(string); ok && idStr != "" {
			return idStr
		}
		if idNum, ok := jsonInt64(respMap["id"]); ok && idNum > 0 {
			return strconv.FormatInt(idNum, 10)
		}
		return ""
	}

	for _, key := range []string{"id", "contract_id", "key_id"} {
		if v := pathParams[key]; v != "" {
			return v
		}
	}
	if idStr, ok := respMap["id"].(string); ok && idStr != "" {
		return idStr
	}
	if idNum, ok := jsonInt64(respMap["id"]); ok && idNum > 0 {
		return strconv.FormatInt(idNum, 10)
	}
	if cid, ok := reqMap["contract_id"].(string); ok && cid != "" {
		return cid
	}
	if name, ok := reqMap["name"].(string); ok && name != "" {
		meta["name"] = name
		return name
	}
	return ""
}

func extractAuditErrorCode(httpStatus int, respMap map[string]any) string {
	if code, ok := respMap["code"].(string); ok && strings.TrimSpace(code) != "" {
		return strings.TrimSpace(code)
	}
	switch httpStatus {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large"
	case http.StatusTooManyRequests:
		return "rate_limited"
	default:
		return fmt.Sprintf("http_%d", httpStatus)
	}
}

func parseJSONObject(raw []byte) map[string]any {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func jsonInt64(v any) (int64, bool) {
	switch typed := v.(type) {
	case float64:
		return int64(typed), true
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		if n, err := strconv.ParseInt(typed, 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}
