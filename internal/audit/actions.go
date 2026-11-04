package audit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sorotrail/sorotrail/internal/requestid"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Canonical administrative action identifiers (#1277).
const (
	ActionCreateAPIKey          = "apikey.create"
	ActionRevokeAPIKey          = "apikey.revoke"
	ActionAddWatchedContract    = "watched_contract.add"
	ActionRemoveWatchedContract = "watched_contract.remove"
	ActionSetSpecOverride       = "contract_spec.set"
	ActionDeleteSpecOverride    = "contract_spec.delete"
	ActionRevertSpecOverride    = "contract_spec.revert"
	ActionDeleteDeadLetter      = "dead_letter.delete"
	ActionBulkDeleteEvents      = "events.bulk_delete"
	ActionCreateSubscription    = "subscription.create"
	ActionUpdateSubscription    = "subscription.update"
	ActionDeleteSubscription    = "subscription.delete"
	ActionAddTenantWatch        = "tenant.watch.add"
	ActionRemoveTenantWatch     = "tenant.watch.remove"
	ActionCreateTenant          = "tenant.create"
	ActionUpdateTenant          = "tenant.update"
	ActionDeleteTenant          = "tenant.delete"
	ActionGrantContract         = "tenant.grant.add"
	ActionRevokeContract        = "tenant.grant.revoke"
	ActionCreateTenantKey       = "tenant.key.create"
	ActionRevokeTenantKey       = "tenant.key.revoke"
	ActionRunBackfill           = "backfill.run"
	ActionRunReplay             = "replay.run"
)

// ActionSpec describes one administrative action in the inventory (#1277).
type ActionSpec struct {
	Action       string
	ResourceType string
	Method       string
	Route        string
	Description  string
}

var httpMutationInventory = []ActionSpec{
	{
		Action:       ActionBulkDeleteEvents,
		ResourceType: "events",
		Method:       "DELETE",
		Route:        "/events",
		Description:  "Bulk delete events before a target ledger",
	},
	{
		Action:       ActionAddWatchedContract,
		ResourceType: "watched_contract",
		Method:       "POST",
		Route:        "/watched-contracts",
		Description:  "Add a contract to the global watch list",
	},
	{
		Action:       ActionRemoveWatchedContract,
		ResourceType: "watched_contract",
		Method:       "DELETE",
		Route:        "/watched-contracts/{id}",
		Description:  "Remove a contract from the global watch list",
	},
	{
		Action:       ActionSetSpecOverride,
		ResourceType: "contract_spec",
		Method:       "PUT",
		Route:        "/contracts/{id}/spec",
		Description:  "Upload or replace a contract spec override",
	},
	{
		Action:       ActionDeleteSpecOverride,
		ResourceType: "contract_spec",
		Method:       "DELETE",
		Route:        "/contracts/{id}/spec",
		Description:  "Delete a contract spec override",
	},
	{
		Action:       ActionRevertSpecOverride,
		ResourceType: "contract_spec",
		Method:       "POST",
		Route:        "/contracts/{id}/spec/revert",
		Description:  "Revert a contract spec override to a previous revision",
	},
	{
		Action:       ActionDeleteDeadLetter,
		ResourceType: "dead_letter",
		Method:       "DELETE",
		Route:        "/dead-letters/{id}",
		Description:  "Delete a dead-letter entry",
	},
	{
		Action:       ActionCreateAPIKey,
		ResourceType: "apikey",
		Method:       "POST",
		Route:        "/apikeys",
		Description:  "Create an API key (records ID and prefix only, never the secret)",
	},
	{
		Action:       ActionRevokeAPIKey,
		ResourceType: "apikey",
		Method:       "DELETE",
		Route:        "/apikeys/{id}",
		Description:  "Revoke an API key by ID",
	},
	{
		Action:       ActionCreateSubscription,
		ResourceType: "subscription",
		Method:       "POST",
		Route:        "/subscriptions",
		Description:  "Create a webhook subscription (never records webhook secret)",
	},
	{
		Action:       ActionUpdateSubscription,
		ResourceType: "subscription",
		Method:       "PUT",
		Route:        "/subscriptions/{id}",
		Description:  "Update a webhook subscription (never records webhook secret)",
	},
	{
		Action:       ActionDeleteSubscription,
		ResourceType: "subscription",
		Method:       "DELETE",
		Route:        "/subscriptions/{id}",
		Description:  "Delete a webhook subscription",
	},
	{
		Action:       ActionAddTenantWatch,
		ResourceType: "watched_contract",
		Method:       "POST",
		Route:        "/tenant/watch",
		Description:  "Add a contract to a tenant's watch list",
	},
	{
		Action:       ActionRemoveTenantWatch,
		ResourceType: "watched_contract",
		Method:       "DELETE",
		Route:        "/tenant/watch/{contract_id}",
		Description:  "Remove a contract from a tenant's watch list",
	},
	{
		Action:       ActionCreateTenant,
		ResourceType: "tenant",
		Method:       "POST",
		Route:        "/admin/tenants",
		Description:  "Create a tenant",
	},
	{
		Action:       ActionUpdateTenant,
		ResourceType: "tenant",
		Method:       "PATCH",
		Route:        "/admin/tenants/{id}",
		Description:  "Update a tenant's state, quotas, or display name",
	},
	{
		Action:       ActionDeleteTenant,
		ResourceType: "tenant",
		Method:       "DELETE",
		Route:        "/admin/tenants/{id}",
		Description:  "Delete a tenant",
	},
	{
		Action:       ActionGrantContract,
		ResourceType: "tenant_grant",
		Method:       "POST",
		Route:        "/admin/tenants/{id}/grants",
		Description:  "Grant a contract to a tenant",
	},
	{
		Action:       ActionRevokeContract,
		ResourceType: "tenant_grant",
		Method:       "DELETE",
		Route:        "/admin/tenants/{id}/grants/{contract_id}",
		Description:  "Revoke a contract grant from a tenant",
	},
	{
		Action:       ActionCreateTenantKey,
		ResourceType: "tenant_key",
		Method:       "POST",
		Route:        "/admin/tenants/{id}/keys",
		Description:  "Create a tenant-scoped API key (records ID and prefix only, never the secret)",
	},
	{
		Action:       ActionRevokeTenantKey,
		ResourceType: "tenant_key",
		Method:       "DELETE",
		Route:        "/admin/keys/{key_id}",
		Description:  "Revoke a tenant-scoped API key",
	},
}

var backgroundActionInventory = []ActionSpec{
	{
		Action:       ActionRunBackfill,
		ResourceType: "backfill",
		Method:       "JOB",
		Route:        "backfill",
		Description:  "Execute a ledger-range backfill job",
	},
	{
		Action:       ActionRunReplay,
		ResourceType: "replay",
		Method:       "JOB",
		Route:        "replay",
		Description:  "Execute a ledger-range replay job",
	},
}

// AllAdminActions returns the complete inventory of HTTP and background
// administrative actions tracked by SoroTrail (#1277).
func AllAdminActions() []ActionSpec {
	out := make([]ActionSpec, 0, len(httpMutationInventory)+len(backgroundActionInventory))
	out = append(out, httpMutationInventory...)
	out = append(out, backgroundActionInventory...)
	return out
}

// HTTPMutationActions returns the inventory of HTTP mutating administrative
// endpoints.
func HTTPMutationActions() []ActionSpec {
	out := make([]ActionSpec, len(httpMutationInventory))
	copy(out, httpMutationInventory)
	return out
}

// ClassifyHTTPMutation maps an HTTP method and chi route pattern to its
// canonical administrative ActionSpec.
func ClassifyHTTPMutation(method, routePattern string) (ActionSpec, bool) {
	m := strings.ToUpper(strings.TrimSpace(method))
	r := strings.TrimSpace(routePattern)
	if r != "/" {
		r = strings.TrimRight(r, "/")
	}
	for _, spec := range httpMutationInventory {
		if spec.Method == m && spec.Route == r {
			return spec, true
		}
	}
	return ActionSpec{}, false
}

// MatchHTTPMutation matches an HTTP method and concrete URL path (or route
// pattern) against the HTTP mutation inventory, returning the matched
// ActionSpec and any extracted path parameters (e.g. "id", "contract_id",
// "key_id"). This allows early middleware rejections (such as 401/403 before
// leaf route dispatch) to be attributed to the exact administrative action.
func MatchHTTPMutation(method, rawPath string) (ActionSpec, map[string]string, bool) {
	if spec, ok := ClassifyHTTPMutation(method, rawPath); ok {
		return spec, map[string]string{}, true
	}
	m := strings.ToUpper(strings.TrimSpace(method))
	cleanPath := strings.TrimSpace(rawPath)
	if cleanPath != "/" {
		cleanPath = strings.TrimRight(cleanPath, "/")
	}
	pathSegs := strings.Split(strings.TrimPrefix(cleanPath, "/"), "/")

	for _, spec := range httpMutationInventory {
		if spec.Method != m {
			continue
		}
		routeSegs := strings.Split(strings.TrimPrefix(spec.Route, "/"), "/")
		if len(routeSegs) != len(pathSegs) {
			continue
		}
		params := make(map[string]string)
		matched := true
		for i, rSeg := range routeSegs {
			pSeg := pathSegs[i]
			if strings.HasPrefix(rSeg, "{") && strings.HasSuffix(rSeg, "}") {
				if pSeg == "" {
					matched = false
					break
				}
				paramName := rSeg[1 : len(rSeg)-1]
				params[paramName] = pSeg
				continue
			}
			if rSeg != pSeg {
				matched = false
				break
			}
		}
		if matched {
			return spec, params, true
		}
	}
	return ActionSpec{}, nil, false
}

// ClassifyActorType determines whether an action was initiated by a human
// operator ("person") or by automation ("automation") based on explicit header
// hints, user-agent strings, or background-job execution context.
func ClassifyActorType(explicitHeader, userAgent string, isBackgroundJob bool) string {
	if isBackgroundJob {
		return store.ActorTypeAutomation
	}
	switch strings.ToLower(strings.TrimSpace(explicitHeader)) {
	case store.ActorTypeAutomation, "bot", "ci", "scheduled_job", "service":
		return store.ActorTypeAutomation
	case store.ActorTypePerson, "human", "user", "operator":
		return store.ActorTypePerson
	}
	ua := strings.ToLower(strings.TrimSpace(userAgent))
	for _, marker := range []string{
		"bot", "automation", "github-actions", "gitlab-ci", "jenkins",
		"circleci", "cron", "scheduler", "terraform", "ansible", "sorotrail-automation",
	} {
		if strings.Contains(ua, marker) {
			return store.ActorTypeAutomation
		}
	}
	return store.ActorTypePerson
}

// JobRunReport describes a programmatic backfill or replay execution to be
// recorded in the administrative action audit log (#1277).
type JobRunReport struct {
	ActorID    string
	ActorType  string
	TenantID   *int64
	FromLedger int64
	ToLedger   int64
	ContractID string
	RemoteAddr string
	RequestID  string
	Err        error
	CreatedAt  time.Time
}

// RecordBackfillRun writes an administrative action audit entry for a backfill
// run (succeeded or failed).
func RecordBackfillRun(ctx context.Context, st store.ActionAuditStore, rep JobRunReport) (store.ActionAuditRecord, error) {
	return recordJobRun(ctx, st, ActionRunBackfill, "backfill", rep)
}

// RecordReplayRun writes an administrative action audit entry for a replay
// run (succeeded or failed).
func RecordReplayRun(ctx context.Context, st store.ActionAuditStore, rep JobRunReport) (store.ActionAuditRecord, error) {
	return recordJobRun(ctx, st, ActionRunReplay, "replay", rep)
}

func recordJobRun(ctx context.Context, st store.ActionAuditStore, action, resourceType string, rep JobRunReport) (store.ActionAuditRecord, error) {
	actorID := strings.TrimSpace(rep.ActorID)
	if actorID == "" {
		actorID = "system:" + resourceType
	}
	actorType := rep.ActorType
	if actorType == "" {
		actorType = store.ActorTypeAutomation
	}
	reqID := strings.TrimSpace(rep.RequestID)
	if reqID == "" {
		reqID = requestid.FromContext(ctx)
	}
	remoteAddr := strings.TrimSpace(rep.RemoteAddr)
	if remoteAddr == "" {
		remoteAddr = "local"
	}
	resourceID := fmt.Sprintf("%d..%d", rep.FromLedger, rep.ToLedger)
	status := store.ActionAuditStatusSucceeded
	errCode := ""
	if rep.Err != nil {
		status = store.ActionAuditStatusFailed
		errCode = "job_failed"
	}
	meta := map[string]any{
		"from_ledger": rep.FromLedger,
		"to_ledger":   rep.ToLedger,
	}
	if rep.ContractID != "" {
		meta["contract_id"] = rep.ContractID
	}
	if rep.Err != nil {
		meta["error"] = rep.Err.Error()
	}

	return st.RecordActionAudit(ctx, store.ActionAuditRecord{
		ActorID:      actorID,
		ActorType:    actorType,
		TenantID:     rep.TenantID,
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Method:       "JOB",
		Route:        resourceType,
		RemoteAddr:   remoteAddr,
		RequestID:    reqID,
		Status:       status,
		HTTPStatus:   0,
		ErrorCode:    errCode,
		Metadata:     meta,
		CreatedAt:    rep.CreatedAt,
	})
}
