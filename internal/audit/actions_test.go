package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/requestid"
	"github.com/sorotrail/sorotrail/internal/store"
)

func TestAllAdminActions_InventoryAndClassification(t *testing.T) {
	all := AllAdminActions()
	require.NotEmpty(t, all)

	seenActions := map[string]bool{}
	for _, spec := range all {
		assert.NotEmpty(t, spec.Action)
		assert.NotEmpty(t, spec.ResourceType)
		assert.NotEmpty(t, spec.Method)
		assert.NotEmpty(t, spec.Route)
		assert.NotEmpty(t, spec.Description)
		assert.False(t, seenActions[spec.Action], "duplicate action identifier %s", spec.Action)
		seenActions[spec.Action] = true
	}

	for _, httpSpec := range HTTPMutationActions() {
		got, ok := ClassifyHTTPMutation(httpSpec.Method, httpSpec.Route)
		require.True(t, ok, "expected %s %s to be classified", httpSpec.Method, httpSpec.Route)
		assert.Equal(t, httpSpec.Action, got.Action)
		assert.Equal(t, httpSpec.ResourceType, got.ResourceType)
	}

	_, ok := ClassifyHTTPMutation("GET", "/events")
	assert.False(t, ok, "read-only GET endpoint must not classify as an HTTP mutation")
}

func TestClassifyActorType(t *testing.T) {
	assert.Equal(t, store.ActorTypeAutomation, ClassifyActorType("", "", true))
	assert.Equal(t, store.ActorTypeAutomation, ClassifyActorType("automation", "Mozilla/5.0", false))
	assert.Equal(t, store.ActorTypeAutomation, ClassifyActorType("bot", "Mozilla/5.0", false))
	assert.Equal(t, store.ActorTypeAutomation, ClassifyActorType("", "GitHub-Actions/2.0", false))
	assert.Equal(t, store.ActorTypeAutomation, ClassifyActorType("", "sorotrail-automation/1.0", false))
	assert.Equal(t, store.ActorTypePerson, ClassifyActorType("person", "GitHub-Actions/2.0", false))
	assert.Equal(t, store.ActorTypePerson, ClassifyActorType("", "curl/8.6.0", false))
}

func TestRecordBackfillAndReplayRuns(t *testing.T) {
	st := store.NewMemoryActionAuditStore()
	ctx := requestid.WithRequestID(context.Background(), "req-job-42")

	bfRec, err := RecordBackfillRun(ctx, st, JobRunReport{
		ActorID:    "sorotrail_0123456789abcdef_deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		FromLedger: 1000,
		ToLedger:   2000,
		ContractID: "CCONTRACT1",
	})
	require.NoError(t, err)
	assert.Equal(t, ActionRunBackfill, bfRec.Action)
	assert.Equal(t, "backfill", bfRec.ResourceType)
	assert.Equal(t, "1000..2000", bfRec.ResourceID)
	assert.Equal(t, "sorotrail_0123456789abcdef", bfRec.ActorID, "raw API key secret must be redacted")
	assert.Equal(t, store.ActorTypeAutomation, bfRec.ActorType)
	assert.Equal(t, "req-job-42", bfRec.RequestID)
	assert.Equal(t, store.ActionAuditStatusSucceeded, bfRec.Status)

	rpRec, err := RecordReplayRun(ctx, st, JobRunReport{
		FromLedger: 3000,
		ToLedger:   3500,
		Err:        errors.New("rpc timeout"),
	})
	require.NoError(t, err)
	assert.Equal(t, ActionRunReplay, rpRec.Action)
	assert.Equal(t, "replay", rpRec.ResourceType)
	assert.Equal(t, "3000..3500", rpRec.ResourceID)
	assert.Equal(t, "system:replay", rpRec.ActorID)
	assert.Equal(t, store.ActionAuditStatusFailed, rpRec.Status)
	assert.Equal(t, "job_failed", rpRec.ErrorCode)
	assert.Equal(t, bfRec.EntryHash, rpRec.PrevHash)

	logs, err := st.QueryActionAuditLogs(ctx, store.ActionAuditFilter{
		Scope: store.OperatorActionAuditScope(),
	})
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.NoError(t, store.VerifyActionAuditChain(logs))
}
