package store

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingReplicaStore struct {
	Store
	calls             []string
	failWith          error
	events            []Event
	ingestionLedger   int64
	contractSummaries []ContractSummary
	stats             Stats
}

func (r *recordingReplicaStore) record(name string) {
	r.calls = append(r.calls, name)
}

func (r *recordingReplicaStore) Ping(_ context.Context) error {
	r.record("Ping")
	return r.failWith
}

func (r *recordingReplicaStore) GetIngestionState(_ context.Context) (IngestionState, error) {
	r.record("GetIngestionState")
	if r.failWith != nil {
		return IngestionState{}, r.failWith
	}
	return IngestionState{LastIngestedLedger: r.ingestionLedger}, nil
}

func (r *recordingReplicaStore) SaveIngestionState(_ context.Context, s IngestionState) error {
	r.record("SaveIngestionState")
	r.ingestionLedger = s.LastIngestedLedger
	return r.failWith
}

func (r *recordingReplicaStore) UpsertEvents(_ context.Context, evs []Event) (int64, error) {
	r.record("UpsertEvents")
	if r.failWith != nil {
		return 0, r.failWith
	}
	r.events = append(r.events, evs...)
	for _, e := range evs {
		if e.Ledger > r.ingestionLedger {
			r.ingestionLedger = e.Ledger
		}
	}
	return int64(len(evs)), nil
}

func (r *recordingReplicaStore) GetEvent(_ context.Context, id string, sc Scope) (Event, error) {
	r.record("GetEvent")
	if r.failWith != nil {
		return Event{}, r.failWith
	}
	for _, e := range r.events {
		if e.ID == id && sc.Allows(e.ContractID) {
			return e, nil
		}
	}
	return Event{}, ErrNotFound
}

func (r *recordingReplicaStore) EventExists(_ context.Context, id string, sc Scope) (bool, error) {
	r.record("EventExists")
	if r.failWith != nil {
		return false, r.failWith
	}
	for _, e := range r.events {
		if e.ID == id && sc.Allows(e.ContractID) {
			return true, nil
		}
	}
	return false, nil
}

func (r *recordingReplicaStore) GetEventsByTxHash(_ context.Context, txHash, excludeID string) ([]Event, error) {
	r.record("GetEventsByTxHash")
	if r.failWith != nil {
		return nil, r.failWith
	}
	var out []Event
	for _, e := range r.events {
		if e.TxHash == txHash && e.ID != excludeID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (r *recordingReplicaStore) QueryEvents(_ context.Context, f EventFilter) ([]Event, string, error) {
	r.record("QueryEvents")
	if r.failWith != nil {
		return nil, "", r.failWith
	}
	var out []Event
	for _, e := range r.events {
		if !f.Scope.Allows(e.ContractID) {
			continue
		}
		if f.ContractID != "" && e.ContractID != f.ContractID {
			continue
		}
		out = append(out, e)
	}
	return out, "", nil
}

func (r *recordingReplicaStore) CountEvents(_ context.Context, f EventFilter) (int64, error) {
	r.record("CountEvents")
	if r.failWith != nil {
		return 0, r.failWith
	}
	evs, _, err := r.QueryEvents(context.Background(), f)
	return int64(len(evs)), err
}

func (r *recordingReplicaStore) AggregateEvents(_ context.Context, _ EventFilter, _ string) ([]AggregateBucket, error) {
	r.record("AggregateEvents")
	if r.failWith != nil {
		return nil, r.failWith
	}
	return []AggregateBucket{{Bucket: "100", Count: int64(len(r.events))}}, nil
}

func (r *recordingReplicaStore) ListContracts(_ context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	r.record("ListContracts")
	if r.failWith != nil {
		return nil, "", r.failWith
	}
	var out []ContractSummary
	for _, cs := range r.contractSummaries {
		if f.Scope.Allows(cs.ContractID) {
			out = append(out, cs)
		}
	}
	return out, "", nil
}

func (r *recordingReplicaStore) CountContracts(_ context.Context, f ContractsFilter) (int64, error) {
	r.record("CountContracts")
	if r.failWith != nil {
		return 0, r.failWith
	}
	list, _, err := r.ListContracts(context.Background(), f)
	return int64(len(list)), err
}

func (r *recordingReplicaStore) GetContractSummary(_ context.Context, id string) (ContractSummary, error) {
	r.record("GetContractSummary")
	if r.failWith != nil {
		return ContractSummary{}, r.failWith
	}
	for _, cs := range r.contractSummaries {
		if cs.ContractID == id {
			return cs, nil
		}
	}
	return ContractSummary{}, ErrNotFound
}

func (r *recordingReplicaStore) ContractEventTypeCounts(_ context.Context, _ string) ([]ContractEventTypeCount, error) {
	r.record("ContractEventTypeCounts")
	if r.failWith != nil {
		return nil, r.failWith
	}
	return []ContractEventTypeCount{{Type: "contract", Count: 1}}, nil
}

func (r *recordingReplicaStore) Stats(_ context.Context, _ Scope) (Stats, error) {
	r.record("Stats")
	if r.failWith != nil {
		return Stats{}, r.failWith
	}
	return r.stats, nil
}

func (r *recordingReplicaStore) QueryAddressEvents(_ context.Context, _ string, f EventFilter) ([]Event, string, error) {
	r.record("QueryAddressEvents")
	if r.failWith != nil {
		return nil, "", r.failWith
	}
	return r.QueryEvents(context.Background(), f)
}

func (r *recordingReplicaStore) CountAddressEvents(_ context.Context, _ string) (int64, error) {
	r.record("CountAddressEvents")
	if r.failWith != nil {
		return 0, r.failWith
	}
	return int64(len(r.events)), nil
}

func (r *recordingReplicaStore) GetAddressSummary(_ context.Context, addr string) (AddressSummary, error) {
	r.record("GetAddressSummary")
	if r.failWith != nil {
		return AddressSummary{}, r.failWith
	}
	if len(r.events) == 0 {
		return AddressSummary{}, ErrNotFound
	}
	return AddressSummary{Address: addr, EventCount: int64(len(r.events))}, nil
}

func (r *recordingReplicaStore) GetContractMeta(_ context.Context, id string) (ContractMeta, error) {
	r.record("GetContractMeta")
	if r.failWith != nil {
		return ContractMeta{}, r.failWith
	}
	return ContractMeta{ContractID: id}, nil
}

func (r *recordingReplicaStore) CountContractEvents(_ context.Context, _ string) (int64, error) {
	r.record("CountContractEvents")
	if r.failWith != nil {
		return 0, r.failWith
	}
	return int64(len(r.events)), nil
}

func (r *recordingReplicaStore) GetContractCursor(_ context.Context, id string) (ContractCursor, error) {
	r.record("GetContractCursor")
	return ContractCursor{ContractID: id, LastIngestedLedger: r.ingestionLedger}, nil
}

// TestReplicaRouterOnlyOverridesAllowlistedMethods parses replica.go via go/ast
// and asserts that the methods defined on *ReplicaRouter that touch r.replica
// match ReplicaSafeMethods() in both directions (#1222).
func TestReplicaRouterOnlyOverridesAllowlistedMethods(t *testing.T) {
	allowed := ReplicaSafeMethods()
	for name, reason := range allowed {
		assert.NotEmpty(t, strings.TrimSpace(reason), "ReplicaSafeMethods entry %s must carry a reason", name)
	}

	fset := token.NewFileSet()
	fAST, err := parser.ParseFile(fset, "replica.go", nil, 0)
	require.NoError(t, err)

	routedOnType := make(map[string]bool)
	helperMethods := map[string]bool{
		"SampleReplicaLag":      true,
		"SetObservedReplicaLag": true,
		"ObservedReplicaLag":    true,
		"MaxReplicaLag":         true,
		"useReplica":            true,
		"cursorAheadOfReplica":  true,
		"Ping":                  true,
	}

	for _, decl := range fAST.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
			continue
		}
		name := fn.Name.Name
		if helperMethods[name] {
			continue
		}
		routedOnType[name] = true
	}

	for name := range routedOnType {
		_, ok := allowed[name]
		assert.Truef(t, ok, "ReplicaRouter method %s is not listed in ReplicaSafeMethods()", name)
	}
	for name := range allowed {
		assert.Truef(t, routedOnType[name], "ReplicaSafeMethods() lists %s, which ReplicaRouter does not implement", name)
	}
}

func TestReplicaRouterRoutingFallbackLagCursorAndReadYourWrites(t *testing.T) {
	ctx := context.Background()
	const contractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	sc := NewScope([]string{contractA})

	primary := &recordingReplicaStore{
		ingestionLedger:   200,
		contractSummaries: []ContractSummary{{ContractID: contractA, EventCount: 2}},
		stats:             Stats{TotalEvents: 2, ContractCount: 1},
	}
	replica := &recordingReplicaStore{
		ingestionLedger:   150,
		contractSummaries: []ContractSummary{{ContractID: contractA, EventCount: 1}},
		stats:             Stats{TotalEvents: 1, ContractCount: 1},
		events: []Event{
			{ID: "00000000000000000100-0000000001", ContractID: contractA, Ledger: 100, TxHash: "tx1"},
		},
	}

	router := NewReplicaRouter(primary, replica, ReplicaRouterOptions{
		MaxReplicaLag: 10 * time.Second,
	})

	t.Run("routed reads go to replica while primary-only reads stay on primary", func(t *testing.T) {
		primary.calls = nil
		replica.calls = nil

		// Primary-only: GetIngestionState and GetContractCursor must never touch replica.
		state, err := router.GetIngestionState(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(200), state.LastIngestedLedger)
		_, err = router.GetContractCursor(ctx, contractA)
		require.NoError(t, err)
		assert.Contains(t, primary.calls, "GetIngestionState")
		assert.Contains(t, primary.calls, "GetContractCursor")
		assert.Empty(t, replica.calls, "primary-only methods must never touch the replica")

		// Replica-safe reads go to replica.
		evs, _, err := router.QueryEvents(ctx, EventFilter{Scope: sc})
		require.NoError(t, err)
		require.Len(t, evs, 1)
		st, err := router.Stats(ctx, sc)
		require.NoError(t, err)
		assert.Equal(t, int64(1), st.TotalEvents)
		assert.Contains(t, replica.calls, "QueryEvents")
		assert.Contains(t, replica.calls, "Stats")
	})

	t.Run("write followed immediately by its own read succeeds even when replica lags", func(t *testing.T) {
		// Write a new event at ledger 200 (goes to primary; replica only has ledger 100).
		newEv := Event{ID: "00000000000000000200-0000000001", ContractID: contractA, Ledger: 200, TxHash: "tx2"}
		_, err := router.UpsertEvents(ctx, []Event{newEv})
		require.NoError(t, err)

		// Immediate GetEvent and EventExists for the newly written event fall back to primary when replica lacks it.
		got, err := router.GetEvent(ctx, newEv.ID, sc)
		require.NoError(t, err)
		assert.Equal(t, newEv.ID, got.ID)

		exists, err := router.EventExists(ctx, newEv.ID, sc)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("cursor minted on primary ahead of replica falls back to primary so rows are not skipped", func(t *testing.T) {
		primary.calls = nil
		replica.calls = nil

		// Cursor at ledger 190 is ahead of replica's LastIngestedLedger (150).
		cursorAhead := EncodeCursor(OrderByLedger, Event{ID: "00000000000000000190-0000000001", Ledger: 190})
		evs, _, err := router.QueryEvents(ctx, EventFilter{Scope: sc, Cursor: cursorAhead})
		require.NoError(t, err)
		assert.NotEmpty(t, evs)
		assert.Contains(t, primary.calls, "QueryEvents", "cursor ahead of replica must fall back to primary")
		assert.NotContains(t, replica.calls, "QueryEvents")
	})

	t.Run("exceeding MaxReplicaLag routes reads to primary", func(t *testing.T) {
		primary.calls = nil
		replica.calls = nil
		router.SetObservedReplicaLag(25 * time.Second)
		defer router.SetObservedReplicaLag(0)

		st, err := router.Stats(ctx, sc)
		require.NoError(t, err)
		assert.Equal(t, int64(2), st.TotalEvents, "lagging replica must fall back to primary Stats")
		assert.Contains(t, primary.calls, "Stats")
		assert.Empty(t, replica.calls)
	})

	t.Run("replica connection failure falls back to primary and Ping stays green", func(t *testing.T) {
		primary.calls = nil
		replica.calls = nil
		replica.failWith = errors.New("connection refused")
		defer func() { replica.failWith = nil }()

		evs, _, err := router.QueryEvents(ctx, EventFilter{Scope: sc})
		require.NoError(t, err)
		assert.NotEmpty(t, evs)
		assert.Contains(t, primary.calls, "QueryEvents")

		// Readiness Ping checks only the primary, so a replica outage does not fail /readyz.
		require.NoError(t, router.Ping(ctx))
		assert.NotContains(t, replica.calls, "Ping")
	})
}
