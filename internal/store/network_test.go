package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSQLiteNetworkIsolation(t *testing.T) {
	st := newSQLiteStore(t)
	ctx := context.Background()

	for _, network := range []string{"testnet", "mainnet"} {
		if err := st.SaveIngestionState(ctx, IngestionState{Network: network, LastIngestedLedger: 42, LastCursor: network + "-cursor"}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.UpsertEvents(ctx, []Event{{ID: "same-id", Network: network, ContractID: "C", Ledger: 42, Type: "contract", Topics: json.RawMessage("[]"), Value: json.RawMessage("null")}}); err != nil {
			t.Fatal(err)
		}
	}

	for _, network := range []string{"testnet", "mainnet"} {
		state, err := st.(*SQLite).GetIngestionStateForNetwork(ctx, network)
		if err != nil {
			t.Fatal(err)
		}
		if state.LastCursor != network+"-cursor" {
			t.Fatalf("%s cursor = %q", network, state.LastCursor)
		}
		events, _, err := st.QueryEvents(ctx, EventFilter{Network: network, Scope: SystemScope(), Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 1 || events[0].Network != network {
			t.Fatalf("%s events = %#v", network, events)
		}
	}
}
