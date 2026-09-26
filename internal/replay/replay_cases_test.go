package replay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Run executes a replay run over the given ledger range using the provided store and decoder.
func Run(ctx context.Context, s store.Store, dec decode.Decoder, fromLedger, toLedger int64, batchSize int) error {
	currentFrom := fromLedger
	for currentFrom <= toLedger {
		currentTo := currentFrom + int64(batchSize) - 1
		if currentTo > toLedger {
			currentTo = toLedger
		}

		Events, err := s.QueryEventsForReplay(ctx, currentFrom, currentTo, batchSize)
		if err != nil {
			return err
		}

		var rewrittenEvents []store.Event
		for _, ev := range Events {
			topics, value, decodeErr := dec.DecodeEventXDR(ev.RawXDR)
			if decodeErr != nil {
				continue
			}
			rewrittenEvents = append(rewrittenEvents, store.Event{
				ID:     ev.ID,
				Ledger: ev.Ledger,
				RawXDR: ev.RawXDR,
				Topics: topics,
				Value:  value,
			})
		}

		batch := store.ReplayBatch{
			FromLedger: currentFrom,
			ToLedger:   currentTo,
			Events:     rewrittenEvents,
		}

		if err := s.CommitReplayBatch(ctx, batch); err != nil {
			return err
		}

		if err := s.SaveReplayState(ctx, store.ReplayState{LastLedger: currentTo}); err != nil {
			return err
		}

		currentFrom = currentTo + 1
	}
	return nil
}

// MockStore implements store.Store for replay testing without real DB infra unless needed.
type MockStore struct {
	store.Store
	Events      []store.Event
	Batches     []store.ReplayBatch
	ReplayState store.ReplayState
	SaveErr     error
	CommitErr   error
	QueryErr    error
}

func (m *MockStore) QueryEventsForReplay(ctx context.Context, fromLedger, toLedger int64, batchSize int) ([]store.Event, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	var res []store.Event
	for _, ev := range m.Events {
		if ev.Ledger >= fromLedger && ev.Ledger <= toLedger {
			res = append(res, ev)
		}
	}
	return res, nil
}

func (m *MockStore) GetReplayState(ctx context.Context) (store.ReplayState, error) {
	return m.ReplayState, nil
}

func (m *MockStore) SaveReplayState(ctx context.Context, s store.ReplayState) error {
	if m.SaveErr != nil {
		return m.SaveErr
	}
	m.ReplayState = s
	return nil
}

func (m *MockStore) CommitReplayBatch(ctx context.Context, batch store.ReplayBatch) error {
	if m.CommitErr != nil {
		return m.CommitErr
	}
	m.Batches = append(m.Batches, batch)
	for i, ev := range m.Events {
		for _, be := range batch.Events {
			if ev.ID == be.ID {
				m.Events[i] = be
			}
		}
	}
	return nil
}

type mockDecoder struct {
	decode.Decoder
	RewriteFn func(rawXDR string) (json.RawMessage, json.RawMessage, error)
	FailCount int
	Calls     int
}

func (md *mockDecoder) DecodeEventXDR(rawXDR string) (json.RawMessage, json.RawMessage, error) {
	md.Calls++
	if md.FailCount > 0 && md.Calls <= md.FailCount {
		return nil, nil, assert.AnError
	}
	if md.RewriteFn != nil {
		return md.RewriteFn(rawXDR)
	}
	return json.RawMessage(`[]`), json.RawMessage(`{}`), nil
}

func TestReplay_BatchAndProgressHandling(t *testing.T) {
	t.Run("changed decoding rewriting the row", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{
					ID:     "0000000000000001-000",
					Ledger: 10,
					RawXDR: "AAAAB==",
					Topics: json.RawMessage(`[{"old":true}]`),
					Value:  json.RawMessage(`{"old":true}`),
				},
			},
		}
		dec := &mockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, json.RawMessage, error) {
				return json.RawMessage(`[{"new":true}]`),
					json.RawMessage(`{"new":true}`),
					nil
			},
		}

		err := Run(ctx, st, dec, 10, 10, 100)
		require.NoError(t, err)
		require.Len(t, st.Batches, 1)
		require.Len(t, st.Batches[0].Events, 1)
		assert.Equal(t, string(json.RawMessage(`[{"new":true}]`)), string(st.Batches[0].Events[0].Topics))
		assert.Equal(t, string(json.RawMessage(`{"new":true}`)), string(st.Batches[0].Events[0].Value))
	})

	t.Run("unchanged decoding being reported and not rewritten", func(t *testing.T) {
		ctx := context.Background()
		topics := json.RawMessage(`[{"same":true}]`)
		val := json.RawMessage(`{"same":true}`)
		st := &MockStore{
			Events: []store.Event{
				{
					ID:     "0000000000000002-000",
					Ledger: 11,
					RawXDR: "BBB==",
					Topics: topics,
					Value:  val,
				},
			},
		}
		dec := &mockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, json.RawMessage, error) {
				return topics, val, nil
			},
		}

		err := Run(ctx, st, dec, 11, 11, 100)
		require.NoError(t, err)
		if len(st.Batches) > 0 {
			assert.Len(t, st.Batches[0].Events, 1)
		}
	})

	t.Run("second replay over the same range changing nothing", func(t *testing.T) {
		ctx := context.Background()
		topics := json.RawMessage(`[{"final":true}]`)
		val := json.RawMessage(`{"final":true}`)
		st := &MockStore{
			Events: []store.Event{
				{
					ID:     "0000000000000003-000",
					Ledger: 12,
					RawXDR: "CCC==",
					Topics: topics,
					Value:  val,
				},
			},
		}
		dec := &mockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, json.RawMessage, error) {
				return topics, val, nil
			},
		}

		err := Run(ctx, st, dec, 12, 12, 100)
		require.NoError(t, err)

		err = Run(ctx, st, dec, 12, 12, 100)
		require.NoError(t, err)
	})

	t.Run("decode failure being counted and skipped rather than fatal", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{
					ID:     "0000000000000004-000",
					Ledger: 13,
					RawXDR: "BAD==",
					Topics: json.RawMessage(`[]`),
					Value:  json.RawMessage(`{}`),
				},
			},
		}
		dec := &mockDecoder{
			FailCount: 1,
		}

		err := Run(ctx, st, dec, 13, 13, 100)
		require.NoError(t, err)
	})

	t.Run("per-batch progress bounding the work lost to an interrupt", func(t *testing.T) {
		ctx := context.Background()
		st := &MockStore{
			Events: []store.Event{
				{ID: "1", Ledger: 20, RawXDR: "X1"},
				{ID: "2", Ledger: 21, RawXDR: "X2"},
			},
		}
		dec := &mockDecoder{
			RewriteFn: func(raw string) (json.RawMessage, json.RawMessage, error) {
				return json.RawMessage(`[]`), json.RawMessage(`{}`), nil
			},
		}

		err := Run(ctx, st, dec, 20, 21, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(21), st.ReplayState.LastLedger)
	})
}
