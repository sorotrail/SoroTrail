package replay

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"

	"github.com/sorotrail/sorotrail/internal/store"
)

// fakeStore is an in-memory stand-in for the Postgres replay surface. It
// models the two properties the engine's correctness rests on: batch
// rewrites and progress commit atomically, and only one replay may hold the
// lock at a time.
type fakeStore struct {
	mu     sync.Mutex
	events map[string]store.DecodedEvent
	state  *store.ReplayState
	locked bool

	// failCommitAfter, when > 0, makes CommitReplayBatch fail once that many
	// commits have succeeded — standing in for a crash mid-replay.
	failCommitAfter int
	commits         int
	// commitErr is what such a failure returns.
	commitErr error
	// onCommit, when set, runs after each successful commit — used to
	// interrupt a run at a known point.
	onCommit func()
}

func newFakeStore(events ...store.DecodedEvent) *fakeStore {
	s := &fakeStore{events: make(map[string]store.DecodedEvent, len(events))}
	for _, e := range events {
		s.events[e.ID] = e
	}
	return s
}

type fakeLock struct{ s *fakeStore }

func (l *fakeLock) Release() {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.s.locked = false
}

func (s *fakeStore) AcquireReplayLock(context.Context) (store.ReplayLock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locked {
		return nil, store.ErrReplayLocked
	}
	s.locked = true
	return &fakeLock{s: s}, nil
}

func (s *fakeStore) GetReplayState(context.Context) (store.ReplayState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return store.ReplayState{}, store.ErrNotFound
	}
	return *s.state, nil
}

func (s *fakeStore) StartReplayState(_ context.Context, from, to int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = &store.ReplayState{FromLedger: from, ToLedger: to}
	return nil
}

func (s *fakeStore) NextReplayBatch(_ context.Context, from, to int64, afterID string, limit int) ([]store.DecodedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.events))
	for id, e := range s.events {
		if e.Ledger >= from && e.Ledger <= to && id > afterID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	batch := make([]store.DecodedEvent, 0, len(ids))
	for _, id := range ids {
		batch = append(batch, s.events[id])
	}
	return batch, nil
}

func (s *fakeStore) CommitReplayBatch(_ context.Context, b store.ReplayBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failCommitAfter > 0 && s.commits >= s.failCommitAfter {
		// Fail before applying anything: a real transaction rolls back the
		// rewrites and the progress marker together.
		return s.commitErr
	}
	for _, w := range b.Events {
		e := s.events[w.ID]
		e.Topics, e.Value = w.Topics, w.Value
		s.events[e.ID] = e
	}
	st := b.State
	s.state = &st
	s.commits++
	if s.onCommit != nil {
		s.onCommit()
	}
	return nil
}

// snapshot returns the decoded columns keyed by event ID, for comparing the
// end state of two runs.
func (s *fakeStore) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.events))
	for id, e := range s.events {
		out[id] = string(e.Topics) + "|" + string(e.Value)
	}
	return out
}

var errCrash = errors.New("simulated crash mid-replay")

// staticDecoder returns a fixed decoding per base64 input, so tests can model
// "the decoder improved" by swapping the map.
type staticDecoder struct {
	out map[string]string
	err error
}

func (d staticDecoder) DecodeScVal(base64XDR string) (json.RawMessage, error) {
	if d.err != nil {
		return nil, d.err
	}
	v, ok := d.out[base64XDR]
	if !ok {
		return nil, errors.New("staticDecoder: no output configured for " + base64XDR)
	}
	return json.RawMessage(v), nil
}
