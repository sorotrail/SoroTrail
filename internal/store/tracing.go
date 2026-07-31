package store

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracingStore decorates a Store with OpenTelemetry spans for persistence and
// query operations. It preserves the underlying store behavior while giving
// operators a trace view of the database boundary.
type TracingStore struct {
	Store
	tracer trace.Tracer
}

// NewTracingStore returns a Store wrapper that records spans for the common
// persistence and lookup operations used by the API and ingester.
func NewTracingStore(base Store, tracer trace.Tracer) Store {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("sorotrail/store")
	}
	return &TracingStore{Store: base, tracer: tracer}
}

func (s *TracingStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "store.UpsertEvents")
	defer span.End()
	span.SetAttributes(attribute.Int("store.event_count", len(events)))
	inserted, err := s.Store.UpsertEvents(ctx, events)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return inserted, err
}

func (s *TracingStore) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	ctx, span := s.tracer.Start(ctx, "store.ReplaceEventsInRange")
	defer span.End()
	span.SetAttributes(attribute.Int64("store.from_ledger", fromLedger), attribute.Int64("store.to_ledger", toLedger), attribute.Int("store.event_count", len(events)))
	err := s.Store.ReplaceEventsInRange(ctx, events, fromLedger, toLedger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (s *TracingStore) GetEvent(ctx context.Context, id string) (Event, error) {
	ctx, span := s.tracer.Start(ctx, "store.GetEvent")
	defer span.End()
	span.SetAttributes(attribute.String("store.event_id", id))
	event, err := s.Store.GetEvent(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return event, err
}

func (s *TracingStore) EventExists(ctx context.Context, id string) (bool, error) {
	ctx, span := s.tracer.Start(ctx, "store.EventExists")
	defer span.End()
	span.SetAttributes(attribute.String("store.event_id", id))
	exists, err := s.Store.EventExists(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return exists, err
}

func (s *TracingStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	ctx, span := s.tracer.Start(ctx, "store.QueryEvents")
	defer span.End()
	span.SetAttributes(attribute.String("store.contract_id", f.ContractID), attribute.String("store.type", f.Type))
	events, cursor, err := s.Store.QueryEvents(ctx, f)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.Int("store.event_count", len(events)))
	}
	return events, cursor, err
}

func (s *TracingStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	ctx, span := s.tracer.Start(ctx, "store.GetIngestionState")
	defer span.End()
	state, err := s.Store.GetIngestionState(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return state, err
}

func (s *TracingStore) SaveIngestionState(ctx context.Context, state IngestionState) error {
	ctx, span := s.tracer.Start(ctx, "store.SaveIngestionState")
	defer span.End()
	span.SetAttributes(attribute.Int64("store.last_ingested_ledger", state.LastIngestedLedger))
	err := s.Store.SaveIngestionState(ctx, state)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
