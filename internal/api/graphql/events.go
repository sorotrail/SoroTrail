package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Resolver holds the dependencies the GraphQL operations need. It is
// constructed once at startup and shared between goroutines — every
// handler-internal method is safe for concurrent use as long as the
// underlying store satisfies the Store concurrency contract.
type Resolver struct {
	store    store.Store
	enricher Enricher // see server.go for interface{} compatibility alias
}

// Enricher is the same shape internal/api defines. Aliased here so
// tests can build a Resolver without pulling the broader api package.
type Enricher = interface {
	EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent
}

// EventResult is the JSON-serializable representation of store.Event.
//
// Field names match the GraphQL schema verbatim — using the same JSON
// keys as the REST /events endpoint keeps cross-transport scripts
// (curl + jq on REST; Relay on GraphQL) working off identical payload
// naming. The GraphQL transport still has its own envelope (the
// Connection), so this struct is wrapped before being returned to a
// client.
type EventResult struct {
	ID               string          `json:"id"`
	ContractID       string          `json:"contractId"`
	Ledger           int64           `json:"ledger"`
	Type             string          `json:"type"`
	TxHash           string          `json:"txHash"`
	TxIndex          int32           `json:"txIndex"`
	OpIndex          int32           `json:"opIndex"`
	InSuccessfulCall bool            `json:"inSuccessfulCall"`
	Topics           json.RawMessage `json:"topics"`
	Value            json.RawMessage `json:"value"`
	CreatedAt        time.Time       `json:"createdAt"`
}

func eventToResult(e store.Event) EventResult {
	return EventResult{
		ID:               e.ID,
		ContractID:       e.ContractID,
		Ledger:           e.Ledger,
		Type:             e.Type,
		TxHash:           e.TxHash,
		TxIndex:          e.TxIndex,
		OpIndex:          e.OpIndex,
		InSuccessfulCall: e.InSuccessfulCall,
		Topics:           e.Topics,
		Value:            e.Value,
		CreatedAt:        e.CreatedAt,
	}
}

// TokenEventResult adds spec-driven decoded field metadata to
// EventResult. decoded=true means resolved.successfully to a spec
// entry; DecodedEvent is nil when decoding produced no useful mapping.
type TokenEventResult struct {
	EventResult
	Decoded      bool            `json:"decoded"`
	DecodedEvent *DecodedResult  `json:"decodedEvent,omitempty"`
}

// DecodedResult mirrors store.DecodedEventResponse so the GraphQL
// payload matches the REST /events?decoded=true shape exactly.
type DecodedResult struct {
	Event  string         `json:"name"`
	Fields map[string]any `json:"fields,omitempty"`
}

// ContractResult mirrors store.WatchedContract for the contracts query.
type ContractResult struct {
	ContractID string    `json:"contractId"`
	AddedAt    time.Time `json:"addedAt"`
}

// PageInfo is the Relay-style connection terminator.
type PageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor,omitempty"`
}

// EventConnection is the events / tokenEvents responses.
type EventConnection struct {
	Edges      []EventEdge `json:"edges"`
	Nodes      []EventResult `json:"nodes"`
	PageInfo   PageInfo   `json:"pageInfo"`
	TotalCount int32      `json:"totalCount"`
}

// TokenEventConnection is the alias-typed connection for tokenEvents.
type TokenEventConnection struct {
	Edges      []TokenEventEdge `json:"edges"`
	Nodes      []TokenEventResult `json:"nodes"`
	PageInfo   PageInfo   `json:"pageInfo"`
	TotalCount int32      `json:"totalCount"`
}

// ContractConnection is the watched-contracts response.
type ContractConnection struct {
	Edges      []ContractEdge `json:"edges"`
	Nodes      []ContractResult `json:"nodes"`
	PageInfo   PageInfo   `json:"pageInfo"`
	TotalCount int32      `json:"totalCount"`
}

// EventEdge / TokenEventEdge / ContractEdge are the Relay edge wrappers.
type EventEdge struct {
	Cursor string       `json:"cursor"`
	Node   EventResult  `json:"node"`
}
type TokenEventEdge struct {
	Cursor string           `json:"cursor"`
	Node   TokenEventResult `json:"node"`
}
type ContractEdge struct {
	Cursor string         `json:"cursor"`
	Node   ContractResult `json:"node"`
}

// EventFilterArgs is the GraphQL-side representation of EventFilterInput
// + PageInput. We deliberately keep the struct small and call into
// queries.BuildEventFilter / queries.ResolvePage so the validation
// rules stay in one place.
type EventFilterArgs struct {
	Filter *FilterInput
	Page   *PageInput
}

// FilterInput maps the GraphQL EventFilterInput input type.
type FilterInput struct {
	ContractID    string                  `json:"contractId"`
	Types         []string                `json:"types"`
	Topic         json.RawMessage         `json:"topic"`
	Topics        *TopicPositionInput     `json:"topics"`
	TopicContains json.RawMessage         `json:"topicContains"`
	TxHash        string                  `json:"txHash"`
	FromLedger    *int64                  `json:"fromLedger"`
	ToLedger      *int64                  `json:"toLedger"`
	FromTime      *time.Time              `json:"fromTime"`
	ToTime        *time.Time              `json:"toTime"`
}

// TopicPositionInput maps the GraphQL TopicPositionFilterInput input type.
type TopicPositionInput struct {
	T0 json.RawMessage `json:"t0"`
	T1 json.RawMessage `json:"t1"`
	T2 json.RawMessage `json:"t2"`
	T3 json.RawMessage `json:"t3"`
}

// PageInput maps the GraphQL PageInput input type.
type PageInput struct {
	First   *int32  `json:"first"`
	After   string  `json:"after"`
	Last    *int32  `json:"last"`
	Before  string  `json:"before"`
	Order   string  `json:"order"`
	OrderBy string  `json:"orderBy"`
}

// buildEventFilter is the GraphQL-to-EventFilter bridge. It pulls
// values from the EventFilterArgs struct, runs the shared
// queries.BuildEventFilter, and returns a (filter, page-cursor-or-empty,
// order, orderBy, error) tuple the resolver uses.
//
// Errors are returned as is so the executor can wrap them in a GraphQL
// error envelope. Validation that lives in queries.BuildEventFilter is
// identical to the REST filterFromQuery validation — there is no
// divergent code path.
func buildEventFilter(args EventFilterArgs) (store.EventFilter, string, string, string, error) {
	if args.Filter == nil {
		args.Filter = &FilterInput{}
	}
	if args.Page == nil {
		args.Page = &PageInput{}
	}

	// Pagination — decode the cursor (if any) BEFORE validating, since
	// the store cursor itself is the page-cursor argument to the SQL
	// query.
	cursor := ""
	if args.Page.After != "" {
		pp, err := DecodeCursor(args.Page.After)
		if err != nil {
			return store.EventFilter{}, "", "", "", err
		}
		cursor = pp.LastID
	}

	pageArgs := queries.PageArgs{
		First:   derefInt(args.Page.First),
		Last:    derefInt(args.Page.Last),
		Before:  args.Page.Before,
		After:   cursor,
		Order:   args.Page.Order,
		OrderBy: args.Page.OrderBy,
	}
	limit, pageCursor, order, orderBy, err := queries.ResolvePage(pageArgs)
	if err != nil {
		return store.EventFilter{}, "", "", "", err
	}

	var topics *TopicPositionInput
	if args.Filter != nil {
		topics = args.Filter.Topics
	}

	queryArgs := queries.EventFilterArgs{
		ContractID:    args.Filter.ContractID,
		Types:         args.Filter.Types,
		Topic:         args.Filter.Topic,
		TxHash:        args.Filter.TxHash,
		FromLedger:    derefInt64(args.Filter.FromLedger),
		ToLedger:      derefInt64(args.Filter.ToLedger),
		FromTime:      derefTime(args.Filter.FromTime),
		ToTime:        derefTime(args.Filter.ToTime),
		Order:         order,
		OrderBy:       orderBy,
		Cursor:        pageCursor,
		Limit:         limit,
	}
	if topics != nil {
		queryArgs.T0 = topics.T0
		queryArgs.T1 = topics.T1
		queryArgs.T2 = topics.T2
		queryArgs.T3 = topics.T3
	}
	if len(args.Filter.TopicContains) > 0 {
		queryArgs.TopicContains = args.Filter.TopicContains
	}

	filter, err := queries.BuildEventFilter(queryArgs)
	if err != nil {
		return filter, "", "", "", err
	}
	return filter, cursor, order, orderBy, nil
}

// resolveEvents is the events(root) resolver.
func (r *Resolver) resolveEvents(ctx context.Context, args EventFilterArgs) (any, error) {
	if r == nil || r.store == nil {
		return nil, errors.New("graphQL server misconfigured: store not wired")
	}
	filter, _, order, orderBy, err := buildEventFilter(args)
	if err != nil {
		return nil, err
	}
	events, nextCursor, err := r.store.QueryEvents(ctx, filter)
	if err != nil {
		return nil, err
	}

	// totalCount is over the same filter, ignoring pagination. Count
	// errors degrade to -1 so the page still succeeds (cf. how REST
	// /events handles it: omits X-Total-Count and continues).
	total, totalErr := r.store.CountEvents(ctx, stripPagination(filter))

	out := EventConnection{
		Edges:      make([]EventEdge, 0, len(events)),
		Nodes:      make([]EventResult, 0, len(events)),
		PageInfo:   PageInfo{HasNextPage: nextCursor != "", EndCursor: EncodeCursor(nextCursor, orderBy, order)},
	}
	for _, e := range events {
		out.Edges = append(out.Edges, EventEdge{
			Cursor: EncodeCursor(e.ID, orderBy, order),
			Node:   eventToResult(e),
		})
		out.Nodes = append(out.Nodes, eventToResult(e))
	}
	if totalErr == nil {
		out.TotalCount = int32(total)
	}

	return out, nil
}

// resolveEvent is the event(id) single-row resolver.
func (r *Resolver) resolveEvent(ctx context.Context, id string) (any, error) {
	if r == nil || r.store == nil {
		return nil, errors.New("graphQL server misconfigured: store not wired")
	}
	if id == "" {
		return nil, errors.New("event id is required")
	}
	e, err := r.store.GetEvent(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return eventToResult(e), nil
}

// resolveTokenEvents is the tokenEvents(root) resolver. tokenEvents
// uses the same backing store + filter as events; the "decoded" output
// is achieved by passing the page through spec.Enricher (the same path
// REST /events?decoded=true uses).
func (r *Resolver) resolveTokenEvents(ctx context.Context, args EventFilterArgs) (any, error) {
	if r == nil || r.store == nil {
		return nil, errors.New("graphQL server misconfigured: store not wired")
	}
	filter, _, order, orderBy, err := buildEventFilter(args)
	if err != nil {
		return nil, err
	}
	events, nextCursor, err := r.store.QueryEvents(ctx, filter)
	if err != nil {
		return nil, err
	}
	total, totalErr := r.store.CountEvents(ctx, stripPagination(filter))

	// enricher may be nil (e.g. on builds where spec decoding is
	// disabled). Construct an equal-length slice of un-decoded
	// EnrichedEvents so the GraphQL response shape stays consistent
	// with the REST `?decoded=true` shape: `decoded: false` for every
	// row, no DecodedEvent.
	enriched := make([]store.EnrichedEvent, len(events))
	if r.enricher != nil {
		specDecoded := r.enricher.EnrichEvents(ctx, events)
		// spec.Enricher may return fewer rows than we requested when
		// a partial enrichment overflows the buffer. Drop the spec view
		// for any rows that didn't make it through so the rest of the
		// response still has a literal Event on the wire.
		if len(specDecoded) == len(events) {
			enriched = specDecoded
		} else {
			for i := range events {
				enriched[i] = store.EnrichedEvent{Event: events[i], Decoded: false}
			}
		}
	} else {
		for i := range events {
			enriched[i] = store.EnrichedEvent{Event: events[i], Decoded: false}
		}
	}

	out := TokenEventConnection{
		Edges:    make([]TokenEventEdge, 0, len(enriched)),
		Nodes:    make([]TokenEventResult, 0, len(enriched)),
		PageInfo: PageInfo{HasNextPage: nextCursor != "", EndCursor: EncodeCursor(nextCursor, orderBy, order)},
	}
	for _, ee := range enriched {
		node := TokenEventResult{
			EventResult: eventToResult(ee.Event),
			Decoded:     ee.Decoded,
		}
		if ee.DecodedEvent != nil {
			node.DecodedEvent = &DecodedResult{
				Event:  ee.DecodedEvent.Event,
				Fields: ee.DecodedEvent.Fields,
			}
		}
		out.Edges = append(out.Edges, TokenEventEdge{
			Cursor: EncodeCursor(ee.ID, orderBy, order),
			Node:   node,
		})
		out.Nodes = append(out.Nodes, node)
	}
	if totalErr == nil {
		out.TotalCount = int32(total)
	}
	return out, nil
}

// stripPagination clones filter with pagination fields zeroed, matching
// what the REST /events/count handler does before calling CountEvents.
func stripPagination(f store.EventFilter) store.EventFilter {
	f2 := f
	f2.Cursor = ""
	f2.Order = ""
	f2.OrderBy = ""
	f2.Limit = 0
	return f2
}

func derefInt(p *int32) int {
	if p == nil {
		return 0
	}
	return int(*p)
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}
