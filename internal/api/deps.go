package api

import "github.com/sorotrail/sorotrail/internal/store"

// ServerDeps bundles the dependencies a transport handler (e.g. the
// GraphQL package) needs to read from the API server without taking a
// dependency on internal/api's larger Server type.
//
// Defined as a struct so future transports (gRPC, batch listeners)
// can build a Handler with a single argument.
type ServerDeps struct {
	// Store is the event store. Required.
	Store store.Store
	// Enricher is the spec-decoding enricher. Optional — pass nil when
	// spec decoding is disabled; the GraphQL tokenEvents resolver
	// returns decoded=false for every row in that case, matching the
	// REST `/events?decoded=false` shape.
	Enricher Enricher
}
