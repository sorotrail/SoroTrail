package graphql

import (
	"context"
	"errors"
	"fmt"

	"github.com/sorotrail/sorotrail/internal/store"
)

// ContractPageInput is the GraphQL-only PageInput slice (no filter).
type ContractPageInput struct {
	First  *int32
	After  string
	Last   *int32
	Before string
	Order  string
}

// resolveContracts returns the watched-contract list (REST-equivalent
// of GET /watched-contracts, no filter argument because it makes no
// sense to filter the runtime watch list — that's what events is for).
func (r *Resolver) resolveContracts(ctx context.Context, args PageInput) (any, error) {
	if r == nil || r.store == nil {
		return nil, errors.New("graphQL server misconfigured: store not wired")
	}

	rows, err := r.store.ListWatchedContracts(ctx)
	if err != nil {
		return nil, err
	}

	// Limit (first/after). Contracts list is small; the same pagination
	// rules as events apply but PageInput doesn't carry orderBy here.
	limit := store.DefaultQueryLimit
	if args.First != nil && *args.First > 0 {
		if int(*args.First) > store.MaxQueryLimit {
			return nil, errLimitTooLarge{have: int(*args.First), max: store.MaxQueryLimit}
		}
		limit = int(*args.First)
	}
	if args.Last != nil || args.Before != "" {
		return nil, errors.New("backward pagination (last/before) is not supported")
	}

	total := int32(len(rows))
	hasNext := false
	endIdx := len(rows)
	if args.After != "" {
		// Resume is by contract_id: the cursor here is base64({id, ...}).
		pp, err := DecodeCursor(args.After)
		if err != nil {
			return nil, err
		}
		idx := indexAfter(rows, pp.LastID)
		if idx < 0 {
			// Cursor references a contract not in the watch list —
			// respond with an empty page rather than failing the request.
			out := ContractConnection{Edges: []ContractEdge{}, Nodes: []ContractResult{}, PageInfo: PageInfo{}}
			out.TotalCount = total
			return out, nil
		}
		rows = rows[idx+1:]
		endIdx = len(rows)
	}
	if endIdx > limit {
		rows = rows[:limit]
		hasNext = true
	}
	out := ContractConnection{
		Edges:      make([]ContractEdge, 0, len(rows)),
		Nodes:      make([]ContractResult, 0, len(rows)),
		PageInfo:   PageInfo{},
		TotalCount: total,
	}
	for _, r := range rows {
		out.Edges = append(out.Edges, ContractEdge{
			Cursor: EncodeCursor(r.ContractID, "", ""),
			Node:   ContractResult{ContractID: r.ContractID, AddedAt: r.AddedAt},
		})
		out.Nodes = append(out.Nodes, ContractResult{ContractID: r.ContractID, AddedAt: r.AddedAt})
	}
	if hasNext && len(out.Edges) > 0 {
		last := out.Edges[len(out.Edges)-1]
		pp, err := DecodeCursor(last.Cursor)
		if err == nil {
			out.PageInfo.HasNextPage = true
			out.PageInfo.EndCursor = EncodeCursor(pp.LastID, "", "")
		}
	}
	return out, nil
}

// resolveContract returns the single watched contract by ID, or nil when
// the contract is not in the watch list.
func (r *Resolver) resolveContract(ctx context.Context, id string) (any, error) {
	if r == nil || r.store == nil {
		return nil, errors.New("graphQL server misconfigured: store not wired")
	}
	if id == "" {
		return nil, errors.New("contract id is required")
	}
	rows, err := r.store.ListWatchedContracts(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.ContractID == id {
			return ContractResult{ContractID: r.ContractID, AddedAt: r.AddedAt}, nil
		}
	}
	return nil, nil
}

func indexAfter(rows []store.WatchedContract, id string) int {
	for i, r := range rows {
		if r.ContractID == id {
			return i
		}
	}
	return -1
}

// errLimitTooLarge is a typed error so the executor can surface its
// text in a GraphQL error path with a stable code.
type errLimitTooLarge struct{ have, max int }

func (e errLimitTooLarge) Error() string {
	return fmt.Sprintf("first must be in [1,%d] (got %d)", e.max, e.have)
}
