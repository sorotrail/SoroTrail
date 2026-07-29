package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// resolverDefault is the active Resolver instance populated at
// handler construction time. Used as a backstop by the dispatch table
// when a route function wants to be sure Handler.New ran.
var resolverDefault struct {
	sync.RWMutex
	r *Resolver
}

// validateAndCount runs the depth (10) + complexity (1000) guards
// against the parsed operation. Both checks run before any field
// resolver so a malicious query is rejected cheaply.
func validateAndCount(op *ast.OperationDefinition) error {
	if err := CheckDepth(op); err != nil {
		return err
	}
	if err := CheckComplexity(op); err != nil {
		return err
	}
	return nil
}

// errorsSlice is the wire shape for GraphQL errors. We always emit
// {"message": ..., "path": [...]} for any failure surfaced through
// the JSON envelope.
type errorsSlice []graphQLError

type graphQLError struct {
	Message string   `json:"message"`
	Path    []string `json:"path,omitempty"`
}

// responseEnvelope is the GraphQL POST response shape. The
// specification does not strictly require `data` non-null on errors,
// but emitting it as `{}` keeps client UX predictable. (Clients
// expecting `data: null` on errors will still see `errors[]`.)
type responseEnvelope struct {
	Data   map[string]any `json:"data"`
	Errors errorsSlice    `json:"errors,omitempty"`
}

// dispatchRoutes forwards a parsed field at the root of the operation
// to one of the resolver methods. Schema-level resolvers are matched
// by field name; each branch returns a (possibly-nil) value or error.
var dispatcher = struct {
	sync.RWMutex
	routes map[string]func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error)
}{
	routes: map[string]func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error){},
}

// RegisterRoute adds a top-level field resolver, keyed by SchemaType
// → field name (e.g. "Query", "events"). Used by server.go's
// registerRoutes() at init time.
//
// The Resolver is the first arg so closures don't need to call into a
// global resolver lookup on every request. New top-level Query fields
// only need to register via RegisterRoute("Query", "name", fn).
func RegisterRoute(parent, name string, fn func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error)) {
	dispatcher.Lock()
	defer dispatcher.Unlock()
	dispatcher.routes[parent+":"+name] = fn
}

// UseResolver sets the active Resolver for the dispatcher table. Used
// as a guard at the entry point of executeOperation and by tests
// that want to swap the resolver at runtime.
func UseResolver(r *Resolver) {
	resolverDefault.Lock()
	resolverDefault.r = r
	resolverDefault.Unlock()
}

// currentResolver returns the active Resolver for the dispatch table.
// Reads are protected by a RWMutex so concurrent ServeHTTP calls are
// safe even when init() rewires the routing.
func currentResolver() *Resolver {
	resolverDefault.RLock()
	r := resolverDefault.r
	resolverDefault.RUnlock()
	return r
}

// executeOperation is the entry point used by the HTTP layer. It
// parses the request, applies the depth/complexity guards, walks the
// operation's selection set, and produces a responseEnvelope.
func executeOperation(ctx context.Context, req *GraphQLRequest) (*responseEnvelope, error) {
	if req == nil || req.Query == "" {
		return nil, fmt.Errorf("query body is required")
	}
	doc, err := parser.ParseQuery(&ast.Source{Name: "request", Input: req.Query})
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(doc.Operations) == 0 {
		return nil, fmt.Errorf("no operation in document")
	}
	if len(doc.Operations) > 1 {
		return nil, fmt.Errorf("multiple operations per request not supported")
	}
	op := doc.Operations[0]
	if op.Operation != ast.Query {
		return nil, fmt.Errorf("only Query operations are supported (got %s)", op.Operation)
	}
	if err := validateAndCount(op); err != nil {
		return nil, err
	}

	vars := map[string]any{}
	for k, v := range req.Variables {
		vars[k] = v
	}

	r := currentResolver()
	if r == nil {
		return nil, fmt.Errorf("graphql handler not initialized")
	}

	data := map[string]any{}
	for _, sel := range op.SelectionSet {
		f, ok := sel.(*ast.Field)
		if !ok {
			continue
		}
		val, err := r.resolveQueryField(ctx, f, vars)
		if err != nil {
			data[f.Alias] = nil
			return &responseEnvelope{
				Data:   data,
				Errors: errorsSlice{{Message: err.Error(), Path: []string{f.Alias}}},
			}, nil
		}
		data[f.Alias] = val
	}
	return &responseEnvelope{Data: data}, nil
}

// resolveQueryField dispatches one root-level field. The dispatcher
// table is keyed by `Query:<field>`.
func (r *Resolver) resolveQueryField(ctx context.Context, f *ast.Field, vars map[string]any) (any, error) {
	dispatcher.RLock()
	fn, ok := dispatcher.routes["Query:"+f.Name]
	dispatcher.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown Query field %q", f.Name)
	}
	args, err := fieldArguments(f, vars)
	if err != nil {
		return nil, err
	}
	return fn(r, ctx, args, vars)
}

// fieldArguments converts the argument list of a GraphQL Field into a
// JSON object so each resolver can json.Unmarshal it into its typed
// args struct. Returns `{}` when the field has no arguments so
// resolvers can rely on non-nil JSON.
func fieldArguments(f *ast.Field, vars map[string]any) (json.RawMessage, error) {
	if len(f.Arguments) == 0 {
		return json.RawMessage("{}"), nil
	}
	args := map[string]json.RawMessage{}
	for _, a := range f.Arguments {
		v, err := argumentValue(a.Value, vars)
		if err != nil {
			return nil, fmt.Errorf("argument %q: %w", a.Name, err)
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		args[a.Name] = b
	}
	return json.Marshal(args)
}

// argumentValue converts a single *ast.Value into a Go value. In v2 a
// value is a struct (Kind + Raw + Children) rather than an interface;
// scalars live in Raw, lists/objects unpack through Children.
func argumentValue(v *ast.Value, vars map[string]any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch v.Kind {
	case ast.Variable:
		if val, ok := vars[v.Raw]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("missing variable %q", v.Raw)
	case ast.IntValue:
		if n, err := strconv.ParseInt(v.Raw, 10, 64); err == nil {
			return n, nil
		}
		return nil, fmt.Errorf("invalid int %q", v.Raw)
	case ast.FloatValue:
		if f, err := strconv.ParseFloat(v.Raw, 64); err == nil {
			return f, nil
		}
		return nil, fmt.Errorf("invalid float %q", v.Raw)
	case ast.StringValue:
		// GraphQL strings come in as quoted; unquote via Unmarshal.
		var s string
		if err := json.Unmarshal([]byte(v.Raw), &s); err == nil {
			return s, nil
		}
		return v.Raw, nil
	case ast.EnumValue:
		return v.Raw, nil
	case ast.BooleanValue:
		return v.Raw == "true", nil
	case ast.NullValue:
		return nil, nil
	case ast.ListValue:
		out := make([]any, len(v.Children))
		for i, child := range v.Children {
			vv, err := argumentValue(child.Value, vars)
			if err != nil {
				return nil, err
			}
			out[i] = vv
		}
		return out, nil
	case ast.ObjectValue:
		out := map[string]any{}
		for _, child := range v.Children {
			vv, err := argumentValue(child.Value, vars)
			if err != nil {
				return nil, err
			}
			out[child.Name] = vv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported arg kind %v", v.Kind)
	}
}

// GraphQLRequest is the wire shape for a POST /graphql body.
type GraphQLRequest struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName,omitempty"`
	Variables     map[string]any `json:"variables,omitempty"`
}
