package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteOperation_SingleOperation(t *testing.T) {
	// Register a test route
	RegisterRoute("Query", "testField", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return "hello world", nil
	})

	UseResolver(&Resolver{})

	req := &GraphQLRequest{
		Query: "query { testField }",
	}

	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, env)
	assert.Empty(t, env.Errors)
	assert.Equal(t, "hello world", env.Data["testField"])
}

func TestExecuteOperation_NamedOperationSelection(t *testing.T) {
	RegisterRoute("Query", "opOne", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return "first", nil
	})
	RegisterRoute("Query", "opTwo", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return "second", nil
	})

	UseResolver(&Resolver{})

	query := `
		query FirstOp { opOne }
		query SecondOp { opTwo }
	`

	// A document with multiple operations is rejected outright, even when
	// an operationName is supplied.
	req := &GraphQLRequest{
		Query:         query,
		OperationName: "SecondOp",
	}

	_, err := executeOperation(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple operations per request not supported")
}

func TestExecuteOperation_AmbiguousSelectionWithoutName(t *testing.T) {
	RegisterRoute(
		"Query",
		"opOne",
		func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
			return "first", nil
		},
	)
	RegisterRoute(
		"Query",
		"opTwo",
		func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
			return "second", nil
		},
	)

	UseResolver(&Resolver{})

	query := `
		query FirstOp { opOne }
		query SecondOp { opTwo }
	`

	req := &GraphQLRequest{
		Query: query,
	}

	// executeOperation accepts at most one operation per document and
	// rejects a multi-operation request outright.
	_, err := executeOperation(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple operations per request not supported")
}

func TestExecuteOperation_FieldArgumentsReachResolver(t *testing.T) {
	var capturedArgs json.RawMessage
	RegisterRoute("Query", "echo", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		capturedArgs = args
		return "ok", nil
	})

	UseResolver(&Resolver{})

	req := &GraphQLRequest{
		Query: `{ echo(msg: "hello arguments") }`,
	}

	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, env)
	assert.Empty(t, env.Errors)
	assert.Contains(t, string(capturedArgs), "hello arguments")
}

func TestExecuteOperation_ResolverErrorSurfacesAsGraphQLError(t *testing.T) {
	RegisterRoute("Query", "fails", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return nil, errors.New("database connection failed")
	})

	UseResolver(&Resolver{})

	req := &GraphQLRequest{
		Query: `{ fails }`,
	}

	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err, "resolver errors should be captured in envelope errors, not returned as execution error")
	require.NotNil(t, env)
	require.Len(t, env.Errors, 1)
	assert.Equal(t, "database connection failed", env.Errors[0].Message)
	assert.Nil(t, env.Data["fails"])
}

func TestExecuteOperation_PartialDataWithErrors(t *testing.T) {
	RegisterRoute("Query", "successField", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return "all good", nil
	})
	RegisterRoute(
		"Query",
		"errorField",
		func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
			return nil, errors.New("partial failure")
		},
	)

	UseResolver(&Resolver{})

	req := &GraphQLRequest{
		Query: `{ successField errorField }`,
	}

	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, env)
	require.Len(t, env.Errors, 1)
	assert.Equal(t, "partial failure", env.Errors[0].Message)
	assert.Equal(t, "all good", env.Data["successField"])
	assert.Nil(t, env.Data["errorField"])
}
