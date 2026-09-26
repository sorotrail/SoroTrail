package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestArgumentValue(t *testing.T) {
	scalar := func(kind ast.ValueKind, raw string) *ast.Value {
		return &ast.Value{Kind: kind, Raw: raw}
	}
	vars := map[string]any{
		"contract": "CABC",
		"limit":    float64(25),
		"cleared":  nil,
	}

	tests := []struct {
		name    string
		value   *ast.Value
		want    any
		wantErr string
	}{
		{name: "int", value: scalar(ast.IntValue, "42"), want: int64(42)},
		{name: "negative int", value: scalar(ast.IntValue, "-7"), want: int64(-7)},
		{name: "int beyond int64", value: scalar(ast.IntValue, "9223372036854775808"), wantErr: `invalid int "9223372036854775808"`},
		{name: "float", value: scalar(ast.FloatValue, "1.5"), want: 1.5},
		{name: "float with exponent", value: scalar(ast.FloatValue, "2e3"), want: 2000.0},
		{name: "quoted string is unquoted", value: scalar(ast.StringValue, `"CABC"`), want: "CABC"},
		{name: "escaped string is decoded", value: scalar(ast.StringValue, `"a\"b"`), want: `a"b`},
		{name: "unquoted string passes through", value: scalar(ast.StringValue, "CABC"), want: "CABC"},
		{name: "enum", value: scalar(ast.EnumValue, "DESC"), want: "DESC"},
		{name: "boolean true", value: scalar(ast.BooleanValue, "true"), want: true},
		{name: "boolean false", value: scalar(ast.BooleanValue, "false"), want: false},
		{name: "non-numeric int", value: scalar(ast.IntValue, "ten"), wantErr: `invalid int "ten"`},
		{name: "float literal as int", value: scalar(ast.IntValue, "1.5"), wantErr: `invalid int "1.5"`},
		{name: "non-numeric float", value: scalar(ast.FloatValue, "abc"), wantErr: `invalid float "abc"`},
		{name: "unsupported kind", value: &ast.Value{Kind: ast.ValueKind(99)}, wantErr: "unsupported arg kind"},
		{name: "nil value", value: nil, want: nil},
		{name: "null literal", value: scalar(ast.NullValue, "null"), want: nil},
		{name: "variable", value: scalar(ast.Variable, "contract"), want: "CABC"},
		{name: "variable keeps its decoded type", value: scalar(ast.Variable, "limit"), want: float64(25)},
		{name: "variable explicitly null", value: scalar(ast.Variable, "cleared"), want: nil},
		{name: "missing variable", value: scalar(ast.Variable, "absent"), wantErr: `missing variable "absent"`},
		{
			name: "list",
			value: &ast.Value{Kind: ast.ListValue, Children: ast.ChildValueList{
				{Value: scalar(ast.IntValue, "1")},
				{Value: scalar(ast.StringValue, `"two"`)},
				{Value: scalar(ast.Variable, "contract")},
			}},
			want: []any{int64(1), "two", "CABC"},
		},
		{name: "empty list", value: &ast.Value{Kind: ast.ListValue}, want: []any{}},
		{
			name: "object",
			value: &ast.Value{Kind: ast.ObjectValue, Children: ast.ChildValueList{
				{Name: "contractId", Value: scalar(ast.Variable, "contract")},
				{Name: "first", Value: scalar(ast.IntValue, "10")},
				{Name: "nested", Value: &ast.Value{Kind: ast.ObjectValue, Children: ast.ChildValueList{
					{Name: "asc", Value: scalar(ast.BooleanValue, "true")},
				}}},
			}},
			want: map[string]any{"contractId": "CABC", "first": int64(10), "nested": map[string]any{"asc": true}},
		},
		{name: "empty object", value: &ast.Value{Kind: ast.ObjectValue}, want: map[string]any{}},
		{
			name: "error inside list propagates",
			value: &ast.Value{Kind: ast.ListValue, Children: ast.ChildValueList{
				{Value: scalar(ast.IntValue, "1")},
				{Value: scalar(ast.IntValue, "x")},
			}},
			wantErr: `invalid int "x"`,
		},
		{
			name: "error inside object propagates",
			value: &ast.Value{Kind: ast.ObjectValue, Children: ast.ChildValueList{
				{Name: "id", Value: scalar(ast.Variable, "absent")},
			}},
			wantErr: `missing variable "absent"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := argumentValue(tc.value, vars)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDepthAndComplexityLimiters(t *testing.T) {
	sel := []ast.Selection{
		&ast.Field{
			Name: "node",
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
				&ast.Field{Name: "name"},
			},
		},
	}
	c := complexity(sel)
	assert.Greater(t, c, 0)

	d := depth(sel, 0)
	assert.Greater(t, d, 0)
}

func TestCursorHandling(t *testing.T) {
	c := EncodeCursor("test_id_123", "id", "ASC")
	assert.NotEmpty(t, c)

	decoded, err := DecodeCursor(c)
	require.NoError(t, err)
	assert.Equal(t, "test_id_123", decoded.LastID)

	_, err = DecodeCursor("invalid_cursor_string_tampered")
	require.Error(t, err)
}

func TestResolverErrorSurfacing(t *testing.T) {
	RegisterRoute("Query", "errorTestField", func(r *Resolver, ctx context.Context, args json.RawMessage, vars map[string]any) (any, error) {
		return nil, errors.New("boom resolver error")
	})
	UseResolver(&Resolver{})

	req := &GraphQLRequest{Query: "{ errorTestField }"}
	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, env)
	require.Len(t, env.Errors, 1)
	assert.Equal(t, "boom resolver error", env.Errors[0].Message)
}

func TestSchemaIntrospectionShape(t *testing.T) {
	UseResolver(&Resolver{})
	req := &GraphQLRequest{Query: "{ __schema { types { name } } }"}
	env, err := executeOperation(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, env)
	assert.Empty(t, env.Errors)
	assert.NotNil(t, env.Data["__schema"])
}
