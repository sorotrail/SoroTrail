package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The zero value is the one that matters most: it is what a read path gets
// when somebody forgets to set a scope, and the entire safety argument for
// #48 rests on that case denying rather than permitting.
func TestScope_ZeroValueDeniesEverything(t *testing.T) {
	var zero Scope

	assert.True(t, zero.DeniesAll(), "the zero Scope must deny everything")
	assert.False(t, zero.IsWildcard(), "the zero Scope must not be wildcard")
	assert.False(t, zero.Allows(contractA), "the zero Scope must allow no contract")
	assert.Empty(t, zero.Contracts())

	// An EventFilter built without touching Scope must inherit that denial:
	// this is the exact shape a forgetful handler would produce.
	var f EventFilter
	assert.True(t, f.Scope.DeniesAll(),
		"an EventFilter with no Scope set must deny, not permit")
}

func TestScope_Wildcard(t *testing.T) {
	for name, sc := range map[string]Scope{
		"wildcard": WildcardScope(),
		"system":   SystemScope(),
	} {
		t.Run(name, func(t *testing.T) {
			assert.True(t, sc.IsWildcard())
			assert.False(t, sc.DeniesAll())
			assert.True(t, sc.Allows(contractA))
			assert.True(t, sc.Allows("anything-at-all"))
		})
	}
}

func TestScope_GrantedContracts(t *testing.T) {
	sc := NewScope([]string{contractB, contractA})

	assert.False(t, sc.IsWildcard())
	assert.False(t, sc.DeniesAll())
	assert.True(t, sc.Allows(contractA))
	assert.True(t, sc.Allows(contractB))
	assert.False(t, sc.Allows("CNOTGRANTED"))
	assert.Equal(t, []string{contractA, contractB}, sc.Contracts(),
		"contracts are returned sorted")
}

// An empty grant list is a tenant that has been created but given nothing.
// It must deny, not degrade into a wildcard.
func TestScope_EmptyGrantsDeny(t *testing.T) {
	for name, ids := range map[string][]string{
		"nil":          nil,
		"empty":        {},
		"only-empties": {"", ""},
	} {
		t.Run(name, func(t *testing.T) {
			sc := NewScope(ids)
			assert.True(t, sc.DeniesAll())
			assert.False(t, sc.IsWildcard())
		})
	}
}

func TestScope_DeduplicatesGrants(t *testing.T) {
	sc := NewScope([]string{contractA, contractA, contractB, contractA})
	assert.Equal(t, []string{contractA, contractB}, sc.Contracts())
}

// Contracts returns a copy: a caller must not be able to widen its own scope
// by writing through the slice it was handed.
func TestScope_ContractsIsACopy(t *testing.T) {
	sc := NewScope([]string{contractA})
	got := sc.Contracts()
	require.Len(t, got, 1)
	got[0] = contractB

	assert.True(t, sc.Allows(contractA), "mutating the returned slice must not change the scope")
	assert.False(t, sc.Allows(contractB))
	assert.Equal(t, []string{contractA}, sc.Contracts())
}

// The fingerprint is a cache-key component, so it must distinguish scopes
// that permit different things and agree for scopes that permit the same
// things regardless of construction order.
func TestScope_Fingerprint(t *testing.T) {
	t.Run("distinguishes different grants", func(t *testing.T) {
		a := NewScope([]string{contractA}).Fingerprint()
		b := NewScope([]string{contractB}).Fingerprint()
		both := NewScope([]string{contractA, contractB}).Fingerprint()

		assert.NotEqual(t, a, b)
		assert.NotEqual(t, a, both)
		assert.NotEqual(t, b, both)
	})

	t.Run("is order independent", func(t *testing.T) {
		assert.Equal(t,
			NewScope([]string{contractA, contractB}).Fingerprint(),
			NewScope([]string{contractB, contractA}).Fingerprint())
	})

	t.Run("separates wildcard, empty and granted", func(t *testing.T) {
		var zero Scope
		assert.Equal(t, "wildcard", WildcardScope().Fingerprint())
		assert.Equal(t, "none", zero.Fingerprint())
		assert.NotEqual(t, WildcardScope().Fingerprint(), NewScope([]string{contractA}).Fingerprint())
	})
}
