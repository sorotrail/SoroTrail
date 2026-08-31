package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// validAddr returns a 56-character string that starts with the given
// prefix and is otherwise filled with a legal base32 character ("A").
func validAddr(prefix byte) string {
	b := make([]byte, 56)
	b[0] = prefix
	for i := 1; i < 56; i++ {
		b[i] = 'A'
	}
	return string(b)
}

func TestIsValidAddress(t *testing.T) {
	validG := validAddr('G')
	validC := validAddr('C')

	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid G address returns true",
			input: validG,
			want:  true,
		},
		{
			name:  "valid C contract address returns true",
			input: validC,
			want:  true,
		},
		{
			name:  "55-character string returns false",
			input: validG[:55],
			want:  false,
		},
		{
			name:  "57-character string returns false",
			input: validG + "A",
			want:  false,
		},
		{
			name:  "correct-length string starting with X returns false",
			input: validAddr('X'),
			want:  false,
		},
		{
			name:  "lowercase input returns false",
			input: "g" + validG[1:],
			want:  false,
		},
		{
			name:  "base32-invalid characters 0, 1, 8, 9 return false",
			input: "G0000000000000000000000000000000000000000000000000000000",
			want:  false,
		},
		{
			name:  "empty string returns false",
			input: "",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidAddress(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPaginationLink(t *testing.T) {
	tests := []struct {
		name            string
		url             string
		cursor          string
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:         "non-empty cursor is set in the returned URL",
			url:          "https://example.com/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			cursor:       "abc123",
			wantContains: []string{"cursor=abc123"},
		},
		{
			name:            "empty cursor removes the cursor parameter entirely",
			url:             "https://example.com/events?cursor=old&contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			cursor:          "",
			wantContains:    []string{"contract_id="},
			wantNotContains: []string{"cursor"},
		},
		{
			name:         "unrelated filters survive unchanged",
			url:          "https://example.com/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&limit=50",
			cursor:       "nextpage",
			wantContains: []string{"cursor=nextpage", "contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", "limit=50"},
		},
		{
			name:            "existing cursor parameter is replaced rather than duplicated",
			url:             "https://example.com/events?cursor=old_value",
			cursor:          "new_value",
			wantContains:    []string{"cursor=new_value"},
			wantNotContains: []string{"cursor=old_value"},
		},
		{
			name:         "path is preserved for a nested route",
			url:          "https://example.com/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events?from_ledger=100",
			cursor:       "page2",
			wantContains: []string{"/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events", "cursor=page2", "from_ledger=100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, tt.url, nil)
			assert.NoError(t, err)

			got := paginationLink(r, tt.cursor)

			for _, s := range tt.wantContains {
				assert.Contains(t, got, s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, got, s)
			}
		})
	}
}

func TestIngestLagLedgers(t *testing.T) {
	tests := []struct {
		name         string
		chainHead    int64
		lastIngested int64
		want         int64
	}{
		{
			name:         "chain head ahead of last ingested returns the difference",
			chainHead:    1000,
			lastIngested: 950,
			want:         50,
		},
		{
			name:         "equal values return zero",
			chainHead:    1000,
			lastIngested: 1000,
			want:         0,
		},
		{
			name:         "zero chain head returns zero rather than negative lag",
			chainHead:    0,
			lastIngested: 100,
			want:         0,
		},
		{
			name:         "negative chain head returns zero rather than negative lag",
			chainHead:    -1,
			lastIngested: 100,
			want:         0,
		},
		{
			name:         "zero last ingested returns zero",
			chainHead:    100,
			lastIngested: 0,
			want:         0,
		},
		{
			name:         "negative last ingested returns zero",
			chainHead:    100,
			lastIngested: -1,
			want:         0,
		},
		{
			name:         "last ingested ahead of chain head does not return negative",
			chainHead:    95,
			lastIngested: 100,
			want:         0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingestLagLedgers(tt.chainHead, tt.lastIngested)
			assert.Equal(t, tt.want, got)
		})
	}
}
