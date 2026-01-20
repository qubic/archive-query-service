package elastic

import (
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/require"
)

func Test_splitFilters(t *testing.T) {
	tests := []struct {
		name        string
		filters     map[string][]string
		wantInclude map[string][]string
		wantExclude map[string][]string
	}{
		{
			name:        "empty filters",
			filters:     map[string][]string{},
			wantInclude: map[string][]string{},
			wantExclude: map[string][]string{},
		},
		{
			name: "only include filters",
			filters: map[string][]string{
				"source":      {"identity1"},
				"destination": {"identity2", "identity3"},
			},
			wantInclude: map[string][]string{
				"source":      {"identity1"},
				"destination": {"identity2", "identity3"},
			},
			wantExclude: map[string][]string{},
		},
		{
			name: "only exclude filters",
			filters: map[string][]string{
				"source-exclude":      {"identity1"},
				"destination-exclude": {"identity2", "identity3"},
			},
			wantInclude: map[string][]string{},
			wantExclude: map[string][]string{
				"source":      {"identity1"},
				"destination": {"identity2", "identity3"},
			},
		},
		{
			name: "mixed filters",
			filters: map[string][]string{
				"source":            {"identity1"},
				"source-exclude":    {"identity2"},
				"amount":            {"100"},
				"inputType-exclude": {"1"},
			},
			wantInclude: map[string][]string{
				"source": {"identity1"},
				"amount": {"100"},
			},
			wantExclude: map[string][]string{
				"source":    {"identity2"},
				"inputType": {"1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInclude, gotExclude := splitFilters(tt.filters)
			require.Equal(t, tt.wantInclude, gotInclude)
			require.Equal(t, tt.wantExclude, gotExclude)
		})
	}
}

func Test_getFilterStrings(t *testing.T) {
	tests := []struct {
		name    string
		filters map[string][]string
		want    []string
	}{
		{
			name:    "empty filters",
			filters: map[string][]string{},
			want:    []string{},
		},
		{
			name: "single filter with single value",
			filters: map[string][]string{
				"source": {"identity1"},
			},
			want: []string{
				`{"term":{"source":"identity1"}}`,
			},
		},
		{
			name: "single filter with multiple values",
			filters: map[string][]string{
				"destination": {"identity2", "identity3"},
			},
			want: []string{
				`{"terms":{"destination":["identity2","identity3"]}}`,
			},
		},
		{
			name: "multiple filters with mixed values",
			filters: map[string][]string{
				"source":      {"identity1"},
				"destination": {"identity2", "identity3"},
				"amount":      {"100"},
			},
			want: []string{
				`{"term":{"amount":"100"}}`,
				`{"terms":{"destination":["identity2","identity3"]}}`,
				`{"term":{"source":"identity1"}}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getFilterStrings(tt.filters)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_getRangeFilterStrings(t *testing.T) {
	tests := []struct {
		name    string
		ranges  map[string][]*entities.Range
		want    []string
		wantErr bool
	}{
		{
			name:   "empty ranges",
			ranges: map[string][]*entities.Range{},
			want:   []string{},
		},
		{
			name: "single range with single operation",
			ranges: map[string][]*entities.Range{
				"amount": {
					{Operation: "gte", Value: "1000"},
				},
			},
			want: []string{
				`{"range":{"amount":{"gte":"1000"}}}`,
			},
		},
		{
			name: "single range with multiple operations",
			ranges: map[string][]*entities.Range{
				"tickNumber": {
					{Operation: "gte", Value: "100"},
					{Operation: "lte", Value: "200"},
				},
			},
			want: []string{
				`{"range":{"tickNumber":{"gte":"100","lte":"200"}}}`,
			},
		},
		{
			name: "multiple ranges",
			ranges: map[string][]*entities.Range{
				"amount": {
					{Operation: "gt", Value: "0"},
				},
				"timestamp": {
					{Operation: "lt", Value: "123456789"},
				},
			},
			want: []string{
				`{"range":{"amount":{"gt":"0"}}}`,
				`{"range":{"timestamp":{"lt":"123456789"}}}`,
			},
		},
		{
			name: "empty range slice returns error",
			ranges: map[string][]*entities.Range{
				"amount": {},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRangeFilterStrings(tt.ranges)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
