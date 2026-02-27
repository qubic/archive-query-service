package elastic

import (
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/require"
)

func Test_createRangeFilter(t *testing.T) {
	tests := []struct {
		name     string
		property string
		ranges   []*entities.Range
		want     string
	}{
		{
			name:     "valid single range",
			property: "tick",
			ranges: []*entities.Range{
				{Operation: "gte", Value: "100"},
			},
			want: `{"range":{"tick":{"gte":"100"}}}`,
		},
		{
			name:     "valid dual range",
			property: "tick",
			ranges: []*entities.Range{
				{Operation: "gte", Value: "100"},
				{Operation: "lte", Value: "200"},
			},
			want: `{"range":{"tick":{"gte":"100","lte":"200"}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createRangeFilter(tt.property, tt.ranges)
			require.NoError(t, err)
			require.JSONEq(t, tt.want, got)
		})
	}
}
