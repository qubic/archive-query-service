package filters

import (
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/require"
)

func TestVerifyNoConflictingFilters(t *testing.T) {
	tests := []struct {
		name       string
		filters    entities.Filters
		wantErr    bool
		errMessage string
	}{
		{
			name: "no conflicts - empty filters",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should:  []entities.ShouldFilter{},
			},
			wantErr: false,
		},
		{
			name: "no conflicts - only include",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
					"field2": {"value2"},
				},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should:  []entities.ShouldFilter{},
			},
			wantErr: false,
		},
		{
			name: "no conflicts - only exclude",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{
					"field1": {"value1"},
					"field2": {"value2"},
				},
				Ranges: map[string][]entities.Range{},
				Should: []entities.ShouldFilter{},
			},
			wantErr: false,
		},
		{
			name: "no conflicts - only ranges",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{},
				Ranges: map[string][]entities.Range{
					"field1": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{},
			},
			wantErr: false,
		},
		{
			name: "no conflicts - different fields in include and exclude",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{
					"field2": {"value2"},
				},
				Ranges: map[string][]entities.Range{},
				Should: []entities.ShouldFilter{},
			},
			wantErr: false,
		},
		{
			name: "conflict - same field in include and exclude",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{
					"field1": {"value2"},
				},
				Ranges: map[string][]entities.Range{},
				Should: []entities.ShouldFilter{},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "conflict - same field in include and ranges",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{},
				Ranges: map[string][]entities.Range{
					"field1": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "conflict - same field in exclude and ranges",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{
					"field1": {"value1"},
				},
				Ranges: map[string][]entities.Range{
					"field1": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "conflict - same field in include and should terms",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field1": {"value2"},
						},
						Ranges: map[string][]entities.Range{},
					},
				},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "allowed - same field in exclude and should ranges",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{
					"field1": {"value1"},
				},
				Ranges: map[string][]entities.Range{},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{},
						Ranges: map[string][]entities.Range{
							"field1": {{Operation: "gte", Value: "10"}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "conflict - same field in ranges and should terms",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{},
				Ranges: map[string][]entities.Range{
					"field1": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field1": {"value1"},
						},
						Ranges: map[string][]entities.Range{},
					},
				},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "no conflicts - multiple should filters with different fields",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field2": {"value2"},
						},
						Ranges: map[string][]entities.Range{},
					},
					{
						Terms: map[string][]string{
							"field3": {"value3"},
						},
						Ranges: map[string][]entities.Range{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "conflict - multiple should filters with conflicting field",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field1": {"value1"},
						},
						Ranges: map[string][]entities.Range{},
					},
					{
						Terms: map[string][]string{
							"field1": {"value2"},
						},
						Ranges: map[string][]entities.Range{},
					},
				},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "conflict - should terms and should ranges same field",
			filters: entities.Filters{
				Include: map[string][]string{},
				Exclude: map[string][]string{},
				Ranges:  map[string][]entities.Range{},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field1": {"value1"},
						},
						Ranges: map[string][]entities.Range{
							"field1": {{Operation: "gte", Value: "10"}},
						},
					},
				},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
		{
			name: "complex - no conflicts with multiple filters",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
					"field2": {"value2"},
				},
				Exclude: map[string][]string{
					"field3": {"value3"},
				},
				Ranges: map[string][]entities.Range{
					"field4": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{
					{
						Terms: map[string][]string{
							"field5": {"value5"},
						},
						Ranges: map[string][]entities.Range{
							"field6": {{Operation: "lte", Value: "100"}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "complex - conflict across all filter types",
			filters: entities.Filters{
				Include: map[string][]string{
					"field1": {"value1"},
				},
				Exclude: map[string][]string{
					"field2": {"value2"},
				},
				Ranges: map[string][]entities.Range{
					"field1": {{Operation: "gte", Value: "10"}},
				},
				Should: []entities.ShouldFilter{},
			},
			wantErr:    true,
			errMessage: "duplicate [field1] filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := VerifyNoConflictingFilters(tt.filters)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
