package filters

import (
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateUnsignedNumericFilterValues(t *testing.T) {
	tests := []struct {
		name              string
		values            []string
		bitSize           int
		maxNumberOfValues int
		wantErr           bool
		errMessage        string
	}{
		{
			name:              "valid single value",
			values:            []string{"123"},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           false,
		},
		{
			name:              "valid multiple values",
			values:            []string{"1", "255"},
			bitSize:           8,
			maxNumberOfValues: 2,
			wantErr:           false,
		},
		{
			name:              "empty values list",
			values:            []string{},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid number of values",
		},
		{
			name:              "too many values",
			values:            []string{"1", "2", "3"},
			bitSize:           32,
			maxNumberOfValues: 2,
			wantErr:           true,
			errMessage:        "invalid number of values",
		},
		{
			name:              "invalid numeric value",
			values:            []string{"abc"},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
		{
			name:              "out of range for bit size",
			values:            []string{"256"},
			bitSize:           8,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
		{
			name:              "negative value as unsigned",
			values:            []string{"-1"},
			bitSize:           64,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUnsignedNumericFilterValues(tt.values, tt.bitSize, tt.maxNumberOfValues)
			if tt.wantErr {
				assert.ErrorContains(t, err, tt.errMessage)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSignedNumericFilterValue(t *testing.T) {
	tests := []struct {
		name              string
		values            []string
		bitSize           int
		maxNumberOfValues int
		wantErr           bool
		errMessage        string
	}{
		{
			name:              "valid positive value",
			values:            []string{"123"},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           false,
		},
		{
			name:              "valid negative value",
			values:            []string{"-123"},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           false,
		},
		{
			name:              "valid multiple values mixed signs",
			values:            []string{"100", "-50"},
			bitSize:           32,
			maxNumberOfValues: 2,
			wantErr:           false,
		},
		{
			name:              "empty values list",
			values:            []string{},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid number of values",
		},
		{
			name:              "too many values",
			values:            []string{"1", "2", "3"},
			bitSize:           32,
			maxNumberOfValues: 2,
			wantErr:           true,
			errMessage:        "invalid number of values",
		},
		{
			name:              "invalid numeric value",
			values:            []string{"abc"},
			bitSize:           32,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
		{
			name:              "out of range for bit size positive",
			values:            []string{"128"},
			bitSize:           8,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
		{
			name:              "out of range for bit size negative",
			values:            []string{"-129"},
			bitSize:           8,
			maxNumberOfValues: 1,
			wantErr:           true,
			errMessage:        "invalid numeric value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSignedNumericFilterValue(tt.values, tt.bitSize, tt.maxNumberOfValues)
			if tt.wantErr {
				assert.ErrorContains(t, err, tt.errMessage)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

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
				require.ErrorContains(t, err, tt.errMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
